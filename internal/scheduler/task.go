package scheduler

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/yilmazo/victoriametrics-data-migrator/internal/types"
)

// TaskQueue is a thread-safe queue for managing migration tasks.
type TaskQueue struct {
	mu       sync.Mutex
	tasks    []*types.Task
	pending  []*types.Task
	running  map[string]*types.Task
	done     []*types.Task
	failed   []*types.Task
	logger   *zap.Logger
	stateFile string
}

// NewTaskQueue creates a new task queue with optional state persistence.
func NewTaskQueue(logger *zap.Logger, stateFile string) *TaskQueue {
	return &TaskQueue{
		running:   make(map[string]*types.Task),
		logger:    logger,
		stateFile: stateFile,
	}
}

// AddTasks enqueues a batch of tasks.
func (q *TaskQueue) AddTasks(tasks []*types.Task) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.tasks = append(q.tasks, tasks...)
	q.pending = append(q.pending, tasks...)
}

// NextTask returns the next pending task, or nil if none available.
func (q *TaskQueue) NextTask() *types.Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) == 0 {
		return nil
	}

	task := q.pending[0]
	q.pending = q.pending[1:]
	task.Status = types.TaskStatusRunning
	now := time.Now()
	task.StartedAt = &now
	q.running[task.ID] = task

	return task
}

// GetTask returns a task by ID from any state (running, pending, done, failed).
func (q *TaskQueue) GetTask(taskID string) *types.Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	if t, ok := q.running[taskID]; ok {
		return t
	}
	for _, t := range q.pending {
		if t.ID == taskID {
			return t
		}
	}
	for _, t := range q.done {
		if t.ID == taskID {
			return t
		}
	}
	for _, t := range q.failed {
		if t.ID == taskID {
			return t
		}
	}
	return nil
}

// CompleteTask marks a task as succeeded.
func (q *TaskQueue) CompleteTask(taskID string, bytesTransferred int64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.running[taskID]
	if !ok {
		q.logger.Warn("Completing unknown task", zap.String("task_id", taskID))
		return
	}

	task.Status = types.TaskStatusSucceeded
	now := time.Now()
	task.CompletedAt = &now
	task.BytesTransferred = bytesTransferred

	delete(q.running, taskID)
	q.done = append(q.done, task)

	q.persistState()
}

// FailTask marks a task as failed. Returns true if the task should be retried.
func (q *TaskQueue) FailTask(taskID string, errMsg string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.running[taskID]
	if !ok {
		q.logger.Warn("Failing unknown task", zap.String("task_id", taskID))
		return false
	}

	delete(q.running, taskID)
	task.Attempts++
	task.Error = errMsg

	if task.Attempts < task.MaxRetries {
		task.Status = types.TaskStatusRetrying
		q.pending = append(q.pending, task) // Re-enqueue
		q.logger.Info("Task will be retried",
			zap.String("task_id", taskID),
			zap.Int("attempt", task.Attempts),
			zap.Int("max_retries", task.MaxRetries),
		)
		q.persistState()
		return true
	}

	task.Status = types.TaskStatusAbandoned
	now := time.Now()
	task.CompletedAt = &now
	q.failed = append(q.failed, task)
	q.persistState()
	return false
}

// ReplaceTasks removes a task from pending/running and replaces it with new sub-tasks
// (used for auto-resplit on failure).
func (q *TaskQueue) ReplaceTasks(oldTaskID string, newTasks []*types.Task) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.running, oldTaskID)

	q.tasks = append(q.tasks, newTasks...)
	q.pending = append(q.pending, newTasks...)
	q.persistState()
}

// Stats returns current queue statistics.
func (q *TaskQueue) Stats() (total, pending, running, succeeded, failed, abandoned int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.tasks), len(q.pending), len(q.running), len(q.done), len(q.failed), q.countAbandoned()
}

func (q *TaskQueue) countAbandoned() int {
	count := 0
	for _, t := range q.failed {
		if t.Status == types.TaskStatusAbandoned {
			count++
		}
	}
	return count
}

// TotalBytes returns the total bytes transferred by completed tasks.
func (q *TaskQueue) TotalBytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()

	var total int64
	for _, t := range q.done {
		total += t.BytesTransferred
	}
	return total
}

// IsComplete returns true when all tasks are done (succeeded or abandoned).
func (q *TaskQueue) IsComplete() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.pending) == 0 && len(q.running) == 0
}

// GetFailedTasks returns all permanently failed tasks.
func (q *TaskQueue) GetFailedTasks() []*types.Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	result := make([]*types.Task, len(q.failed))
	copy(result, q.failed)
	return result
}

// GetAllTasks returns all tasks for state persistence.
func (q *TaskQueue) GetAllTasks() []*types.Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	result := make([]*types.Task, len(q.tasks))
	copy(result, q.tasks)
	return result
}

// persistState writes the current state to disk for crash recovery.
func (q *TaskQueue) persistState() {
	if q.stateFile == "" {
		return
	}

	state := types.MigrationState{
		LastUpdated:    time.Now(),
		Tasks:          q.tasks,
		CompletedTasks: len(q.done),
		FailedTasks:    len(q.failed),
		TotalBytes:     q.totalBytesLocked(),
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		q.logger.Error("Failed to marshal state", zap.Error(err))
		return
	}

	if err := os.WriteFile(q.stateFile, data, 0644); err != nil {
		q.logger.Error("Failed to persist state", zap.Error(err))
	}
}

func (q *TaskQueue) totalBytesLocked() int64 {
	var total int64
	for _, t := range q.done {
		total += t.BytesTransferred
	}
	return total
}

// LoadState attempts to load a persisted migration state from disk.
func LoadState(stateFile string) (*types.MigrationState, error) {
	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading state file: %w", err)
	}

	var state types.MigrationState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parsing state file: %w", err)
	}

	return &state, nil
}
