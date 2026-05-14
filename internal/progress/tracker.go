// Package progress provides migration progress tracking and reporting.
package progress

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/yilmazo/victoriametrics-data-migrator/internal/scheduler"
	"github.com/yilmazo/victoriametrics-data-migrator/internal/types"
)

// Tracker monitors migration progress and reports status periodically.
type Tracker struct {
	mu              sync.Mutex
	queue           *scheduler.TaskQueue
	startTime       time.Time
	logger          *zap.Logger
	interval        time.Duration
	stopCh          chan struct{}
	totalRanges     int
	currentRange    int
	currentRangeStr string
	stopOnce        sync.Once
}

// NewTracker creates a new progress tracker.
func NewTracker(queue *scheduler.TaskQueue, interval time.Duration, totalRanges int, logger *zap.Logger) *Tracker {
	return &Tracker{
		queue:       queue,
		startTime:   time.Now(),
		logger:      logger,
		interval:    interval,
		stopCh:      make(chan struct{}),
		totalRanges: totalRanges,
	}
}

// Start begins periodic progress logging.
func (t *Tracker) Start() {
	// Reset stop mechanism for re-use across time ranges
	t.stopCh = make(chan struct{})
	t.stopOnce = sync.Once{}

	go func() {
		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				t.logProgress()
			case <-t.stopCh:
				return
			}
		}
	}()
}

// Stop ends periodic progress logging. Safe to call multiple times.
func (t *Tracker) Stop() {
	t.stopOnce.Do(func() {
		close(t.stopCh)
	})
}

// SetCurrentRange updates which time range is being processed.
func (t *Tracker) SetCurrentRange(idx int, rangeStr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.currentRange = idx
	t.currentRangeStr = rangeStr
}

// logProgress logs the current migration status.
func (t *Tracker) logProgress() {
	total, pending, running, succeeded, failed, abandoned := t.queue.Stats()
	elapsed := time.Since(t.startTime)
	totalBytes := t.queue.TotalBytes()

	t.mu.Lock()
	rangeIdx := t.currentRange
	rangeStr := t.currentRangeStr
	totalRanges := t.totalRanges
	t.mu.Unlock()

	// Estimate time remaining
	var eta string
	if succeeded > 0 {
		avgTime := elapsed / time.Duration(succeeded)
		remaining := avgTime * time.Duration(pending+running)
		eta = remaining.Round(time.Second).String()
	} else {
		eta = "calculating..."
	}

	t.logger.Info("Migration progress",
		zap.String("elapsed", elapsed.Round(time.Second).String()),
		zap.Int("total_tasks", total),
		zap.Int("succeeded", succeeded),
		zap.Int("pending", pending),
		zap.Int("running", running),
		zap.Int("failed", failed),
		zap.Int("abandoned", abandoned),
		zap.String("bytes_transferred", humanBytes(totalBytes)),
		zap.String("time_range", fmt.Sprintf("%d/%d", rangeIdx+1, totalRanges)),
		zap.String("current_range", rangeStr),
		zap.String("eta", eta),
	)
}

// GenerateReport creates the final migration report.
func (t *Tracker) GenerateReport(metricsCount int) *types.MigrationReport {
	total, _, _, succeeded, _, abandoned := t.queue.Stats()
	endTime := time.Now()
	duration := endTime.Sub(t.startTime)
	totalBytes := t.queue.TotalBytes()

	report := &types.MigrationReport{
		StartTime:       t.startTime,
		EndTime:         endTime,
		Duration:        duration.Round(time.Second).String(),
		TotalTasks:      total,
		Succeeded:       succeeded,
		Failed:          0,
		Abandoned:       abandoned,
		TotalBytes:      totalBytes,
		TotalBytesHuman: humanBytes(totalBytes),
		MetricsMigrated: metricsCount,
		TimeRangesTotal: t.totalRanges,
		TimeRangesDone:  t.currentRange + 1,
	}

	// Add failed task details
	for _, task := range t.queue.GetFailedTasks() {
		report.FailedTaskDetails = append(report.FailedTaskDetails, types.FailedTaskDetail{
			ID:       task.ID,
			Selector: task.Selector,
			TimeRange: fmt.Sprintf("%s — %s",
				task.TimeStart.Format("2006-01-02"),
				task.TimeEnd.Format("2006-01-02")),
			Error:    task.Error,
			Attempts: task.Attempts,
		})
	}

	// Count retried tasks
	for _, task := range t.queue.GetAllTasks() {
		if task.Attempts > 1 && task.Status == types.TaskStatusSucceeded {
			report.Retried++
		}
	}

	return report
}

// WriteReport writes the migration report to a JSON file.
func WriteReport(report *types.MigrationReport, path string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling report: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writing report file: %w", err)
	}

	return nil
}

// humanBytes converts a byte count to a human-readable string.
func humanBytes(b int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
		TB = 1024 * GB
	)

	switch {
	case b >= TB:
		return fmt.Sprintf("%.2f TB", float64(b)/float64(TB))
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
