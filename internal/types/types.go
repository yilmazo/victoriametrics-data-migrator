// Package types contains shared types used across the vm-migrator application.
package types

import (
	"fmt"
	"time"
)

// TaskStatus represents the lifecycle state of a migration task.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusSucceeded TaskStatus = "succeeded"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusRetrying  TaskStatus = "retrying"
	TaskStatusAbandoned TaskStatus = "abandoned"
)

// Task represents a single migration unit: a specific series selector
// over a specific time range, to be executed by a vmctl worker.
type Task struct {
	ID             string     `json:"id" yaml:"id"`
	MetricName     string     `json:"metric_name" yaml:"metric_name"`
	Selector       string     `json:"selector" yaml:"selector"`
	TimeStart      time.Time  `json:"time_start" yaml:"time_start"`
	TimeEnd        time.Time  `json:"time_end" yaml:"time_end"`
	EstSeriesCount int        `json:"est_series_count" yaml:"est_series_count"`
	Status         TaskStatus `json:"status" yaml:"status"`
	Attempts       int        `json:"attempts" yaml:"attempts"`
	MaxRetries     int        `json:"max_retries" yaml:"max_retries"`
	WorkerID       string     `json:"worker_id,omitempty" yaml:"worker_id,omitempty"`
	Error          string     `json:"error,omitempty" yaml:"error,omitempty"`
	StartedAt      *time.Time `json:"started_at,omitempty" yaml:"started_at,omitempty"`
	CompletedAt    *time.Time `json:"completed_at,omitempty" yaml:"completed_at,omitempty"`
	BytesTransferred int64   `json:"bytes_transferred,omitempty" yaml:"bytes_transferred,omitempty"`
}

// TimeRange represents a time interval [Start, End).
type TimeRange struct {
	Start time.Time `json:"start" yaml:"start"`
	End   time.Time `json:"end" yaml:"end"`
}

// String returns a human-readable representation.
func (tr TimeRange) String() string {
	return fmt.Sprintf("[%s — %s)", tr.Start.Format("2006-01-02"), tr.End.Format("2006-01-02"))
}

// LabelValueCount holds a label value and its associated series count.
type LabelValueCount struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

// TSDBStatus holds the response from /api/v1/status/tsdb.
type TSDBStatus struct {
	TotalSeries                  int               `json:"totalSeries"`
	TotalLabelValuePairs         int               `json:"totalLabelValuePairs"`
	SeriesCountByMetricName      []LabelValueCount `json:"seriesCountByMetricName"`
	SeriesCountByFocusLabelValue []LabelValueCount `json:"seriesCountByFocusLabelValue"`
	SeriesCountByLabelValuePair  []LabelValueCount `json:"seriesCountByLabelValuePair"`
	LabelValueCountByLabelName   []LabelValueCount `json:"labelValueCountByLabelName"`
}

// MigrationState represents the persisted state of a migration for resume support.
type MigrationState struct {
	MigrationID    string      `json:"migration_id" yaml:"migration_id"`
	ConfigHash     string      `json:"config_hash" yaml:"config_hash"`
	StartedAt      time.Time   `json:"started_at" yaml:"started_at"`
	LastUpdated    time.Time   `json:"last_updated" yaml:"last_updated"`
	TimeRanges     []TimeRange `json:"time_ranges" yaml:"time_ranges"`
	CurrentRangeIdx int        `json:"current_range_idx" yaml:"current_range_idx"`
	Tasks          []*Task     `json:"tasks" yaml:"tasks"`
	CompletedTasks int         `json:"completed_tasks" yaml:"completed_tasks"`
	FailedTasks    int         `json:"failed_tasks" yaml:"failed_tasks"`
	TotalBytes     int64       `json:"total_bytes" yaml:"total_bytes"`
}

// MigrationReport is the final report generated after migration completes.
type MigrationReport struct {
	StartTime        time.Time          `json:"start_time"`
	EndTime          time.Time          `json:"end_time"`
	Duration         string             `json:"duration"`
	TotalTasks       int                `json:"total_tasks"`
	Succeeded        int                `json:"succeeded"`
	Failed           int                `json:"failed"`
	Abandoned        int                `json:"abandoned"`
	Retried          int                `json:"retried"`
	TotalBytes       int64              `json:"total_bytes"`
	TotalBytesHuman  string             `json:"total_bytes_human"`
	MetricsMigrated  int                `json:"metrics_migrated"`
	TimeRangesTotal  int                `json:"time_ranges_total"`
	TimeRangesDone   int                `json:"time_ranges_done"`
	FailedTaskDetails []FailedTaskDetail `json:"failed_tasks,omitempty"`
}

// FailedTaskDetail provides information about a task that failed permanently.
type FailedTaskDetail struct {
	ID        string `json:"id"`
	Selector  string `json:"selector"`
	TimeRange string `json:"time_range"`
	Error     string `json:"error"`
	Attempts  int    `json:"attempts"`
}
