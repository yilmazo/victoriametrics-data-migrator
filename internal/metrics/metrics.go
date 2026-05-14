// Package metrics provides Prometheus metrics for monitoring migration progress.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// TasksTotal tracks the total number of tasks by status.
	TasksTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "vm_migrator",
		Name:      "tasks_total",
		Help:      "Total number of migration tasks by status.",
	}, []string{"status"})

	// TaskDuration tracks the duration of completed tasks.
	TaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "vm_migrator",
		Name:      "task_duration_seconds",
		Help:      "Duration of completed migration tasks in seconds.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1s to ~4.5h
	}, []string{"metric"})

	// BytesTransferred tracks the total bytes transferred.
	BytesTransferred = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "vm_migrator",
		Name:      "bytes_transferred_total",
		Help:      "Total bytes transferred during migration.",
	})

	// TimeRangesProcessed tracks completed time ranges.
	TimeRangesProcessed = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "vm_migrator",
		Name:      "time_ranges_processed",
		Help:      "Number of time ranges that have been fully processed.",
	})

	// TimeRangesTotal tracks total time ranges.
	TimeRangesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "vm_migrator",
		Name:      "time_ranges_total",
		Help:      "Total number of time ranges to process.",
	})

	// ActiveWorkers tracks currently running worker Jobs.
	ActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "vm_migrator",
		Name:      "active_workers",
		Help:      "Number of currently active worker Jobs.",
	})

	// MetricsDiscovered tracks the number of discovered metrics per time range.
	MetricsDiscovered = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "vm_migrator",
		Name:      "metrics_discovered",
		Help:      "Number of metrics discovered in the current time range.",
	})

	// SplitOperations tracks the number of series splitting operations performed.
	SplitOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "vm_migrator",
		Name:      "split_operations_total",
		Help:      "Total number of series split operations by result.",
	}, []string{"result"}) // result: "no_split", "split", "recursive_split", "error"

	// TaskRetries tracks the number of task retries.
	TaskRetries = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "vm_migrator",
		Name:      "task_retries_total",
		Help:      "Total number of task retries.",
	})
)
