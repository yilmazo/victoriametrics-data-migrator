// Package orchestrator ties together all components to execute the migration.
package orchestrator

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/yilmazo/victoriametrics-data-migrator/internal/config"
	"github.com/yilmazo/victoriametrics-data-migrator/internal/discovery"
	intmetrics "github.com/yilmazo/victoriametrics-data-migrator/internal/metrics"
	"github.com/yilmazo/victoriametrics-data-migrator/internal/progress"
	"github.com/yilmazo/victoriametrics-data-migrator/internal/scheduler"
	"github.com/yilmazo/victoriametrics-data-migrator/internal/splitter"
	"github.com/yilmazo/victoriametrics-data-migrator/internal/types"
	"github.com/yilmazo/victoriametrics-data-migrator/internal/worker"
)

// Orchestrator coordinates the entire migration process.
type Orchestrator struct {
	cfg         *config.Config
	client      *discovery.Client
	splitter    *splitter.Splitter
	queue       *scheduler.TaskQueue
	workerMgr   *worker.Manager
	tracker     *progress.Tracker
	logger      *zap.Logger
	migrationID string
	dryRun      bool
	taskCounter int
}

// New creates a new Orchestrator.
func New(cfg *config.Config, logger *zap.Logger, dryRun bool) (*Orchestrator, error) {
	migrationID := generateMigrationID(cfg)
	stateFile := fmt.Sprintf("migration-state-%s.json", migrationID)

	vmClient := discovery.NewClient(cfg.Source, logger)

	spl := splitter.New(
		vmClient,
		cfg.Splitting.MaxRegexValues,
		cfg.Splitting.ExcludeSplitLabels,
		cfg.Splitting.PreferredSplitLabels,
		logger,
	)

	queue := scheduler.NewTaskQueue(logger, stateFile)

	o := &Orchestrator{
		cfg:         cfg,
		client:      vmClient,
		splitter:    spl,
		queue:       queue,
		logger:      logger,
		migrationID: migrationID,
		dryRun:      dryRun,
	}

	if !dryRun {
		mgr, err := worker.NewManager(cfg, migrationID, logger)
		if err != nil {
			return nil, fmt.Errorf("creating worker manager: %w", err)
		}
		o.workerMgr = mgr
	}

	return o, nil
}

// Run executes the complete migration workflow.
func (o *Orchestrator) Run(ctx context.Context) error {
	o.logger.Info("Starting migration",
		zap.String("migration_id", o.migrationID),
		zap.String("source", o.cfg.Source.VmselectURL),
		zap.String("destination", o.cfg.Destination.VminsertURL),
		zap.Bool("dry_run", o.dryRun),
	)

	// Start metrics server if enabled
	if o.cfg.Monitoring.Enabled {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			o.logger.Info("Starting metrics server", zap.String("address", o.cfg.Monitoring.Address))
			if err := http.ListenAndServe(o.cfg.Monitoring.Address, mux); err != nil {
				o.logger.Error("Metrics server error", zap.Error(err))
			}
		}()
	}

	// Step 1: Parse time range
	startDate, err := config.ParseDate(o.cfg.Migration.StartDate)
	if err != nil {
		return fmt.Errorf("parsing start date: %w", err)
	}
	endDate, err := config.ParseDate(o.cfg.Migration.EndDate)
	if err != nil {
		return fmt.Errorf("parsing end date: %w", err)
	}

	// Step 2: Split time range into intervals
	timeRanges := scheduler.SplitTimeRange(startDate, endDate, o.cfg.Migration.TimeStep, o.cfg.Migration.ReverseOrder)
	o.logger.Info("Time range split",
		zap.Int("intervals", len(timeRanges)),
		zap.String("step", o.cfg.Migration.TimeStep),
		zap.Bool("reverse", o.cfg.Migration.ReverseOrder),
	)
	intmetrics.TimeRangesTotal.Set(float64(len(timeRanges)))

	// Build exclude regex patterns
	excludePatterns, err := compileExcludePatterns(o.cfg.ExcludeMetrics)
	if err != nil {
		return fmt.Errorf("compiling exclude patterns: %w", err)
	}

	// Step 3: Start progress tracker
	progressInterval, _ := time.ParseDuration(o.cfg.Logging.ProgressInterval)
	o.tracker = progress.NewTracker(o.queue, progressInterval, len(timeRanges), o.logger)

	totalMetricsProcessed := 0

	// Step 4: Process each time range
	for rangeIdx, tr := range timeRanges {
		select {
		case <-ctx.Done():
			o.logger.Info("Migration cancelled")
			return ctx.Err()
		default:
		}

		o.logger.Info("Processing time range",
			zap.Int("range", rangeIdx+1),
			zap.Int("total_ranges", len(timeRanges)),
			zap.String("start", tr.Start.Format("2006-01-02")),
			zap.String("end", tr.End.Format("2006-01-02")),
		)
		o.tracker.SetCurrentRange(rangeIdx, tr.String())

		// 4a: Discover metrics for this time range
		metrics, err := o.client.DiscoverMetrics(ctx, o.cfg.Migration.FilterMatch, tr.Start, tr.End)
		if err != nil {
			o.logger.Error("Failed to discover metrics", zap.Error(err))
			continue
		}

		// Filter out excluded metrics
		metrics = filterMetrics(metrics, excludePatterns)
		intmetrics.MetricsDiscovered.Set(float64(len(metrics)))

		o.logger.Info("Metrics to process in this range",
			zap.Int("count", len(metrics)),
			zap.String("range", tr.String()),
		)

		// 4b: For each metric, split and create tasks
		var rangeTasks []*types.Task
		for _, metric := range metrics {
			tasks, err := o.createTasksForMetric(ctx, metric, tr)
			if err != nil {
				o.logger.Error("Failed to create tasks for metric",
					zap.String("metric", metric),
					zap.Error(err),
				)
				continue
			}
			rangeTasks = append(rangeTasks, tasks...)
		}

		totalMetricsProcessed += len(metrics)

		if len(rangeTasks) == 0 {
			o.logger.Info("No tasks for this time range, skipping")
			intmetrics.TimeRangesProcessed.Inc()
			continue
		}

		o.queue.AddTasks(rangeTasks)

		if o.dryRun {
			o.logDryRunSummary(rangeTasks, tr)
			intmetrics.TimeRangesProcessed.Inc()
			continue
		}

		// 4c: Execute tasks for this time range
		o.tracker.Start()
		if err := o.executeTasks(ctx); err != nil {
			o.tracker.Stop()
			return fmt.Errorf("executing tasks for range %s: %w", tr.String(), err)
		}
		o.tracker.Stop()

		intmetrics.TimeRangesProcessed.Inc()
	}

	// Step 5: Generate report
	if o.dryRun {
		o.logger.Info("Dry run complete",
			zap.Int("total_metrics", totalMetricsProcessed),
			zap.Int("total_tasks", len(o.queue.GetAllTasks())),
		)
		return nil
	}

	report := o.tracker.GenerateReport(totalMetricsProcessed)

	o.logger.Info("Migration complete",
		zap.String("duration", report.Duration),
		zap.Int("succeeded", report.Succeeded),
		zap.Int("failed", report.Abandoned),
		zap.String("bytes", report.TotalBytesHuman),
	)

	// Write report file
	if err := progress.WriteReport(report, o.cfg.Logging.ReportFile); err != nil {
		o.logger.Error("Failed to write report", zap.Error(err))
	} else {
		o.logger.Info("Report written", zap.String("file", o.cfg.Logging.ReportFile))
	}

	// Cleanup
	if o.workerMgr != nil {
		if err := o.workerMgr.CleanupAll(ctx); err != nil {
			o.logger.Error("Failed to cleanup worker jobs", zap.Error(err))
		}
	}

	if report.Abandoned > 0 {
		return fmt.Errorf("%d tasks failed permanently, see report for details", report.Abandoned)
	}

	return nil
}

// createTasksForMetric discovers cardinality and splits a metric into tasks.
func (o *Orchestrator) createTasksForMetric(ctx context.Context, metric string, tr types.TimeRange) ([]*types.Task, error) {
	maxSeries := o.cfg.EffectiveMaxSeriesForMetric(metric)

	// Extract base selector from filter_match (removing __name__ part if any)
	baseSelector := extractBaseSelector(o.cfg.Migration.FilterMatch)

	result, err := o.splitter.SplitMetric(ctx, metric, baseSelector, maxSeries, tr.Start, tr.End)
	if err != nil {
		return nil, err
	}

	if len(result.Selectors) == 1 {
		intmetrics.SplitOperations.WithLabelValues("no_split").Inc()
	} else {
		intmetrics.SplitOperations.WithLabelValues("split").Inc()
	}

	o.logger.Info("Metric split result",
		zap.String("metric", metric),
		zap.Int("total_series", result.TotalSeries),
		zap.Int("tasks", len(result.Selectors)),
	)

	var tasks []*types.Task
	for _, sel := range result.Selectors {
		o.taskCounter++
		task := &types.Task{
			ID:             fmt.Sprintf("t%d", o.taskCounter),
			MetricName:     metric,
			Selector:       sel.Selector,
			TimeStart:      tr.Start,
			TimeEnd:        tr.End,
			EstSeriesCount: sel.EstSeriesCount,
			Status:         types.TaskStatusPending,
			MaxRetries:     o.cfg.Retry.MaxRetries,
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// executeTasks runs tasks from the queue using K8s Jobs, maintaining
// at most worker_count concurrent Jobs.
func (o *Orchestrator) executeTasks(ctx context.Context) error {
	maxWorkers := o.cfg.Workers.Count

	// Start watching for job completions
	watcher, err := o.workerMgr.WatchJobs(ctx)
	if err != nil {
		return fmt.Errorf("starting job watcher: %w", err)
	}
	defer watcher.Stop()

	// Track active jobs
	activeJobs := make(map[string]string) // jobName -> taskID

	// Fill initial worker pool
	for len(activeJobs) < maxWorkers {
		task := o.queue.NextTask()
		if task == nil {
			break
		}
		if err := o.launchTask(ctx, task, activeJobs); err != nil {
			o.logger.Error("Failed to launch task", zap.String("task_id", task.ID), zap.Error(err))
			o.queue.FailTask(task.ID, err.Error())
		}
	}

	// Process job events until queue is complete
	for !o.queue.IsComplete() {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event, ok := <-watcher.ResultChan():
			if !ok {
				// Watcher channel closed, restart it
				o.logger.Warn("Job watcher closed, restarting")
				watcher, err = o.workerMgr.WatchJobs(ctx)
				if err != nil {
					return fmt.Errorf("restarting job watcher: %w", err)
				}
				continue
			}

			if event.Type != watch.Modified {
				continue
			}

			job, ok := event.Object.(*batchv1.Job)
			if !ok {
				continue
			}

			if !worker.IsJobComplete(job) {
				continue
			}

			taskID := worker.JobTaskID(job)
			if taskID == "" {
				continue
			}

			// Handle job completion
			if worker.IsJobSucceeded(job) {
				o.logger.Info("Task succeeded",
					zap.String("task_id", taskID),
					zap.String("job", job.Name),
				)

				// Try to parse bytes from logs
				bytesTransferred := o.parseJobBytes(ctx, job.Name)
				o.queue.CompleteTask(taskID, bytesTransferred)
				intmetrics.TasksTotal.WithLabelValues("succeeded").Inc()
				intmetrics.BytesTransferred.Add(float64(bytesTransferred))
			} else {
				reason := worker.JobFailureReason(job)
				o.logger.Warn("Task failed",
					zap.String("task_id", taskID),
					zap.String("job", job.Name),
					zap.String("reason", reason),
				)

				retrying := o.queue.FailTask(taskID, reason)
				if retrying {
					intmetrics.TaskRetries.Inc()
				} else {
					intmetrics.TasksTotal.WithLabelValues("abandoned").Inc()
				}
			}

			// Clean up job
			delete(activeJobs, job.Name)
			intmetrics.ActiveWorkers.Set(float64(len(activeJobs)))

			// Delete the completed job
			if err := o.workerMgr.DeleteJob(ctx, job.Name); err != nil {
				o.logger.Warn("Failed to delete job", zap.String("job", job.Name), zap.Error(err))
			}

			// Launch next task if capacity available
			for len(activeJobs) < maxWorkers {
				task := o.queue.NextTask()
				if task == nil {
					break
				}
				if err := o.launchTask(ctx, task, activeJobs); err != nil {
					o.logger.Error("Failed to launch task", zap.String("task_id", task.ID), zap.Error(err))
					o.queue.FailTask(task.ID, err.Error())
				}
			}
		}
	}

	return nil
}

// launchTask creates a K8s Job for a task and tracks it.
func (o *Orchestrator) launchTask(ctx context.Context, task *types.Task, activeJobs map[string]string) error {
	job, err := o.workerMgr.CreateJob(ctx, task)
	if err != nil {
		return err
	}

	activeJobs[job.Name] = task.ID
	task.WorkerID = job.Name
	intmetrics.ActiveWorkers.Set(float64(len(activeJobs)))

	return nil
}

// parseJobBytes tries to extract bytes transferred from vmctl job logs.
func (o *Orchestrator) parseJobBytes(ctx context.Context, jobName string) int64 {
	logs, err := o.workerMgr.GetJobLogs(ctx, jobName)
	if err != nil {
		o.logger.Debug("Could not get job logs for bytes parsing", zap.Error(err))
		return 0
	}

	// vmctl outputs: "total bytes: 7.8 MB"
	// Simple parsing — can be improved
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "total bytes:") {
			parts := strings.Split(line, "total bytes:")
			if len(parts) > 1 {
				bytesStr := strings.TrimSpace(parts[1])
				bytesStr = strings.Split(bytesStr, ";")[0]
				return parseHumanBytes(bytesStr)
			}
		}
	}
	return 0
}

// logDryRunSummary prints a summary of what would be executed.
func (o *Orchestrator) logDryRunSummary(tasks []*types.Task, tr types.TimeRange) {
	o.logger.Info("DRY RUN: Tasks that would be created",
		zap.String("time_range", tr.String()),
		zap.Int("task_count", len(tasks)),
	)

	for _, task := range tasks {
		o.logger.Info("  DRY RUN task",
			zap.String("id", task.ID),
			zap.String("metric", task.MetricName),
			zap.String("selector", task.Selector),
			zap.Int("est_series", task.EstSeriesCount),
		)
	}
}

// extractBaseSelector extracts the non-__name__ part of a filter_match selector.
func extractBaseSelector(filterMatch string) string {
	if filterMatch == "" {
		return ""
	}

	sel := strings.TrimSpace(filterMatch)
	sel = strings.TrimPrefix(sel, "{")
	sel = strings.TrimSuffix(sel, "}")

	// Split matchers and filter out __name__
	var matchers []string
	for _, m := range splitMatchers(sel) {
		m = strings.TrimSpace(m)
		if m == "" || strings.HasPrefix(m, "__name__") {
			continue
		}
		matchers = append(matchers, m)
	}

	return strings.Join(matchers, ",")
}

// splitMatchers splits a comma-separated list of PromQL matchers,
// handling quoted strings correctly.
func splitMatchers(s string) []string {
	var result []string
	var current strings.Builder
	inQuote := false
	escaped := false

	for _, c := range s {
		if escaped {
			current.WriteRune(c)
			escaped = false
			continue
		}
		if c == '\\' {
			escaped = true
			current.WriteRune(c)
			continue
		}
		if c == '"' {
			inQuote = !inQuote
			current.WriteRune(c)
			continue
		}
		if c == ',' && !inQuote {
			result = append(result, current.String())
			current.Reset()
			continue
		}
		current.WriteRune(c)
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// compileExcludePatterns compiles regex patterns from the exclude_metrics config.
func compileExcludePatterns(patterns []string) ([]*regexp.Regexp, error) {
	var compiled []*regexp.Regexp
	for _, p := range patterns {
		re, err := regexp.Compile("^" + p + "$")
		if err != nil {
			return nil, fmt.Errorf("invalid exclude pattern %q: %w", p, err)
		}
		compiled = append(compiled, re)
	}
	return compiled, nil
}

// filterMetrics removes metrics matching any exclude pattern.
func filterMetrics(metrics []string, excludes []*regexp.Regexp) []string {
	if len(excludes) == 0 {
		return metrics
	}

	var filtered []string
	for _, m := range metrics {
		excluded := false
		for _, re := range excludes {
			if re.MatchString(m) {
				excluded = true
				break
			}
		}
		if !excluded {
			filtered = append(filtered, m)
		}
	}
	return filtered
}

// generateMigrationID creates a short deterministic ID from the config.
func generateMigrationID(cfg *config.Config) string {
	h := sha256.New()
	h.Write([]byte(cfg.Source.VmselectURL))
	h.Write([]byte(cfg.Destination.VminsertURL))
	h.Write([]byte(cfg.Migration.StartDate))
	h.Write([]byte(cfg.Migration.EndDate))
	sum := h.Sum(nil)
	return fmt.Sprintf("%x", sum[:6])
}

// parseHumanBytes converts a human-readable bytes string to int64.
func parseHumanBytes(s string) int64 {
	s = strings.TrimSpace(s)
	s = strings.ToUpper(s)

	multipliers := map[string]int64{
		"TB": 1024 * 1024 * 1024 * 1024,
		"GB": 1024 * 1024 * 1024,
		"MB": 1024 * 1024,
		"KB": 1024,
		"B":  1,
	}

	for suffix, mult := range multipliers {
		if strings.HasSuffix(s, suffix) {
			numStr := strings.TrimSpace(strings.TrimSuffix(s, suffix))
			var val float64
			if _, err := fmt.Sscanf(numStr, "%f", &val); err == nil {
				return int64(val * float64(mult))
			}
		}
	}

	return 0
}
