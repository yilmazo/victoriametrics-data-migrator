// Package orchestrator ties together all components to execute the migration.
package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

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

	// Deploy worker pool (if not dry run)
	if !o.dryRun && o.workerMgr != nil {
		o.logger.Info("Deploying worker pool", zap.Int("count", o.cfg.Workers.Count))
		if err := o.workerMgr.DeployWorkers(ctx); err != nil {
			return fmt.Errorf("deploying workers: %w", err)
		}
		intmetrics.ActiveWorkers.Set(float64(o.workerMgr.WorkerCount()))
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

		// 4b: For each metric, create a single optimistic task (no API calls)
		var rangeTasks []*types.Task
		for _, metric := range metrics {
			task := o.createTasksForMetric(metric, tr)
			rangeTasks = append(rangeTasks, task)
			intmetrics.SplitOperations.WithLabelValues("no_split").Inc()
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

	// Cleanup workers
	if o.workerMgr != nil {
		if err := o.workerMgr.Cleanup(ctx); err != nil {
			o.logger.Error("Failed to cleanup workers", zap.Error(err))
		}
	}

	// Log failed task details so they're visible in kubectl logs
	if len(report.FailedTaskDetails) > 0 {
		o.logger.Warn("Permanently failed tasks:")
		for _, ft := range report.FailedTaskDetails {
			o.logger.Warn("  Failed task",
				zap.String("task_id", ft.ID),
				zap.String("selector", ft.Selector),
				zap.String("time_range", ft.TimeRange),
				zap.Int("attempts", ft.Attempts),
				zap.String("error", ft.Error),
			)
		}
	}

	// Print full report JSON to logs (visible via kubectl logs even after container exit)
	reportJSON, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		o.logger.Info("Migration report:\n" + string(reportJSON))
	}

	if report.Abandoned > 0 {
		return fmt.Errorf("%d tasks failed permanently, see logs above for details", report.Abandoned)
	}

	return nil
}

// createTasksForMetric creates a single optimistic task per metric.
// No cardinality analysis is done upfront — if the task fails due to high
// cardinality, it will be split reactively (see resplitFailedTask).
func (o *Orchestrator) createTasksForMetric(metric string, tr types.TimeRange) *types.Task {
	baseSelector := extractBaseSelector(o.cfg.Migration.FilterMatch)
	selector := buildOptimisticSelector(metric, baseSelector)

	o.taskCounter++
	return &types.Task{
		ID:         fmt.Sprintf("t%d", o.taskCounter),
		MetricName: metric,
		Selector:   selector,
		TimeStart:  tr.Start,
		TimeEnd:    tr.End,
		Status:     types.TaskStatusPending,
		MaxRetries: o.cfg.Retry.MaxRetries,
	}
}

// buildOptimisticSelector creates a simple {__name__="metric"} selector,
// optionally including extra matchers from the base filter.
func buildOptimisticSelector(metric string, baseSelector string) string {
	sel := strings.TrimSpace(baseSelector)
	sel = strings.TrimPrefix(sel, "{")
	sel = strings.TrimSuffix(sel, "}")
	sel = strings.TrimSpace(sel)

	if sel != "" {
		return fmt.Sprintf(`{__name__="%s",%s}`, metric, sel)
	}
	return fmt.Sprintf(`{__name__="%s"}`, metric)
}

// isCardinalityRelatedError checks whether the error output from vmctl
// indicates that VictoriaMetrics rejected the request due to series limits.
// VM embeds the actual error text inside vmctl's output — there is no
// reliable HTTP status code to parse.
func isCardinalityRelatedError(errorMsg string) bool {
	if errorMsg == "" {
		return false
	}

	// Known patterns from VictoriaMetrics error responses:
	//   "the number of matching timeseries exceeds ..."
	//   "the number of matching series exceeds ..."
	//   "...search.maxUniqueTimeseries..."
	//   "...search.maxSeries..."
	patterns := []string{
		"timeseries exceeds",
		"series exceeds",
		"search.max",
	}

	lower := strings.ToLower(errorMsg)
	for _, p := range patterns {
		if strings.Contains(lower, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

// resplitFailedTask handles a task that failed due to high cardinality.
func (o *Orchestrator) resplitFailedTask(ctx context.Context, task *types.Task) ([]*types.Task, error) {
	maxSeries := o.cfg.EffectiveMaxSeriesForMetric(task.MetricName)

	fastCount, err := o.client.GetSeriesCountFast(ctx, task.Selector, task.TimeEnd)
	if err != nil {
		o.logger.Warn("Fast count failed, proceeding with split anyway",
			zap.String("metric", task.MetricName),
			zap.Error(err),
		)
		fastCount = maxSeries + 1
	}

	o.logger.Info("Fast cardinality check for failed task",
		zap.String("task_id", task.ID),
		zap.String("metric", task.MetricName),
		zap.Int("fast_count", fastCount),
		zap.Int("max_series", maxSeries),
	)

	if fastCount <= maxSeries {
		o.logger.Info("Series count within limits, not a cardinality issue",
			zap.String("task_id", task.ID),
			zap.Int("count", fastCount),
		)
		return nil, nil
	}

	baseSelector := extractBaseSelector(o.cfg.Migration.FilterMatch)
	result, err := o.splitter.SplitMetric(ctx, task.MetricName, baseSelector, maxSeries, task.TimeStart, task.TimeEnd)
	if err != nil {
		return nil, fmt.Errorf("splitting metric %s: %w", task.MetricName, err)
	}

	if len(result.Selectors) <= 1 {
		o.logger.Warn("Splitter could not break metric into smaller pieces",
			zap.String("metric", task.MetricName),
			zap.Int("total_series", result.TotalSeries),
		)
		return nil, nil
	}

	o.logger.Info("Metric split into sub-tasks",
		zap.String("metric", task.MetricName),
		zap.Int("total_series", result.TotalSeries),
		zap.Int("sub_tasks", len(result.Selectors)),
	)

	intmetrics.SplitOperations.WithLabelValues("split").Inc()

	var subTasks []*types.Task
	for _, sel := range result.Selectors {
		o.taskCounter++
		subTask := &types.Task{
			ID:             fmt.Sprintf("t%d", o.taskCounter),
			MetricName:     task.MetricName,
			Selector:       sel.Selector,
			TimeStart:      task.TimeStart,
			TimeEnd:        task.TimeEnd,
			EstSeriesCount: sel.EstSeriesCount,
			Status:         types.TaskStatusPending,
			MaxRetries:     o.cfg.Retry.MaxRetries,
			SplitAttempted: true,
		}
		subTasks = append(subTasks, subTask)
	}

	return subTasks, nil
}

// executeTasks dispatches tasks to the worker pool until the queue is complete.
func (o *Orchestrator) executeTasks(ctx context.Context) error {
	// Track in-flight tasks
	type inflightTask struct {
		task   *types.Task
		doneCh chan struct{}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	inflight := make(map[string]*inflightTask)

	// Process results channel
	type taskResult struct {
		taskID   string
		result   *worker.TaskResult
		err      error
	}
	resultCh := make(chan taskResult, o.cfg.Workers.Count)

	// dispatchNext tries to dispatch pending tasks to idle workers
	dispatchNext := func() {
		for {
			w := o.workerMgr.AcquireWorker()
			if w == nil {
				break // no idle workers
			}

			task := o.queue.NextTask()
			if task == nil {
				o.workerMgr.ReleaseWorker(w)
				break // no pending tasks
			}

			mu.Lock()
			inflight[task.ID] = &inflightTask{task: task}
			mu.Unlock()

			intmetrics.ActiveWorkers.Set(float64(o.workerMgr.WorkerCount() - o.workerMgr.IdleWorkerCount()))

			wg.Add(1)
			go func(w2 interface{ /* workerConn */ }, t *types.Task) {
				defer wg.Done()
				res, err := o.workerMgr.DispatchTask(ctx, w, t)
				o.workerMgr.ReleaseWorker(w)
				resultCh <- taskResult{taskID: t.ID, result: res, err: err}
			}(w, task)
		}
	}

	// Initial dispatch
	dispatchNext()

	// Process results until queue is complete
	for !o.queue.IsComplete() {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case r := <-resultCh:
			mu.Lock()
			delete(inflight, r.taskID)
			mu.Unlock()

			if r.err != nil {
				// gRPC or network error
				o.logger.Error("Task dispatch error",
					zap.String("task_id", r.taskID),
					zap.Error(r.err),
				)
				retrying := o.queue.FailTask(r.taskID, r.err.Error())
				if retrying {
					intmetrics.TaskRetries.Inc()
				} else {
					intmetrics.TasksTotal.WithLabelValues("abandoned").Inc()
				}
			} else if r.result.ExitCode == 0 {
				// Success
				o.logger.Info("Task succeeded",
					zap.String("task_id", r.taskID),
					zap.Int64("bytes", r.result.BytesTransferred),
				)
				o.queue.CompleteTask(r.taskID, r.result.BytesTransferred)
				intmetrics.TasksTotal.WithLabelValues("succeeded").Inc()
				intmetrics.BytesTransferred.Add(float64(r.result.BytesTransferred))
			} else {
				// vmctl failed
				reason := r.result.ErrorMessage
				if reason == "" {
					reason = fmt.Sprintf("vmctl exited with code %d", r.result.ExitCode)
				}

				// Check both the error message and the full logs for cardinality patterns
				cardinalityIssue := isCardinalityRelatedError(r.result.Logs) || isCardinalityRelatedError(reason)

				o.logger.Warn("Task failed",
					zap.String("task_id", r.taskID),
					zap.String("reason", reason),
					zap.Bool("cardinality_issue", cardinalityIssue),
					zap.Int("exit_code", r.result.ExitCode),
				)

				task := o.queue.GetTask(r.taskID)
				if task != nil && !task.SplitAttempted && cardinalityIssue {
					o.logger.Info("Cardinality limit hit — metric exceeds search.max*, resplitting",
						zap.String("task_id", r.taskID),
						zap.String("metric", task.MetricName),
					)

					subTasks, splitErr := o.resplitFailedTask(ctx, task)
					if splitErr != nil {
						o.logger.Error("Resplit failed", zap.String("task_id", r.taskID), zap.Error(splitErr))
					}

					if len(subTasks) > 0 {
						o.queue.ReplaceTasks(r.taskID, subTasks)
						intmetrics.TasksTotal.WithLabelValues("resplit").Inc()
					} else {
						retrying := o.queue.FailTask(r.taskID, reason)
						if retrying {
							intmetrics.TaskRetries.Inc()
						} else {
							intmetrics.TasksTotal.WithLabelValues("abandoned").Inc()
						}
					}
				} else {
					retrying := o.queue.FailTask(r.taskID, reason)
					if retrying {
						intmetrics.TaskRetries.Inc()
					} else {
						intmetrics.TasksTotal.WithLabelValues("abandoned").Inc()
					}
				}
			}

			intmetrics.ActiveWorkers.Set(float64(o.workerMgr.WorkerCount() - o.workerMgr.IdleWorkerCount()))

			// Dispatch more tasks now that a worker is free
			dispatchNext()
		}
	}

	return nil
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
