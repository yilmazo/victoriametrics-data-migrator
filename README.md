# victoriametrics-data-migrator

A tool for migrating large volumes of metrics data between VictoriaMetrics clusters.

## Problem

The official `vmctl` tool splits work **by metric name only**. When a metric has high cardinality, a single vmctl task overwhelms the source `vmselect` and hits its limits. Additionally, vmctl cannot parallelize work across multiple workers.

## Solution

`victoriametrics-data-migrator` solves both problems:

1. **Optimistic migration with reactive splitting** — Starts migrating immediately without expensive upfront analysis. If a task fails due to high cardinality, it automatically splits the metric by label values and retries
2. **Distributed execution** — Spawns Kubernetes Jobs running `vmctl` across multiple worker pods, processing tasks in parallel

## How It Works

```
1. Parse YAML config
2. Split time range into intervals (day/hour/month), newest first
3. For each interval:
   a. Discover metric names matching the selector
   b. Create 1 task per metric (no upfront cardinality analysis)
   c. Launch K8s Jobs (up to worker_count concurrent)
   d. On task failure:
      - Check if failure is cardinality-related (maxSeries, timeout, OOM)
      - Fast count via count() query to confirm high cardinality
      - Split by label values using TSDB API + bin-packing
      - Re-queue sub-tasks
   e. Track progress, retry non-cardinality failures
4. Generate final report
```

This **optimistic** approach avoids expensive `/api/v1/status/tsdb` calls for the vast majority of metrics (which are low-cardinality and migrate fine without splitting). Only the small fraction of metrics that actually fail get analyzed and split.

## Quick Start

### Prerequisites

- Go 1.26+
- Access to a Kubernetes cluster (for worker pods)
- Source VictoriaMetrics cluster (vmselect)
- Destination VictoriaMetrics cluster (vminsert)

### Build

```bash
make build
```

### Configure

```bash
cp deploy/examples/config.yaml vm-migrator.yaml
# Edit vm-migrator.yaml with your cluster details
```

### Dry Run

Test your configuration without executing any migration:

```bash
./bin/vm-migrator migrate --config vm-migrator.yaml --dry-run
```

### Run Migration

```bash
# Apply RBAC first
kubectl apply -f deploy/rbac.yaml

# Start migration
./bin/vm-migrator migrate --config vm-migrator.yaml
```

## Configuration

See [deploy/examples/config.yaml](deploy/examples/config.yaml) for a fully documented example.

### Key Settings

| Setting | Description | Default |
|:---|:---|:---|
| `migration.time_step` | Time range split granularity | `day` |
| `migration.reverse_order` | Process newest data first | `true` |
| `splitting.max_series_per_task` | Max series per vmctl task | `100000` |
| `splitting.safety_margin` | Safety margin on max_series | `0.2` |
| `workers.count` | Concurrent K8s worker Jobs | `5` |
| `retry.max_retries` | Retries per failed task | `3` |
| `retry.auto_resplit` | Re-split on failure | `true` |

## Series Splitting Algorithm

Splitting is triggered **reactively** when a vmctl task fails due to high cardinality:

1. Fast pre-check using `count({selector})` to confirm the metric exceeds `max_series_per_task`
2. Query `/api/v1/status/tsdb` with `focusLabel` to get series distribution per label value
3. Pick the label with the best splitting characteristics (most values, even distribution)
4. Bin-pack label values into groups ≤ `max_series_per_task`
5. Generate PromQL selectors with `=~` regex matchers
6. If any single label value still exceeds the limit, recurse with the next label
7. Sub-tasks are marked `SplitAttempted` to prevent infinite resplitting

## Monitoring

Enable the Prometheus metrics endpoint:

```yaml
monitoring:
  enabled: true
  address: ":9090"
```

Exposed metrics:
- `vm_migrator_tasks_total{status}` — Task counts by status
- `vm_migrator_bytes_transferred_total` — Total bytes migrated
- `vm_migrator_active_workers` — Currently running worker Jobs
- `vm_migrator_time_ranges_processed` — Completed time ranges
- `vm_migrator_task_duration_seconds` — Task execution time histogram

## E2E Testing

Run the end-to-end test suite using minikube and Podman:

```bash
make e2e            # Full run (minikube → deploy → data → migrate → verify)
make e2e-rerun      # Skip minikube setup, reuse existing cluster
make e2e-cleanup    # Delete minikube cluster
```

The e2e test generates 10,310 high-cardinality time series, migrates them across two VictoriaMetrics instances, and verifies series counts match.

## Project Structure

```
├── cmd/vm-migrator/        # CLI entry point
├── internal/
│   ├── config/             # YAML config parsing & validation
│   ├── discovery/          # VictoriaMetrics API client
│   ├── splitter/           # Series selector splitting algorithm
│   ├── scheduler/          # Time range splitting & task queue
│   ├── worker/             # K8s Job lifecycle management
│   ├── orchestrator/       # Main workflow coordinator
│   ├── progress/           # Progress tracking & reporting
│   ├── metrics/            # Prometheus metrics
│   └── types/              # Shared types
├── e2e/                    # End-to-end test suite
│   ├── run_e2e.sh          # Test orchestration script
│   ├── teardown.sh         # Cleanup script
│   ├── Dockerfile          # E2E image (migrator + datagen)
│   ├── config.yaml         # Migration config for tests
│   ├── datagen/            # High-cardinality data generator
│   └── manifests/          # K8s manifests for test VMs
├── deploy/
│   ├── rbac.yaml           # K8s RBAC for coordinator & workers
│   └── examples/config.yaml
├── Makefile
└── README.md
```

## License

MIT
