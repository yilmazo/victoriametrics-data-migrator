// Package config handles YAML configuration parsing and validation for vm-migrator.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for vm-migrator.
type Config struct {
	Source      SourceConfig      `yaml:"source"`
	Destination DestinationConfig `yaml:"destination"`
	Migration  MigrationConfig   `yaml:"migration"`
	Splitting  SplittingConfig   `yaml:"splitting"`
	Workers    WorkersConfig     `yaml:"workers"`
	Retry      RetryConfig       `yaml:"retry"`
	Logging    LoggingConfig     `yaml:"logging"`
	Monitoring MonitoringConfig  `yaml:"monitoring"`

	// Optional filters
	ExcludeMetrics  []string                  `yaml:"exclude_metrics,omitempty"`
	MetricOverrides map[string]MetricOverride `yaml:"metric_overrides,omitempty"`
}

// SourceConfig defines the source VictoriaMetrics cluster connection.
type SourceConfig struct {
	VmselectURL string            `yaml:"vmselect_url"`
	BearerToken string            `yaml:"bearer_token,omitempty"`
	BasicAuth   BasicAuthConfig   `yaml:"basic_auth,omitempty"`
	Headers     map[string]string `yaml:"headers,omitempty"`
	TLS         TLSConfig         `yaml:"tls,omitempty"`
}

// DestinationConfig defines the destination VictoriaMetrics cluster connection.
type DestinationConfig struct {
	VminsertURL string            `yaml:"vminsert_url"`
	BearerToken string            `yaml:"bearer_token,omitempty"`
	BasicAuth   BasicAuthConfig   `yaml:"basic_auth,omitempty"`
	Headers     map[string]string `yaml:"headers,omitempty"`
	TLS         TLSConfig         `yaml:"tls,omitempty"`
}

// BasicAuthConfig holds basic auth credentials.
type BasicAuthConfig struct {
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
}

// TLSConfig holds TLS-related configuration.
type TLSConfig struct {
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty"`
	CAFile             string `yaml:"ca_file,omitempty"`
	CertFile           string `yaml:"cert_file,omitempty"`
	KeyFile            string `yaml:"key_file,omitempty"`
}

// MigrationConfig defines the migration parameters.
type MigrationConfig struct {
	FilterMatch  string `yaml:"filter_match"`
	StartDate    string `yaml:"start_date"`
	EndDate      string `yaml:"end_date"`
	TimeStep     string `yaml:"time_step"`
	ReverseOrder bool   `yaml:"reverse_order"`
}

// SplittingConfig controls how high-cardinality metrics are split.
type SplittingConfig struct {
	MaxSeriesPerTask    int      `yaml:"max_series_per_task"`
	SafetyMargin        float64  `yaml:"safety_margin"`
	MaxRegexValues      int      `yaml:"max_regex_values"`
	PreferredSplitLabels []string `yaml:"preferred_split_labels,omitempty"`
	ExcludeSplitLabels  []string `yaml:"exclude_split_labels,omitempty"`
}

// WorkersConfig defines Kubernetes worker pod settings.
type WorkersConfig struct {
	Count     int             `yaml:"count"`
	Namespace string          `yaml:"namespace"`
	GRPCPort  int             `yaml:"grpc_port"`
	Pod       PodConfig       `yaml:"pod"`
	Vmctl     VmctlConfig     `yaml:"vmctl"`
}

// PodConfig holds Kubernetes pod spec configuration.
type PodConfig struct {
	Image           string            `yaml:"image"`
	ImagePullPolicy string            `yaml:"image_pull_policy,omitempty"`
	ImagePullSecret string            `yaml:"image_pull_secret,omitempty"`
	VmctlPath       string            `yaml:"vmctl_path,omitempty"`
	Resources       ResourcesConfig   `yaml:"resources,omitempty"`
	NodeSelector    map[string]string `yaml:"node_selector,omitempty"`
	Tolerations     []Toleration      `yaml:"tolerations,omitempty"`
	ServiceAccount  string            `yaml:"service_account,omitempty"`
}

// ResourcesConfig holds K8s resource requests and limits.
type ResourcesConfig struct {
	Requests ResourceSpec `yaml:"requests,omitempty"`
	Limits   ResourceSpec `yaml:"limits,omitempty"`
}

// ResourceSpec holds CPU and memory specifications.
type ResourceSpec struct {
	CPU    string `yaml:"cpu,omitempty"`
	Memory string `yaml:"memory,omitempty"`
}

// Toleration represents a Kubernetes toleration.
type Toleration struct {
	Key      string `yaml:"key,omitempty"`
	Operator string `yaml:"operator,omitempty"`
	Value    string `yaml:"value,omitempty"`
	Effect   string `yaml:"effect,omitempty"`
}

// VmctlConfig holds vmctl-specific settings per worker.
type VmctlConfig struct {
	Concurrency           int      `yaml:"concurrency"`
	RateLimit             int      `yaml:"rate_limit"`
	DisableBinaryProtocol bool     `yaml:"disable_binary_protocol"`
	DisableHTTPKeepAlive  bool     `yaml:"disable_http_keep_alive"`
	BackoffRetries        int      `yaml:"backoff_retries"`
	ExtraLabels           []string `yaml:"extra_labels,omitempty"`
}

// RetryConfig defines retry behavior for failed tasks.
type RetryConfig struct {
	MaxRetries    int     `yaml:"max_retries"`
	AutoResplit   bool    `yaml:"auto_resplit"`
	ResplitFactor float64 `yaml:"resplit_factor"`
}

// LoggingConfig defines logging behavior.
type LoggingConfig struct {
	ProgressInterval string `yaml:"progress_interval"`
	Level            string `yaml:"level"`
	ReportFile       string `yaml:"report_file"`
}

// MonitoringConfig defines Prometheus metrics endpoint settings.
type MonitoringConfig struct {
	Enabled bool   `yaml:"enabled"`
	Address string `yaml:"address"`
}

// MetricOverride allows per-metric configuration overrides.
type MetricOverride struct {
	MaxSeriesPerTask     int      `yaml:"max_series_per_task,omitempty"`
	PreferredSplitLabels []string `yaml:"preferred_split_labels,omitempty"`
}

// LoadConfig reads and parses a YAML configuration file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	applyDefaults(cfg)

	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return cfg, nil
}

// applyDefaults sets default values for optional configuration fields.
func applyDefaults(cfg *Config) {
	if cfg.Migration.TimeStep == "" {
		cfg.Migration.TimeStep = "day"
	}
	if cfg.Splitting.MaxSeriesPerTask == 0 {
		cfg.Splitting.MaxSeriesPerTask = 100000
	}
	if cfg.Splitting.SafetyMargin == 0 {
		cfg.Splitting.SafetyMargin = 0.2
	}
	if cfg.Splitting.MaxRegexValues == 0 {
		cfg.Splitting.MaxRegexValues = 200
	}
	if len(cfg.Splitting.ExcludeSplitLabels) == 0 {
		cfg.Splitting.ExcludeSplitLabels = []string{"__name__"}
	}
	if cfg.Workers.Count == 0 {
		cfg.Workers.Count = 5
	}
	if cfg.Workers.Namespace == "" {
		cfg.Workers.Namespace = "vm-migration"
	}
	if cfg.Workers.Pod.Image == "" {
		cfg.Workers.Pod.Image = "vm-migrator:latest"
	}
	if cfg.Workers.Pod.ImagePullPolicy == "" {
		cfg.Workers.Pod.ImagePullPolicy = "IfNotPresent"
	}
	if cfg.Workers.Pod.ServiceAccount == "" {
		cfg.Workers.Pod.ServiceAccount = "vm-migrator-worker"
	}
	if cfg.Workers.Pod.VmctlPath == "" {
		cfg.Workers.Pod.VmctlPath = "/usr/local/bin/vmctl"
	}
	if cfg.Workers.GRPCPort == 0 {
		cfg.Workers.GRPCPort = 9091
	}
	if cfg.Workers.Vmctl.Concurrency == 0 {
		cfg.Workers.Vmctl.Concurrency = 2
	}
	if cfg.Workers.Vmctl.BackoffRetries == 0 {
		cfg.Workers.Vmctl.BackoffRetries = 10
	}
	if cfg.Retry.MaxRetries == 0 {
		cfg.Retry.MaxRetries = 3
	}
	if cfg.Retry.ResplitFactor == 0 {
		cfg.Retry.ResplitFactor = 0.5
	}
	if cfg.Logging.ProgressInterval == "" {
		cfg.Logging.ProgressInterval = "30s"
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
	if cfg.Logging.ReportFile == "" {
		cfg.Logging.ReportFile = "migration_report.json"
	}
	if cfg.Monitoring.Address == "" {
		cfg.Monitoring.Address = ":9090"
	}
}

// validateConfig checks required fields and value validity.
func validateConfig(cfg *Config) error {
	var errs []string

	if cfg.Source.VmselectURL == "" {
		errs = append(errs, "source.vmselect_url is required")
	}
	if cfg.Destination.VminsertURL == "" {
		errs = append(errs, "destination.vminsert_url is required")
	}
	if cfg.Migration.StartDate == "" {
		errs = append(errs, "migration.start_date is required")
	}
	if cfg.Migration.EndDate == "" {
		errs = append(errs, "migration.end_date is required")
	}

	// Validate time step
	validSteps := map[string]bool{"day": true, "hour": true, "month": true}
	if !validSteps[cfg.Migration.TimeStep] {
		errs = append(errs, fmt.Sprintf("migration.time_step must be one of: day, hour, month (got: %s)", cfg.Migration.TimeStep))
	}

	// Validate dates are parseable
	if cfg.Migration.StartDate != "" {
		if _, err := ParseDate(cfg.Migration.StartDate); err != nil {
			errs = append(errs, fmt.Sprintf("migration.start_date: %v", err))
		}
	}
	if cfg.Migration.EndDate != "" {
		if _, err := ParseDate(cfg.Migration.EndDate); err != nil {
			errs = append(errs, fmt.Sprintf("migration.end_date: %v", err))
		}
	}

	// Validate parsed dates order
	if cfg.Migration.StartDate != "" && cfg.Migration.EndDate != "" {
		start, err1 := ParseDate(cfg.Migration.StartDate)
		end, err2 := ParseDate(cfg.Migration.EndDate)
		if err1 == nil && err2 == nil && !start.Before(end) {
			errs = append(errs, "migration.start_date must be before migration.end_date")
		}
	}

	// Validate splitting config
	if cfg.Splitting.MaxSeriesPerTask < 100 {
		errs = append(errs, "splitting.max_series_per_task should be at least 100")
	}
	if cfg.Splitting.SafetyMargin < 0 || cfg.Splitting.SafetyMargin >= 1 {
		errs = append(errs, "splitting.safety_margin must be between 0.0 and 1.0 (exclusive)")
	}

	// Validate worker count
	if cfg.Workers.Count < 1 {
		errs = append(errs, "workers.count must be at least 1")
	}

	// Validate logging level
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[cfg.Logging.Level] {
		errs = append(errs, fmt.Sprintf("logging.level must be one of: debug, info, warn, error (got: %s)", cfg.Logging.Level))
	}

	// Validate progress interval
	if _, err := time.ParseDuration(cfg.Logging.ProgressInterval); err != nil {
		errs = append(errs, fmt.Sprintf("logging.progress_interval: invalid duration: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("configuration errors:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

// ParseDate parses a date string in DD.MM.YYYY or RFC3339 format.
func ParseDate(s string) (time.Time, error) {
	// Try DD.MM.YYYY format first
	if t, err := time.Parse("02.01.2006", s); err == nil {
		return t, nil
	}
	// Try RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	// Try date-only ISO format
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unable to parse date %q (supported formats: DD.MM.YYYY, YYYY-MM-DD, RFC3339)", s)
}

// EffectiveMaxSeries returns the max series per task after applying the safety margin.
func (cfg *Config) EffectiveMaxSeries() int {
	return int(float64(cfg.Splitting.MaxSeriesPerTask) * (1.0 - cfg.Splitting.SafetyMargin))
}

// EffectiveMaxSeriesForMetric returns per-metric max or the global default.
func (cfg *Config) EffectiveMaxSeriesForMetric(metricName string) int {
	if override, ok := cfg.MetricOverrides[metricName]; ok && override.MaxSeriesPerTask > 0 {
		return int(float64(override.MaxSeriesPerTask) * (1.0 - cfg.Splitting.SafetyMargin))
	}
	return cfg.EffectiveMaxSeries()
}

// SplitLabelsForMetric returns the preferred split labels for a metric.
func (cfg *Config) SplitLabelsForMetric(metricName string) []string {
	if override, ok := cfg.MetricOverrides[metricName]; ok && len(override.PreferredSplitLabels) > 0 {
		return override.PreferredSplitLabels
	}
	return cfg.Splitting.PreferredSplitLabels
}
