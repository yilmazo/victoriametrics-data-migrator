// vm-migrator is a distributed CLI tool for migrating metrics data
// between VictoriaMetrics clusters.
//
// It discovers metrics from a source vmselect, intelligently splits
// high-cardinality metrics into manageable chunks, and distributes
// migration work across Kubernetes worker pods running vmctl.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/yilmazo/victoriametrics-data-migrator/internal/config"
	"github.com/yilmazo/victoriametrics-data-migrator/internal/orchestrator"
)

var (
	version    = "dev"
	configFile string
	dryRun     bool
	logLevel   string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "vm-migrator",
		Short: "Distributed VictoriaMetrics metrics migration tool",
		Long: `vm-migrator orchestrates large-scale metrics migration between VictoriaMetrics clusters.

It discovers metrics from a source vmselect, analyzes cardinality, splits
high-cardinality metrics into manageable chunks, and distributes work across
Kubernetes worker pods running vmctl.`,
		Version: version,
	}

	migrateCmd := &cobra.Command{
		Use:   "migrate",
		Short: "Start the metrics migration",
		Long: `Starts the migration process based on the configuration file.

The migration proceeds in these steps:
1. Split the time range into intervals (day/hour/month)
2. For each interval, discover metrics matching the selector
3. Analyze cardinality and split high-cardinality metrics
4. Create Kubernetes Jobs running vmctl for each task
5. Track progress and report results`,
		RunE: runMigrate,
	}

	migrateCmd.Flags().StringVarP(&configFile, "config", "c", "vm-migrator.yaml", "Path to configuration file")
	migrateCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Discover and plan without executing migration")
	migrateCmd.Flags().StringVar(&logLevel, "log-level", "", "Override log level from config (debug, info, warn, error)")

	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Check the status of a running migration",
		Long:  "Reads the persisted migration state file and displays current progress.",
		RunE:  runStatus,
	}

	statusCmd.Flags().StringVarP(&configFile, "config", "c", "vm-migrator.yaml", "Path to configuration file")

	rootCmd.AddCommand(migrateCmd, statusCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runMigrate(cmd *cobra.Command, args []string) error {
	// Load config
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Override log level if specified via flag
	if logLevel != "" {
		cfg.Logging.Level = logLevel
	}

	// Initialize logger
	logger, err := newLogger(cfg.Logging.Level)
	if err != nil {
		return fmt.Errorf("initializing logger: %w", err)
	}
	defer logger.Sync()

	logger.Info("vm-migrator starting",
		zap.String("version", version),
		zap.String("config", configFile),
		zap.Bool("dry_run", dryRun),
	)

	// Set up context with signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("Received signal, initiating graceful shutdown", zap.String("signal", sig.String()))
		cancel()
	}()

	// Create and run orchestrator
	orch, err := orchestrator.New(cfg, logger, dryRun)
	if err != nil {
		return fmt.Errorf("creating orchestrator: %w", err)
	}

	if err := orch.Run(ctx); err != nil {
		logger.Error("Migration failed", zap.Error(err))
		return err
	}

	return nil
}

func runStatus(cmd *cobra.Command, args []string) error {
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	logger, err := newLogger(cfg.Logging.Level)
	if err != nil {
		return fmt.Errorf("initializing logger: %w", err)
	}
	defer logger.Sync()

	// Try to find and read state file
	// The state file name is based on the migration ID
	stateFiles, _ := os.ReadDir(".")
	for _, f := range stateFiles {
		if !f.IsDir() && len(f.Name()) > 16 && f.Name()[:16] == "migration-state-" {
			logger.Info("Found state file", zap.String("file", f.Name()))

			state, err := os.ReadFile(f.Name())
			if err != nil {
				logger.Error("Failed to read state file", zap.Error(err))
				continue
			}

			fmt.Println(string(state))
		}
	}

	return nil
}

// newLogger creates a zap logger with the specified level.
func newLogger(level string) (*zap.Logger, error) {
	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info":
		zapLevel = zapcore.InfoLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}

	cfg := zap.Config{
		Level:       zap.NewAtomicLevelAt(zapLevel),
		Development: false,
		Encoding:    "console",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "T",
			LevelKey:       "L",
			NameKey:        "N",
			CallerKey:      "",
			MessageKey:     "M",
			StacktraceKey:  "",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.CapitalColorLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	return cfg.Build()
}
