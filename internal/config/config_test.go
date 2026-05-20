package config

import (
	"testing"
	"os"
	"path/filepath"
)

func TestParseDate(t *testing.T) {
	tests := []struct {
		input    string
		wantYear int
		wantMonth int
		wantDay  int
		wantErr  bool
	}{
		{"13.01.2026", 2026, 1, 13, false},
		{"01.12.2025", 2025, 12, 1, false},
		{"2026-01-13", 2026, 1, 13, false},
		{"2026-01-13T00:00:00Z", 2026, 1, 13, false},
		{"invalid", 0, 0, 0, true},
		{"13/01/2026", 0, 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseDate(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseDate(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr {
				if got.Year() != tt.wantYear || int(got.Month()) != tt.wantMonth || got.Day() != tt.wantDay {
					t.Errorf("ParseDate(%q) = %v, want %d-%02d-%02d", tt.input, got, tt.wantYear, tt.wantMonth, tt.wantDay)
				}
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	configYAML := `
source:
  vmselect_url: "http://vmselect:8481/select/0/prometheus"

destination:
  vminsert_url: "http://vminsert:8480/insert/0/prometheus"

migration:
  filter_match: '{__name__!~"vm_.*"}'
  start_date: "13.01.2026"
  end_date: "13.05.2026"
  time_step: "day"
  reverse_order: true

splitting:
  max_series_per_task: 100000
  safety_margin: 0.2

workers:
  count: 5
  namespace: "vm-migration"

logging:
  level: "info"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(configYAML), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error: %v", err)
	}

	if cfg.Source.VmselectURL != "http://vmselect:8481/select/0/prometheus" {
		t.Errorf("unexpected source URL: %s", cfg.Source.VmselectURL)
	}
	if cfg.Migration.TimeStep != "day" {
		t.Errorf("unexpected time step: %s", cfg.Migration.TimeStep)
	}
	if cfg.Workers.Count != 5 {
		t.Errorf("unexpected worker count: %d", cfg.Workers.Count)
	}
	if cfg.Workers.GRPCPort != 9091 {
		t.Errorf("unexpected gRPC port: %d", cfg.Workers.GRPCPort)
	}
	if cfg.Workers.Pod.Image != "vm-migrator:latest" {
		t.Errorf("unexpected worker image: %s", cfg.Workers.Pod.Image)
	}
	if cfg.Workers.Pod.VmctlPath != "/usr/local/bin/vmctl" {
		t.Errorf("unexpected vmctl path: %s", cfg.Workers.Pod.VmctlPath)
	}
	if cfg.EffectiveMaxSeries() != 80000 {
		t.Errorf("unexpected effective max series: %d", cfg.EffectiveMaxSeries())
	}
}

func TestLoadConfigValidationErrors(t *testing.T) {
	configYAML := `
source:
  vmselect_url: ""
destination:
  vminsert_url: ""
migration:
  start_date: ""
  end_date: ""
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(configYAML), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}
