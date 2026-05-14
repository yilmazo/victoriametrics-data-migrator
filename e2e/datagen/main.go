// datagen generates high-cardinality timeseries data and imports it into
// a VictoriaMetrics instance. Designed for e2e testing of vm-migrator.
//
// It creates 3 metrics with varying cardinality to exercise the splitting algorithm:
//   - e2e_requests_total:    10,000 series (forces splitting)
//   - e2e_histogram_bucket:  300 series   (may split depending on threshold)
//   - e2e_simple_gauge:      10 series    (no splitting needed)
//
// Data is generated for a configurable number of days with 30-minute intervals.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"time"
)

// importEntry represents a single JSON line for /api/v1/import.
type importEntry struct {
	Metric     map[string]string `json:"metric"`
	Values     []float64         `json:"values"`
	Timestamps []int64           `json:"timestamps"`
}

func main() {
	var (
		vmURL    string
		days     int
		interval int
		dryRun   bool
	)
	flag.StringVar(&vmURL, "vm-url", envOrDefault("VM_URL", "http://localhost:8428"), "VictoriaMetrics URL")
	flag.IntVar(&days, "days", intEnvOrDefault("DAYS", 3), "Number of days of data to generate")
	flag.IntVar(&interval, "interval", intEnvOrDefault("INTERVAL_MINUTES", 30), "Data point interval in minutes")
	flag.BoolVar(&dryRun, "dry-run", false, "Print stats without sending data")
	flag.Parse()

	endTime := time.Now().UTC().Truncate(time.Hour)
	startTime := endTime.AddDate(0, 0, -days)

	log.Printf("Data generation config:")
	log.Printf("  VM URL:     %s", vmURL)
	log.Printf("  Start:      %s", startTime.Format(time.RFC3339))
	log.Printf("  End:        %s", endTime.Format(time.RFC3339))
	log.Printf("  Interval:   %d minutes", interval)

	// Generate timestamps
	timestamps := generateTimestamps(startTime, endTime, time.Duration(interval)*time.Minute)
	log.Printf("  Points/series: %d", len(timestamps))

	// Define metrics
	metrics := []metricDef{
		highCardinalityMetric(),
		histogramMetric(),
		simpleGaugeMetric(),
	}

	totalSeries := 0
	for _, m := range metrics {
		count := m.seriesCount()
		totalSeries += count
		log.Printf("  Metric %-40s: %6d series", m.name, count)
	}
	log.Printf("  Total series:  %d", totalSeries)
	log.Printf("  Total points:  %d", totalSeries*len(timestamps))

	if dryRun {
		log.Println("Dry run mode, exiting.")
		return
	}

	// Generate and import data
	client := &http.Client{Timeout: 5 * time.Minute}

	for _, m := range metrics {
		log.Printf("Generating metric: %s (%d series)...", m.name, m.seriesCount())
		if err := generateAndImport(client, vmURL, m, timestamps); err != nil {
			log.Fatalf("Failed to generate %s: %v", m.name, err)
		}
		log.Printf("  Done: %s", m.name)
	}

	// Force flush
	log.Println("Forcing flush...")
	resp, err := http.Get(vmURL + "/internal/force_flush")
	if err != nil {
		log.Printf("Warning: force flush failed: %v", err)
	} else {
		resp.Body.Close()
	}

	// Verify by querying series count
	log.Println("Verifying data...")
	time.Sleep(2 * time.Second) // Wait for indexing
	verifyData(client, vmURL, startTime, endTime)

	log.Println("Data generation complete!")
}

type metricDef struct {
	name   string
	labels []labelDef
	valGen func(seriesIdx int, tsIdx int) float64
}

type labelDef struct {
	name   string
	values []string
}

func (m metricDef) seriesCount() int {
	count := 1
	for _, l := range m.labels {
		count *= len(l.values)
	}
	return count
}

// highCardinalityMetric creates 10,000 series: 20 instances × 25 paths × 4 methods × 5 statuses
func highCardinalityMetric() metricDef {
	instances := makeValues("inst", 20)
	paths := makeValues("/api/v1/path", 25)
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	statuses := []string{"200", "201", "400", "404", "500"}

	return metricDef{
		name: "e2e_requests_total",
		labels: []labelDef{
			{name: "instance", values: instances},
			{name: "path", values: paths},
			{name: "method", values: methods},
			{name: "status", values: statuses},
		},
		valGen: func(seriesIdx, tsIdx int) float64 {
			// Monotonically increasing counter with some jitter
			return float64(tsIdx*10 + seriesIdx%100)
		},
	}
}

// histogramMetric creates 300 series: 10 instances × 3 methods × 10 le buckets
func histogramMetric() metricDef {
	instances := makeValues("inst", 10)
	methods := []string{"GET", "POST", "PUT"}
	le := []string{"0.001", "0.005", "0.01", "0.05", "0.1", "0.5", "1", "5", "10", "+Inf"}

	return metricDef{
		name: "e2e_histogram_bucket",
		labels: []labelDef{
			{name: "instance", values: instances},
			{name: "method", values: methods},
			{name: "le", values: le},
		},
		valGen: func(seriesIdx, tsIdx int) float64 {
			return float64(tsIdx * (seriesIdx%10 + 1))
		},
	}
}

// simpleGaugeMetric creates 10 series: 10 hosts
func simpleGaugeMetric() metricDef {
	hosts := makeValues("host", 10)
	return metricDef{
		name: "e2e_simple_gauge",
		labels: []labelDef{
			{name: "host", values: hosts},
		},
		valGen: func(seriesIdx, tsIdx int) float64 {
			return 50.0 + 20.0*math.Sin(float64(tsIdx)/10.0) + rand.Float64()*5.0
		},
	}
}

func makeValues(prefix string, count int) []string {
	vals := make([]string, count)
	for i := 0; i < count; i++ {
		vals[i] = fmt.Sprintf("%s-%03d", prefix, i+1)
	}
	return vals
}

func generateTimestamps(start, end time.Time, interval time.Duration) []int64 {
	var ts []int64
	for t := start; t.Before(end); t = t.Add(interval) {
		ts = append(ts, t.UnixMilli())
	}
	return ts
}

// generateAndImport creates all series for a metric and sends them in batches.
func generateAndImport(client *http.Client, vmURL string, m metricDef, timestamps []int64) error {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	batchSize := 0
	totalSent := 0
	totalSeries := m.seriesCount()

	// Generate all label combinations
	combos := generateLabelCombinations(m.labels)

	for seriesIdx, combo := range combos {
		// Build metric labels
		labels := map[string]string{"__name__": m.name}
		for k, v := range combo {
			labels[k] = v
		}

		// Generate values for all timestamps
		values := make([]float64, len(timestamps))
		for i := range timestamps {
			values[i] = m.valGen(seriesIdx, i)
		}

		entry := importEntry{
			Metric:     labels,
			Values:     values,
			Timestamps: timestamps,
		}

		if err := encoder.Encode(entry); err != nil {
			return fmt.Errorf("encoding entry: %w", err)
		}
		batchSize++

		// Send in batches of 500 series
		if batchSize >= 500 {
			if err := sendBatch(client, vmURL, buf.Bytes()); err != nil {
				return fmt.Errorf("sending batch: %w", err)
			}
			totalSent += batchSize
			if totalSent%2000 == 0 {
				log.Printf("    Progress: %d/%d series (%.1f%%)", totalSent, totalSeries, float64(totalSent)/float64(totalSeries)*100)
			}
			buf.Reset()
			batchSize = 0
		}
	}

	// Send remaining
	if batchSize > 0 {
		if err := sendBatch(client, vmURL, buf.Bytes()); err != nil {
			return fmt.Errorf("sending final batch: %w", err)
		}
		totalSent += batchSize
	}

	log.Printf("    Sent %d series total", totalSent)
	return nil
}

// generateLabelCombinations creates the cartesian product of all label values.
func generateLabelCombinations(labels []labelDef) []map[string]string {
	if len(labels) == 0 {
		return []map[string]string{{}}
	}

	first := labels[0]
	rest := generateLabelCombinations(labels[1:])

	var combos []map[string]string
	for _, val := range first.values {
		for _, r := range rest {
			combo := make(map[string]string, len(r)+1)
			for k, v := range r {
				combo[k] = v
			}
			combo[first.name] = val
			combos = append(combos, combo)
		}
	}
	return combos
}

func sendBatch(client *http.Client, vmURL string, data []byte) error {
	url := vmURL + "/api/v1/import"
	resp, err := client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func verifyData(client *http.Client, vmURL string, start, end time.Time) {
	url := fmt.Sprintf("%s/api/v1/series/count", vmURL)
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("Warning: could not verify series count: %v", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	log.Printf("Series count response: %s", string(body))

	// Also verify specific metrics
	for _, name := range []string{"e2e_requests_total", "e2e_histogram_bucket", "e2e_simple_gauge"} {
		checkURL := fmt.Sprintf("%s/api/v1/label/__name__/values?match[]={__name__=%q}&start=%d&end=%d",
			vmURL, name, start.Unix(), end.Unix())
		resp2, err := client.Get(checkURL)
		if err != nil {
			log.Printf("Warning: could not verify %s: %v", name, err)
			continue
		}
		body2, _ := io.ReadAll(resp2.Body)
		resp2.Body.Close()
		log.Printf("  %s: %s", name, string(body2))
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func intEnvOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var i int
		if _, err := fmt.Sscanf(v, "%d", &i); err == nil {
			return i
		}
	}
	return def
}
