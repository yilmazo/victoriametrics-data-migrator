package splitter

import (
	"testing"

	"github.com/yilmazo/victoriametrics-data-migrator/internal/types"
)

func TestBinPack_AllFitInOne(t *testing.T) {
	values := []types.LabelValueCount{
		{Name: "a", Value: 1000},
		{Name: "b", Value: 2000},
		{Name: "c", Value: 3000},
	}

	bins, oversized := BinPack(values, 10000, 200)

	if len(oversized) != 0 {
		t.Errorf("expected 0 oversized, got %d", len(oversized))
	}
	if len(bins) != 1 {
		t.Errorf("expected 1 bin, got %d", len(bins))
	}
	if bins[0].SeriesCount != 6000 {
		t.Errorf("expected bin series count 6000, got %d", bins[0].SeriesCount)
	}
}

func TestBinPack_MultipleBins(t *testing.T) {
	values := []types.LabelValueCount{
		{Name: "a", Value: 5000},
		{Name: "b", Value: 5000},
		{Name: "c", Value: 5000},
		{Name: "d", Value: 5000},
	}

	bins, oversized := BinPack(values, 10000, 200)

	if len(oversized) != 0 {
		t.Errorf("expected 0 oversized, got %d", len(oversized))
	}
	if len(bins) != 2 {
		t.Errorf("expected 2 bins, got %d", len(bins))
	}
}

func TestBinPack_OversizedValues(t *testing.T) {
	values := []types.LabelValueCount{
		{Name: "big", Value: 150000},
		{Name: "small1", Value: 5000},
		{Name: "small2", Value: 3000},
	}

	bins, oversized := BinPack(values, 100000, 200)

	if len(oversized) != 1 {
		t.Fatalf("expected 1 oversized, got %d", len(oversized))
	}
	if oversized[0].Name != "big" {
		t.Errorf("expected oversized value 'big', got %q", oversized[0].Name)
	}
	if len(bins) != 1 {
		t.Errorf("expected 1 bin for small values, got %d", len(bins))
	}
}

func TestBinPack_MaxValuesLimit(t *testing.T) {
	// Create 10 values, each 1000 series, max 3 values per bin
	var values []types.LabelValueCount
	for i := 0; i < 10; i++ {
		values = append(values, types.LabelValueCount{
			Name:  string(rune('a' + i)),
			Value: 1000,
		})
	}

	bins, oversized := BinPack(values, 100000, 3)

	if len(oversized) != 0 {
		t.Errorf("expected 0 oversized, got %d", len(oversized))
	}
	// With max 3 per bin and 10 values: ceil(10/3) = 4 bins
	if len(bins) != 4 {
		t.Errorf("expected 4 bins (max 3 values each), got %d", len(bins))
	}
	for _, bin := range bins {
		if len(bin.LabelValues) > 3 {
			t.Errorf("bin has %d values, exceeds max 3", len(bin.LabelValues))
		}
	}
}

func TestBinPack_Empty(t *testing.T) {
	bins, oversized := BinPack(nil, 100000, 200)
	if bins != nil || oversized != nil {
		t.Errorf("expected nil results for empty input")
	}
}

func TestBinPack_SkipsZeroValues(t *testing.T) {
	values := []types.LabelValueCount{
		{Name: "a", Value: 5000},
		{Name: "b", Value: 0},
		{Name: "c", Value: 3000},
	}

	bins, _ := BinPack(values, 100000, 200)

	totalValues := 0
	for _, b := range bins {
		totalValues += len(b.LabelValues)
	}
	if totalValues != 2 {
		t.Errorf("expected 2 values (skipping zero), got %d", totalValues)
	}
}

func TestEscapeRegex(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"hello.world", `hello\.world`},
		{"a|b", `a\|b`},
		{"value(1)", `value\(1\)`},
		{"no-special", "no-special"},
		{"100%", "100%"},
		{"a*b+c", `a\*b\+c`},
	}

	for _, tt := range tests {
		got := escapeRegex(tt.input)
		if got != tt.want {
			t.Errorf("escapeRegex(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestBuildMatch(t *testing.T) {
	tests := []struct {
		metric   string
		selector string
		want     string
	}{
		{"http_requests_total", "", `{__name__="http_requests_total"}`},
		{"http_requests_total", `instance="prod-01"`, `{__name__="http_requests_total",instance="prod-01"}`},
		{"http_requests_total", `{instance="prod-01"}`, `{__name__="http_requests_total",instance="prod-01"}`},
	}

	for _, tt := range tests {
		got := buildMatch(tt.metric, tt.selector)
		if got != tt.want {
			t.Errorf("buildMatch(%q, %q) = %q, want %q", tt.metric, tt.selector, got, tt.want)
		}
	}
}

func TestJoinSelector(t *testing.T) {
	tests := []struct {
		base    string
		matcher string
		want    string
	}{
		{"", `instance="prod"`, `instance="prod"`},
		{`job="api"`, `instance="prod"`, `job="api",instance="prod"`},
		{`{job="api"}`, `instance="prod"`, `job="api",instance="prod"`},
	}

	for _, tt := range tests {
		got := joinSelector(tt.base, tt.matcher)
		if got != tt.want {
			t.Errorf("joinSelector(%q, %q) = %q, want %q", tt.base, tt.matcher, got, tt.want)
		}
	}
}

func TestScoreLabelDistribution(t *testing.T) {
	// Even distribution should score higher than skewed
	even := []types.LabelValueCount{
		{Name: "a", Value: 100},
		{Name: "b", Value: 100},
		{Name: "c", Value: 100},
		{Name: "d", Value: 100},
	}

	skewed := []types.LabelValueCount{
		{Name: "big", Value: 1000},
		{Name: "small", Value: 10},
	}

	evenScore := scoreLabelDistribution(even)
	skewedScore := scoreLabelDistribution(skewed)

	if evenScore <= skewedScore {
		t.Errorf("even distribution (score=%.2f) should score higher than skewed (score=%.2f)",
			evenScore, skewedScore)
	}
}
