// Package splitter implements the series selector splitting algorithm
// for partitioning high-cardinality metrics into manageable chunks.
package splitter

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/yilmazo/victoriametrics-data-migrator/internal/discovery"
	"github.com/yilmazo/victoriametrics-data-migrator/internal/types"
)

// Splitter analyzes metrics and generates series selectors that each match
// at most maxSeries time series.
type Splitter struct {
	client         *discovery.Client
	maxRegexValues int
	excludeLabels  map[string]bool
	preferLabels   []string
	logger         *zap.Logger
}

// New creates a new Splitter.
func New(client *discovery.Client, maxRegexValues int, excludeLabels []string, preferLabels []string, logger *zap.Logger) *Splitter {
	excl := make(map[string]bool)
	for _, l := range excludeLabels {
		excl[l] = true
	}
	return &Splitter{
		client:         client,
		maxRegexValues: maxRegexValues,
		excludeLabels:  excl,
		preferLabels:   preferLabels,
		logger:         logger,
	}
}

// SplitResult holds the output of the splitting process for a single metric.
type SplitResult struct {
	MetricName string
	Selectors  []SelectorInfo
	TotalSeries int
}

// SelectorInfo contains a series selector and its estimated series count.
type SelectorInfo struct {
	Selector       string
	EstSeriesCount int
}

// SplitMetric analyzes a metric's cardinality and produces selectors that
// each match at most maxSeries time series.
func (s *Splitter) SplitMetric(ctx context.Context, metric string, baseSelector string, maxSeries int, start, end time.Time) (*SplitResult, error) {
	match := buildMatch(metric, baseSelector)

	// Step 1: Get total series count
	totalSeries, err := s.client.GetSeriesCount(ctx, match, start, end)
	if err != nil {
		return nil, fmt.Errorf("getting series count for %s: %w", metric, err)
	}

	s.logger.Info("Metric series count",
		zap.String("metric", metric),
		zap.Int("total_series", totalSeries),
		zap.Int("max_series", maxSeries),
	)

	// If within limits, return single selector
	if totalSeries <= maxSeries {
		return &SplitResult{
			MetricName:  metric,
			TotalSeries: totalSeries,
			Selectors: []SelectorInfo{{
				Selector:       match,
				EstSeriesCount: totalSeries,
			}},
		}, nil
	}

	// Step 2: Recursive split
	selectors, err := s.recursiveSplit(ctx, metric, baseSelector, maxSeries, start, end, nil)
	if err != nil {
		return nil, err
	}

	return &SplitResult{
		MetricName:  metric,
		TotalSeries: totalSeries,
		Selectors:   selectors,
	}, nil
}

// recursiveSplit performs recursive label-based partitioning.
func (s *Splitter) recursiveSplit(ctx context.Context, metric string, baseSelector string, maxSeries int, start, end time.Time, usedLabels []string) ([]SelectorInfo, error) {
	match := buildMatch(metric, baseSelector)

	// Discover available labels
	labels, err := s.client.DiscoverLabels(ctx, metric, baseSelector, start, end)
	if err != nil {
		return nil, fmt.Errorf("discovering labels for %s: %w", metric, err)
	}

	// Filter labels: remove excluded and already-used labels
	candidateLabels := s.filterLabels(labels, usedLabels)
	if len(candidateLabels) == 0 {
		s.logger.Warn("No more labels to split by, returning oversized selector",
			zap.String("metric", metric),
			zap.String("selector", match),
		)
		total, _ := s.client.GetSeriesCount(ctx, match, start, end)
		return []SelectorInfo{{Selector: match, EstSeriesCount: total}}, nil
	}

	// Step 3: Find the best split label
	bestLabel, distribution, err := s.findBestSplitLabel(ctx, metric, baseSelector, candidateLabels, start, end)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Split label selected",
		zap.String("metric", metric),
		zap.String("label", bestLabel),
		zap.Int("distinct_values", len(distribution)),
	)

	// Step 4: Bin-pack label values
	bins, oversized := BinPack(distribution, maxSeries, s.maxRegexValues)

	s.logger.Info("Bin-packing result",
		zap.String("metric", metric),
		zap.Int("bins", len(bins)),
		zap.Int("oversized_values", len(oversized)),
	)

	var selectors []SelectorInfo

	// Generate selectors for normal bins
	for _, bin := range bins {
		selector := appendRegexSelector(baseSelector, bestLabel, bin.LabelValues)
		match := buildMatch(metric, selector)
		selectors = append(selectors, SelectorInfo{
			Selector:       match,
			EstSeriesCount: bin.SeriesCount,
		})
	}

	// Recursively split oversized values
	for _, ov := range oversized {
		subSelector := appendExactSelector(baseSelector, bestLabel, ov.Name)
		subSelectors, err := s.recursiveSplit(ctx, metric, subSelector, maxSeries, start, end, append(usedLabels, bestLabel))
		if err != nil {
			return nil, fmt.Errorf("recursive split for %s=%s: %w", bestLabel, ov.Name, err)
		}
		selectors = append(selectors, subSelectors...)
	}

	// Handle series that don't have the split label at all (remainder)
	remainderSelector := appendEmptyLabelSelector(baseSelector, bestLabel)
	remainderMatch := buildMatch(metric, remainderSelector)
	remainderCount, err := s.client.GetSeriesCount(ctx, remainderMatch, start, end)
	if err != nil {
		s.logger.Warn("Failed to check remainder series", zap.Error(err))
	} else if remainderCount > 0 {
		s.logger.Info("Remainder series without split label",
			zap.String("metric", metric),
			zap.String("label", bestLabel),
			zap.Int("count", remainderCount),
		)
		if remainderCount > maxSeries {
			remSelectors, err := s.recursiveSplit(ctx, metric, remainderSelector, maxSeries, start, end, append(usedLabels, bestLabel))
			if err != nil {
				return nil, err
			}
			selectors = append(selectors, remSelectors...)
		} else {
			selectors = append(selectors, SelectorInfo{
				Selector:       remainderMatch,
				EstSeriesCount: remainderCount,
			})
		}
	}

	return selectors, nil
}

// filterLabels returns candidate labels for splitting, removing excluded
// and already-used labels. It orders them by preference if configured.
func (s *Splitter) filterLabels(labels []string, usedLabels []string) []string {
	used := make(map[string]bool)
	for _, l := range usedLabels {
		used[l] = true
	}

	var candidates []string
	for _, l := range labels {
		if s.excludeLabels[l] || used[l] {
			continue
		}
		candidates = append(candidates, l)
	}

	// Sort: preferred labels first, then alphabetical
	if len(s.preferLabels) > 0 {
		preferIdx := make(map[string]int)
		for i, l := range s.preferLabels {
			preferIdx[l] = i
		}

		sort.SliceStable(candidates, func(i, j int) bool {
			pi, okI := preferIdx[candidates[i]]
			pj, okJ := preferIdx[candidates[j]]
			if okI && okJ {
				return pi < pj
			}
			if okI {
				return true
			}
			if okJ {
				return false
			}
			return candidates[i] < candidates[j]
		})
	}

	return candidates
}

// findBestSplitLabel evaluates candidate labels and returns the one that
// produces the best partition (most granularity, most even distribution).
func (s *Splitter) findBestSplitLabel(ctx context.Context, metric string, baseSelector string, candidateLabels []string, start, end time.Time) (string, []types.LabelValueCount, error) {
	match := buildMatch(metric, baseSelector)

	type labelScore struct {
		label        string
		distribution []types.LabelValueCount
		score        float64
	}

	var scores []labelScore

	for _, label := range candidateLabels {
		dist, err := s.client.GetSeriesDistribution(ctx, match, label, start, end)
		if err != nil {
			s.logger.Warn("Failed to get distribution for label",
				zap.String("label", label),
				zap.Error(err),
			)
			continue
		}

		if len(dist) == 0 {
			continue
		}

		score := scoreLabelDistribution(dist)
		scores = append(scores, labelScore{
			label:        label,
			distribution: dist,
			score:        score,
		})

		s.logger.Debug("Label score",
			zap.String("metric", metric),
			zap.String("label", label),
			zap.Int("values", len(dist)),
			zap.Float64("score", score),
		)
	}

	if len(scores) == 0 {
		return "", nil, fmt.Errorf("no suitable labels found for splitting metric %s", metric)
	}

	// Pick the label with the highest score
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	best := scores[0]
	return best.label, best.distribution, nil
}

// scoreLabelDistribution calculates a score for how well a label's value
// distribution would work for splitting. Higher is better.
//
// Factors:
// - Number of distinct values (more = better granularity)
// - Evenness of distribution (more even = better balance)
// - Absence of dominant outliers (no single value too large)
func scoreLabelDistribution(dist []types.LabelValueCount) float64 {
	if len(dist) == 0 {
		return 0
	}

	// Number of values contributes to granularity
	numValues := float64(len(dist))

	// Calculate total and max
	total := 0
	maxVal := 0
	for _, d := range dist {
		total += d.Value
		if d.Value > maxVal {
			maxVal = d.Value
		}
	}

	if total == 0 {
		return 0
	}

	// Evenness: 1.0 = perfectly even, 0.0 = all in one value
	avgVal := float64(total) / numValues
	evenness := avgVal / float64(maxVal)

	// Granularity bonus: more values = better (log scale)
	granularity := numValues

	// Combined score: favor granularity with a bonus for evenness
	return granularity * (0.5 + 0.5*evenness)
}

// buildMatch constructs a complete match[] selector.
func buildMatch(metric string, selector string) string {
	sel := strings.TrimSpace(selector)
	if sel == "" {
		return fmt.Sprintf(`{__name__="%s"}`, metric)
	}
	sel = strings.TrimPrefix(sel, "{")
	sel = strings.TrimSuffix(sel, "}")
	sel = strings.TrimSpace(sel)

	// Check if __name__ is already in the selector
	if strings.Contains(sel, "__name__") {
		return "{" + sel + "}"
	}
	if sel != "" {
		return fmt.Sprintf(`{__name__="%s",%s}`, metric, sel)
	}
	return fmt.Sprintf(`{__name__="%s"}`, metric)
}

// appendRegexSelector adds a label=~"val1|val2|..." matcher to a selector.
func appendRegexSelector(baseSelector string, label string, values []string) string {
	// Escape regex special characters in values
	escaped := make([]string, len(values))
	for i, v := range values {
		escaped[i] = escapeRegex(v)
	}
	regex := strings.Join(escaped, "|")
	matcher := fmt.Sprintf(`%s=~"%s"`, label, regex)
	return joinSelector(baseSelector, matcher)
}

// appendExactSelector adds a label="value" matcher to a selector.
func appendExactSelector(baseSelector string, label string, value string) string {
	matcher := fmt.Sprintf(`%s="%s"`, label, value)
	return joinSelector(baseSelector, matcher)
}

// appendEmptyLabelSelector adds a label="" matcher (matches series without the label).
func appendEmptyLabelSelector(baseSelector string, label string) string {
	matcher := fmt.Sprintf(`%s=""`, label)
	return joinSelector(baseSelector, matcher)
}

// joinSelector appends a matcher to a base selector string.
func joinSelector(baseSelector string, matcher string) string {
	sel := strings.TrimSpace(baseSelector)
	sel = strings.TrimPrefix(sel, "{")
	sel = strings.TrimSuffix(sel, "}")
	sel = strings.TrimSpace(sel)

	if sel == "" {
		return matcher
	}
	return sel + "," + matcher
}

// escapeRegex escapes special regex characters in a string for use in PromQL =~ matchers.
func escapeRegex(s string) string {
	special := `\.+*?^${}()[]|`
	var b strings.Builder
	for _, c := range s {
		if strings.ContainsRune(special, c) {
			b.WriteRune('\\')
		}
		b.WriteRune(c)
	}
	return b.String()
}
