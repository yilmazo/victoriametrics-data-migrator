package splitter

import (
	"sort"

	"github.com/yilmazo/victoriametrics-data-migrator/internal/types"
)

// Bin represents a group of label values that can be handled together
// in a single vmctl task without exceeding the series limit.
type Bin struct {
	LabelValues []string
	SeriesCount int
}

// BinPack distributes label values into bins such that each bin's total
// series count does not exceed maxSeries, and each bin contains at most
// maxValues label values.
//
// Values that individually exceed maxSeries are returned in the oversized slice.
// Uses a greedy first-fit decreasing algorithm.
func BinPack(values []types.LabelValueCount, maxSeries int, maxValues int) (bins []Bin, oversized []types.LabelValueCount) {
	if len(values) == 0 {
		return nil, nil
	}

	// Sort by series count descending for better packing
	sorted := make([]types.LabelValueCount, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Value > sorted[j].Value
	})

	for _, v := range sorted {
		// Skip values with 0 series
		if v.Value <= 0 {
			continue
		}

		// If a single value exceeds the limit, mark as oversized
		if v.Value > maxSeries {
			oversized = append(oversized, v)
			continue
		}

		// Try to fit into an existing bin
		placed := false
		for i := range bins {
			if bins[i].SeriesCount+v.Value <= maxSeries && len(bins[i].LabelValues) < maxValues {
				bins[i].LabelValues = append(bins[i].LabelValues, v.Name)
				bins[i].SeriesCount += v.Value
				placed = true
				break
			}
		}

		// Create new bin if no existing bin can fit this value
		if !placed {
			bins = append(bins, Bin{
				LabelValues: []string{v.Name},
				SeriesCount: v.Value,
			})
		}
	}

	return bins, oversized
}
