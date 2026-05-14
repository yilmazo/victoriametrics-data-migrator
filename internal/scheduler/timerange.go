// Package scheduler handles time range splitting and task queue management.
package scheduler

import (
	"time"

	"github.com/yilmazo/victoriametrics-data-migrator/internal/types"
)

// SplitTimeRange divides the [start, end) interval into sub-intervals
// based on the step type. If reverse is true, the returned ranges are
// ordered from newest to oldest.
func SplitTimeRange(start, end time.Time, step string, reverse bool) []types.TimeRange {
	var ranges []types.TimeRange

	current := start
	for current.Before(end) {
		next := advanceByStep(current, step)
		if next.After(end) {
			next = end
		}
		ranges = append(ranges, types.TimeRange{
			Start: current,
			End:   next,
		})
		current = next
	}

	if reverse {
		reverseRanges(ranges)
	}

	return ranges
}

// advanceByStep advances a time by one step unit.
func advanceByStep(t time.Time, step string) time.Time {
	switch step {
	case "hour":
		return t.Add(time.Hour)
	case "day":
		return t.AddDate(0, 0, 1)
	case "month":
		return t.AddDate(0, 1, 0)
	default:
		return t.AddDate(0, 0, 1) // default to day
	}
}

// reverseRanges reverses a slice of TimeRange in place.
func reverseRanges(ranges []types.TimeRange) {
	for i, j := 0, len(ranges)-1; i < j; i, j = i+1, j-1 {
		ranges[i], ranges[j] = ranges[j], ranges[i]
	}
}
