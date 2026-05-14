package scheduler

import (
	"testing"
	"time"
)

func TestSplitTimeRange_Day(t *testing.T) {
	start := time.Date(2026, 1, 13, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC)

	ranges := SplitTimeRange(start, end, "day", false)

	if len(ranges) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(ranges))
	}

	// Check first range
	if !ranges[0].Start.Equal(time.Date(2026, 1, 13, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("range[0].Start = %v", ranges[0].Start)
	}
	if !ranges[0].End.Equal(time.Date(2026, 1, 14, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("range[0].End = %v", ranges[0].End)
	}

	// Check last range
	if !ranges[2].Start.Equal(time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("range[2].Start = %v", ranges[2].Start)
	}
	if !ranges[2].End.Equal(time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("range[2].End = %v", ranges[2].End)
	}
}

func TestSplitTimeRange_DayReverse(t *testing.T) {
	start := time.Date(2026, 1, 13, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC)

	ranges := SplitTimeRange(start, end, "day", true)

	if len(ranges) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(ranges))
	}

	// First should be newest
	if !ranges[0].Start.Equal(time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("range[0].Start = %v, want 2026-01-15", ranges[0].Start)
	}
	// Last should be oldest
	if !ranges[2].Start.Equal(time.Date(2026, 1, 13, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("range[2].Start = %v, want 2026-01-13", ranges[2].Start)
	}
}

func TestSplitTimeRange_Month(t *testing.T) {
	start := time.Date(2026, 1, 13, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)

	ranges := SplitTimeRange(start, end, "month", false)

	if len(ranges) != 4 {
		t.Fatalf("expected 4 ranges, got %d", len(ranges))
	}

	// First range: Jan 13 - Feb 13
	if !ranges[0].Start.Equal(time.Date(2026, 1, 13, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("range[0].Start = %v", ranges[0].Start)
	}
	if !ranges[0].End.Equal(time.Date(2026, 2, 13, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("range[0].End = %v", ranges[0].End)
	}
}

func TestSplitTimeRange_Hour(t *testing.T) {
	start := time.Date(2026, 1, 13, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 13, 3, 0, 0, 0, time.UTC)

	ranges := SplitTimeRange(start, end, "hour", false)

	if len(ranges) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(ranges))
	}
}

func TestSplitTimeRange_LargeRange(t *testing.T) {
	// 13.01.2026 to 13.05.2026 with day step and reverse
	start := time.Date(2026, 1, 13, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)

	ranges := SplitTimeRange(start, end, "day", true)

	// 120 days between Jan 13 and May 13
	if len(ranges) != 120 {
		t.Fatalf("expected 120 ranges, got %d", len(ranges))
	}

	// First range should be the newest (May 12 - May 13)
	if !ranges[0].Start.Equal(time.Date(2026, 5, 12, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("first range start = %v, want 2026-05-12", ranges[0].Start)
	}
	if !ranges[0].End.Equal(time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("first range end = %v, want 2026-05-13", ranges[0].End)
	}
}
