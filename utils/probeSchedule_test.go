package utils

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestProbeScheduleDelaysAreStableAndWithinInterval(t *testing.T) {
	first := probeScheduleDelays("ping", 60, []uint{4, 1, 3, 2})
	second := probeScheduleDelays("ping", 60, []uint{2, 3, 1, 4})
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("schedule changed with input order: %v != %v", first, second)
	}
	for id, delay := range first {
		if delay < minimumProbeStartDelay || delay >= time.Minute {
			t.Fatalf("task %d delay %s is outside the interval", id, delay)
		}
	}
}

func TestProbeScheduleDelaysSpreadTasksAcrossInterval(t *testing.T) {
	delays := probeScheduleDelays("tcp-quality", 900, []uint{1, 2, 3, 4})
	ordered := make([]time.Duration, 0, len(delays))
	for _, delay := range delays {
		ordered = append(ordered, delay)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })

	period := 15 * time.Minute
	wantGap := period / time.Duration(len(ordered))
	for index, delay := range ordered {
		next := ordered[(index+1)%len(ordered)]
		gap := next - delay
		if index == len(ordered)-1 {
			gap += period
		}
		if gap < wantGap-time.Millisecond || gap > wantGap+time.Millisecond {
			t.Fatalf("gap %d = %s, want about %s (delays %v)", index, gap, wantGap, ordered)
		}
	}
}
