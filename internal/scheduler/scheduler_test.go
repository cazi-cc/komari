package scheduler

import (
	"context"
	"testing"
	"time"
)

func TestCronScheduleUsesSystemLocalWallClock(t *testing.T) {
	originalLocal := time.Local
	time.Local = time.FixedZone("UTC+8", 8*60*60)
	t.Cleanup(func() { time.Local = originalLocal })

	schedule, err := Parse("0 0 9 * * *")
	if err != nil {
		t.Fatalf("parse schedule: %v", err)
	}
	after := time.Date(2026, 7, 17, 0, 30, 0, 0, time.UTC)
	want := time.Date(2026, 7, 17, 1, 0, 0, 0, time.UTC)
	if got := schedule.Next(after); !got.Equal(want) {
		t.Fatalf("next run = %s, want %s", got, want)
	} else if got.Location() != time.UTC {
		t.Fatalf("next run location = %s, want UTC", got.Location())
	}
}

func TestEverySchedulePreservesElapsedDuration(t *testing.T) {
	schedule, err := Parse("@every 90s")
	if err != nil {
		t.Fatalf("parse schedule: %v", err)
	}
	after := time.Now()
	if got := schedule.Next(after); got.Sub(after) != 90*time.Second {
		t.Fatalf("interval = %s, want 90s", got.Sub(after))
	}
}

func TestManagerStartDelayOnlyAppliesToFirstRun(t *testing.T) {
	manager := NewManager()
	t.Cleanup(manager.StopAll)
	runs := make(chan time.Time, 2)
	started := time.Now()
	if err := manager.AddContextFuncWithStartDelay("delayed", "@every 40ms", 20*time.Millisecond, func(context.Context) {
		runs <- time.Now()
	}); err != nil {
		t.Fatalf("add delayed job: %v", err)
	}

	var first time.Time
	select {
	case first = <-runs:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for first run")
	}
	if delay := first.Sub(started); delay < 15*time.Millisecond || delay > 200*time.Millisecond {
		t.Fatalf("first delay = %s, want about 20ms", delay)
	}

	select {
	case second := <-runs:
		if interval := second.Sub(first); interval < 30*time.Millisecond || interval > 200*time.Millisecond {
			t.Fatalf("repeat interval = %s, want about 40ms", interval)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for second run")
	}
}
