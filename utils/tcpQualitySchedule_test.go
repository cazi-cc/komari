package utils

import (
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
)

func TestTCPQualityExperimentalDueUsesStableHourlyPhase(t *testing.T) {
	task := models.TCPQualityTask{Interval: 900, ExperimentalInterval: 3600, SchedulePhaseMS: 120000}
	base := time.Unix(0, 0).UTC().Add(2 * time.Minute)
	if !tcpQualityExperimentalDue(task, base.Add(4*time.Hour)) {
		t.Fatal("hourly experimental probe was not due on its stable phase")
	}
	for _, offset := range []time.Duration{15 * time.Minute, 30 * time.Minute, 45 * time.Minute} {
		if tcpQualityExperimentalDue(task, base.Add(4*time.Hour).Add(offset)) {
			t.Fatalf("experimental probe was unexpectedly due at offset %s", offset)
		}
	}
}

func TestTCPQualityExperimentalDueFollowsEditedIntervals(t *testing.T) {
	now := time.Unix(8*3600, 0).UTC()
	task := models.TCPQualityTask{Interval: 1800, ExperimentalInterval: 3600}
	if !tcpQualityExperimentalDue(task, now) {
		t.Fatal("experimental probe should run every second task interval")
	}
	task.ExperimentalInterval = 5400
	if tcpQualityExperimentalDue(task, now) {
		t.Fatal("edited three-slot interval should produce a new phase plan")
	}
}
