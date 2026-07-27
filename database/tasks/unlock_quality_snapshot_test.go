package tasks

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestUnlockQualityScoreGuards(t *testing.T) {
	base := unlockQualityRouteSummary{
		Status: "available", FailurePercent: 0, TTFBP50MS: 120, TTFBP95MS: 180,
		ConnectMS: 20, TLSMS: 40, JitterMS: 5,
	}
	score, _ := scoreUnlockQualityRoute(base)
	if score < 90 {
		t.Fatalf("healthy route scored %.1f, want >= 90", score)
	}
	base.FailurePercent = 5
	score, _ = scoreUnlockQualityRoute(base)
	if score > 69.9 {
		t.Fatalf("5%% failure score %.1f exceeds cap", score)
	}
	base.Status = "region_limited"
	base.FailurePercent = 0
	score, _ = scoreUnlockQualityRoute(base)
	if score > 39.9 {
		t.Fatalf("region-limited score %.1f exceeds cap", score)
	}
}

func TestUnlockQualityPublicSnapshotHasNoEndpointFields(t *testing.T) {
	payload, err := json.Marshal(unlockQualitySnapshot{
		TaskName: "ChatGPT", Service: "chatgpt",
		Nodes: []unlockQualitySnapshotNode{{Name: "node", System: unlockQualityRouteSummary{RouteMode: "system"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(payload))
	for _, forbidden := range []string{`"ip"`, `"address"`, `"domain"`, `"url"`, `"dns_server"`, `"fixed_address"`, `"hostname"`, `"port"`} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("public snapshot contains forbidden field %s: %s", forbidden, payload)
		}
	}
}

func TestUnlockQualitySnapshotRefreshIntervals(t *testing.T) {
	tests := map[int]time.Duration{
		1:  time.Minute,
		6:  5 * time.Minute,
		24: 5 * time.Minute,
		72: 15 * time.Minute,
		168: 15 * time.Minute,
	}
	for hours, expected := range tests {
		if got := unlockQualitySnapshotRefreshInterval(hours); got != expected {
			t.Fatalf("%d hour refresh interval = %s, want %s", hours, got, expected)
		}
	}
}

func TestUnlockQualityAbnormalVerdictsShareOneSequence(t *testing.T) {
	for _, verdict := range []string{"partial", "region_limited", "unavailable"} {
		if !unlockQualityVerdictAbnormal(verdict) {
			t.Fatalf("%q must be abnormal", verdict)
		}
	}
	if unlockQualityVerdictAbnormal("available") || unlockQualityVerdictAbnormal("") {
		t.Fatal("available and empty states must not be abnormal")
	}
}
