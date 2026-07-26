package tasks

import (
	"encoding/json"
	"strings"
	"testing"

	v2 "github.com/komari-monitor/komari/protocol/v2"
)

func TestNormalizeTCPQualityScoreConfigRestoresInvalidWeightGroups(t *testing.T) {
	value := defaultTCPQualityScoreConfig()
	value.StandardLossWeight = 0
	value.StandardP50Weight = 0
	value.StandardP95Weight = 0
	value.StandardCoverageWeight = 0
	value.MinimumRuns = 0
	value.MinimumTargetCoveragePercent = 200

	got := normalizeTCPQualityScoreConfig(value)
	if got.StandardLossWeight != 55 || got.StandardP50Weight != 15 ||
		got.StandardP95Weight != 25 || got.StandardCoverageWeight != 5 {
		t.Fatalf("invalid standard weights were not reset: %#v", got)
	}
	if got.MinimumRuns != 1 {
		t.Fatalf("minimum runs = %d, want clamped value 1", got.MinimumRuns)
	}
	if got.MinimumTargetCoveragePercent != 100 {
		t.Fatalf("minimum target coverage = %v, want 100", got.MinimumTargetCoveragePercent)
	}
}

func TestTCPAbsoluteLatencyCapsRejectExtremeLatency(t *testing.T) {
	if got := tcpP50AbsoluteScore(350); got != 30 {
		t.Fatalf("P50 score at 350ms = %v, want 30", got)
	}
	if got := tcpP50AbsoluteScore(350.1); got != 0 {
		t.Fatalf("P50 score above 350ms = %v, want 0", got)
	}
	if got := tcpP95AbsoluteScore(500); got != 30 {
		t.Fatalf("P95 score at 500ms = %v, want 30", got)
	}
	if got := tcpP95AbsoluteScore(500.1); got != 0 {
		t.Fatalf("P95 score above 500ms = %v, want 0", got)
	}
}

func TestTCPQualityLossGuardUsesConfiguredCaps(t *testing.T) {
	config := defaultTCPQualityScoreConfig()
	if got := applyTCPQualityLossGuard(99, 3, config); got != 84.9 {
		t.Fatalf("warning guard = %v, want 84.9", got)
	}
	if got := applyTCPQualityLossGuard(99, 5, config); got != 69.9 {
		t.Fatalf("critical guard = %v, want 69.9", got)
	}
	if got := applyTCPQualityLossGuard(99, 10, config); got != 49.9 {
		t.Fatalf("severe guard = %v, want 49.9", got)
	}
}

func TestTCPQualityPublicSnapshotDoesNotContainEndpointFields(t *testing.T) {
	payload, err := json.Marshal(tcpQualitySnapshot{
		TaskID:  1,
		Privacy: "labels only",
	})
	if err != nil {
		t.Fatal(err)
	}
	text := string(payload)
	for _, forbidden := range []string{`"address"`, `"host"`, `"port"`, `"target"`} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("public snapshot contains endpoint field %s: %s", forbidden, text)
		}
	}
}

func TestTCPQualityResultUsableExcludesToolFailures(t *testing.T) {
	for _, code := range []string{"", "partial_loss", "no_response"} {
		if !tcpQualityResultUsable(v2.TCPQualityTargetResult{ErrorCode: code}) {
			t.Fatalf("measurement result %q was excluded", code)
		}
	}
	for _, code := range []string{
		"parse_error",
		"partial_parse",
		"nping_error",
		"nping_unavailable",
		"timeout",
		"unsupported_platform",
		"task_already_running",
		"unknown_error",
	} {
		if tcpQualityResultUsable(v2.TCPQualityTargetResult{ErrorCode: code}) {
			t.Fatalf("tool failure %q was included in scoring", code)
		}
	}
}
