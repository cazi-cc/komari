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
	if got.StandardLossWeight != 55 || got.StandardP50Weight != 20 ||
		got.StandardP95Weight != 15 || got.StandardCoverageWeight != 10 {
		t.Fatalf("invalid standard weights were not reset: %#v", got)
	}
	if got.MinimumRuns != 1 {
		t.Fatalf("minimum runs = %d, want clamped value 1", got.MinimumRuns)
	}
	if got.MinimumTargetCoveragePercent != 100 {
		t.Fatalf("minimum target coverage = %v, want 100", got.MinimumTargetCoveragePercent)
	}
}

func TestDefaultTCPQualityScoreConfigMatchesCaziBaseline(t *testing.T) {
	config := defaultTCPQualityScoreConfig()

	if config.ModelVersion != 4 {
		t.Fatalf("model version = %d, want 4", config.ModelVersion)
	}
	assertTCPScoreValue(t, "overall ICMP weight", config.OverallICMPWeight, 25)
	assertTCPScoreValue(t, "overall standard weight", config.OverallStandardWeight, 45)
	assertTCPScoreValue(t, "overall large weight", config.OverallLargeWeight, 30)
	assertTCPScoreValue(t, "standard loss weight", config.StandardLossWeight, 55)
	assertTCPScoreValue(t, "standard P50 weight", config.StandardP50Weight, 20)
	assertTCPScoreValue(t, "standard P95 weight", config.StandardP95Weight, 15)
	assertTCPScoreValue(t, "standard coverage weight", config.StandardCoverageWeight, 10)
	assertTCPScoreValue(t, "large loss weight", config.LargeLossWeight, 55)
	assertTCPScoreValue(t, "large extra loss weight", config.LargeExtraLossWeight, 25)
	assertTCPScoreValue(t, "large P95 degradation weight", config.LargeP95DegradationWeight, 20)
	assertTCPScoreValue(t, "large coverage weight", config.LargeCoverageWeight, 0)
	assertTCPScoreValue(t, "excellent threshold", config.ExcellentThreshold, 90)
	assertTCPScoreValue(t, "good threshold", config.GoodThreshold, 80)
	assertTCPScoreValue(t, "fair threshold", config.FairThreshold, 60)
}

func TestNormalizeTCPQualityScoreConfigKeepsAdministratorThresholds(t *testing.T) {
	config := defaultTCPQualityScoreConfig()
	config.ModelVersion = 99
	config.ExcellentThreshold = 91
	config.GoodThreshold = 79
	config.FairThreshold = 58

	normalized := normalizeTCPQualityScoreConfig(config)
	if normalized.ModelVersion != tcpQualityScoreModelVersion {
		t.Fatalf("normalized model version = %d, want %d", normalized.ModelVersion, tcpQualityScoreModelVersion)
	}
	assertTCPScoreValue(t, "excellent threshold", normalized.ExcellentThreshold, 91)
	assertTCPScoreValue(t, "good threshold", normalized.GoodThreshold, 79)
	assertTCPScoreValue(t, "fair threshold", normalized.FairThreshold, 58)
}

func TestParseTCPQualityScoreConfigAcceptsNewerThemeModelVersion(t *testing.T) {
	settings := map[string]json.RawMessage{
		"tcpQualityScoreModelVersion": json.RawMessage(`99`),
		"tcpOverallICMPWeight":       json.RawMessage(`21`),
		"tcpExcellentThreshold":      json.RawMessage(`91`),
		"tcpGoodThreshold":           json.RawMessage(`79`),
		"tcpFairThreshold":           json.RawMessage(`58`),
	}

	config := parseTCPQualityScoreConfig(settings)
	assertTCPScoreValue(t, "overall ICMP weight", config.OverallICMPWeight, 21)
	assertTCPScoreValue(t, "excellent threshold", config.ExcellentThreshold, 91)
	assertTCPScoreValue(t, "good threshold", config.GoodThreshold, 79)
	assertTCPScoreValue(t, "fair threshold", config.FairThreshold, 58)
}

func TestTCPQualityGradeUsesConfiguredThresholds(t *testing.T) {
	config := defaultTCPQualityScoreConfig()
	if grade := tcpQualityGrade(90, config); grade != "优秀" {
		t.Fatalf("grade at excellent threshold = %q, want 优秀", grade)
	}
	if grade := tcpQualityGrade(89.9, config); grade != "良好" {
		t.Fatalf("grade below excellent threshold = %q, want 良好", grade)
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

func assertTCPScoreValue(t *testing.T, name string, got, want float64) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %.1f, want %.1f", name, got, want)
	}
}
