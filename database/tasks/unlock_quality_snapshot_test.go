package tasks

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	v2 "github.com/komari-monitor/komari/protocol/v2"
	"gorm.io/gorm/schema"
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

func TestNormalizeUnlockQualityProtectedResponses(t *testing.T) {
	results := []v2.UnlockQualityEndpointResult{
		{EndpointKey: "web", Verdict: "partial", SamplesReceived: 1, HTTPStatusCode: 403},
		{EndpointKey: "auth", Verdict: "partial", SamplesReceived: 1, HTTPStatusCode: 403},
		{EndpointKey: "api", Verdict: "available", SamplesReceived: 1, HTTPStatusCode: 401},
		{EndpointKey: "static", Verdict: "partial", SamplesReceived: 1, HTTPStatusCode: 404},
		{EndpointKey: "trace", Verdict: "available", SamplesReceived: 1, HTTPStatusCode: 200},
	}
	if got := normalizeUnlockQualityVerdict(results, "verify", "partial"); got != "available" {
		t.Fatalf("normalized verdict = %q, want available", got)
	}
	results[2].Verdict = "region_limited"
	results[2].HTTPStatusCode = 403
	if got := normalizeUnlockQualityVerdict(results, "verify", "partial"); got != "region_limited" {
		t.Fatalf("region-limited verdict = %q, want region_limited", got)
	}
}

func TestNormalizeStoredUnlockQualityRun(t *testing.T) {
	results := []v2.UnlockQualityEndpointResult{
		{EndpointKey: "web", Verdict: "partial", SamplesReceived: 1, HTTPStatusCode: 403},
		{EndpointKey: "auth", Verdict: "partial", SamplesReceived: 1, HTTPStatusCode: 403},
		{EndpointKey: "api", Verdict: "available", SamplesReceived: 1, HTTPStatusCode: 401},
		{EndpointKey: "static", Verdict: "partial", SamplesReceived: 1, HTTPStatusCode: 404},
		{EndpointKey: "trace", Verdict: "available", SamplesReceived: 1, HTTPStatusCode: 200},
	}
	payload, err := encodeUnlockQualityResults(results)
	if err != nil {
		t.Fatal(err)
	}
	run := models.UnlockQualityRun{Verdict: "partial", ProbeKind: "verify", Payload: payload}
	if got := normalizedUnlockQualityRunVerdict(run); got != "available" {
		t.Fatalf("stored verdict = %q, want available", got)
	}
}

func TestUnlockQualityPublicSnapshotHasNoEndpointFields(t *testing.T) {
	payload, err := json.Marshal(unlockQualitySnapshot{
		TaskName: "ChatGPT", Service: "chatgpt",
		Nodes: []unlockQualitySnapshotNode{{
			Name: "node", System: unlockQualityRouteSummary{RouteMode: "system"},
			Relay: &unlockQualityRouteSummary{RouteMode: "relay"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(payload))
	for _, forbidden := range []string{`"ip"`, `"address"`, `"domain"`, `"url"`, `"proxy"`, `"dns_server"`, `"fixed_address"`, `"hostname"`, `"port"`} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("public snapshot contains forbidden field %s: %s", forbidden, payload)
		}
	}
}

func TestNormalizeUnlockQualityRelayTask(t *testing.T) {
	task := models.UnlockQualityTask{
		Name: "ChatGPT", Service: "chatgpt", Interval: 60, VerifyInterval: 900,
		SampleCount: 1, TimeoutMS: 10000, Clients: models.StringArray{"node-a"},
		RelayEnabled: true, RelayClients: models.StringArray{"node-a"},
		RelayProxyURL: "socks5://user:secret@127.0.0.1:1080",
	}
	if err := NormalizeUnlockQualityTask(&task); err != nil {
		t.Fatalf("valid relay task rejected: %v", err)
	}
	task.RelayClients = models.StringArray{"node-b"}
	if err := NormalizeUnlockQualityTask(&task); err == nil {
		t.Fatal("relay client outside task assignment must be rejected")
	}
	task.RelayClients = models.StringArray{"node-a"}
	task.RelayProxyURL = "ftp://127.0.0.1:21"
	if err := NormalizeUnlockQualityTask(&task); err == nil {
		t.Fatal("unsupported relay scheme must be rejected")
	}
}

func TestUnlockQualitySnapshotRefreshIntervals(t *testing.T) {
	tests := map[int]time.Duration{
		1:   time.Minute,
		6:   5 * time.Minute,
		24:  5 * time.Minute,
		72:  15 * time.Minute,
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

func TestUnlockQualityRunTTFBColumnNames(t *testing.T) {
	parsed, err := schema.Parse(&models.UnlockQualityRun{}, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		t.Fatal(err)
	}
	for fieldName, want := range map[string]string{
		"TTFBP50MS": unlockQualityTTFBP50Column,
		"TTFBP95MS": unlockQualityTTFBP95Column,
	} {
		field := parsed.LookUpField(fieldName)
		if field == nil {
			t.Fatalf("field %s not found", fieldName)
		}
		if field.DBName != want {
			t.Fatalf("%s database name = %q, want %q", fieldName, field.DBName, want)
		}
	}
}
