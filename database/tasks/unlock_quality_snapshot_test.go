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
		}},
		PathBindings: []unlockQualityPathBinding{{PingTaskID: 7, ExitNodeUUID: "node-b", Family: 4}},
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

func TestBuildUnlockQualityPathBindings(t *testing.T) {
	clients := []models.Client{
		{UUID: "node-a", IPv4: "192.0.2.10", IPv6: "2001:db8::10"},
		{UUID: "node-b", IPv4: "192.0.2.20"},
	}
	pingTasks := []models.PingTask{
		{Id: 1, Type: "icmp", Target: "192.0.2.10"},
		{Id: 2, Type: "icmp", Target: "2001:db8::10"},
		{Id: 3, Type: "tcp", Target: "192.0.2.20"},
		{Id: 4, Type: "icmp", Target: "example.com"},
	}
	bindings := buildUnlockQualityPathBindings(clients, pingTasks)
	if len(bindings) != 2 {
		t.Fatalf("binding count = %d, want 2", len(bindings))
	}
	if bindings[0].ExitNodeUUID != "node-a" || bindings[0].Family != 4 || bindings[0].PingTaskID != 1 {
		t.Fatalf("unexpected IPv4 binding: %+v", bindings[0])
	}
	if bindings[1].ExitNodeUUID != "node-a" || bindings[1].Family != 6 || bindings[1].PingTaskID != 2 {
		t.Fatalf("unexpected IPv6 binding: %+v", bindings[1])
	}
}

func TestFilterUnlockQualityPathBindings(t *testing.T) {
	bindings := []unlockQualityPathBinding{
		{PingTaskID: 1, ExitNodeUUID: "node-a", Family: 4},
		{PingTaskID: 2, ExitNodeUUID: "node-b", Family: 4},
	}
	filtered := filterUnlockQualityPathBindings(bindings, []models.Client{{UUID: "node-b"}})
	if len(filtered) != 1 || filtered[0].ExitNodeUUID != "node-b" {
		t.Fatalf("unexpected filtered bindings: %+v", filtered)
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
