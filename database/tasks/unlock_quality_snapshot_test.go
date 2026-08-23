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
		Status: "available", FailurePercent: 0, TTFBP50MS: 80, TTFBP95MS: 120,
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

func TestUnlockQualityScoreRewardsVisibleLatency(t *testing.T) {
	fast := unlockQualityRouteSummary{
		Status: "available", FailurePercent: 0, TTFBP50MS: 16, TTFBP95MS: 25,
		ConnectMS: 2, TLSMS: 6, JitterMS: 4,
	}
	slow := unlockQualityRouteSummary{
		Status: "available", FailurePercent: 0, TTFBP50MS: 171, TTFBP95MS: 209,
		ConnectMS: 2, TLSMS: 84, JitterMS: 30,
	}
	fastScore, _ := scoreUnlockQualityRoute(fast)
	slowScore, _ := scoreUnlockQualityRoute(slow)
	if fastScore <= slowScore {
		t.Fatalf("fast route scored %.1f, want greater than slow route %.1f", fastScore, slowScore)
	}
	if slowScore >= 90 {
		t.Fatalf("171/209 ms route scored %.1f, want below excellent grade", slowScore)
	}
}

func TestUnlockQualityScoreDoesNotDoubleCountTransport(t *testing.T) {
	base := unlockQualityRouteSummary{
		Status: "available", FailurePercent: 0, TTFBP50MS: 20, TTFBP95MS: 35,
		ConnectMS: 2, TLSMS: 8,
	}
	baseScore, _ := scoreUnlockQualityRoute(base)
	base.ConnectMS = 80
	base.TLSMS = 180
	changedScore, _ := scoreUnlockQualityRoute(base)
	if baseScore != changedScore {
		t.Fatalf("transport phases changed score from %.1f to %.1f although TTFB already includes them", baseScore, changedScore)
	}
}

func TestUnlockQualityTailFactorRewardsZeroSpread(t *testing.T) {
	if factor := unlockQualityTailFactor(0); factor != 1 {
		t.Fatalf("zero TTFB tail spread factor = %.1f, want 1", factor)
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
		{UUID: "node-a", IPv4: "192.0.2.10", IPv6: "2001:db8::10", ReachableAddresses: models.StringArray{"198.51.100.10"}},
		{UUID: "node-b", IPv4: "192.0.2.20"},
	}
	pingTasks := []models.PingTask{
		{Id: 1, Type: "icmp", Target: "192.0.2.10"},
		{Id: 2, Type: "icmp", Target: "2001:db8::10"},
		{Id: 3, Type: "tcp", Target: "192.0.2.20"},
		{Id: 4, Type: "icmp", Target: "example.com"},
		{Id: 5, Type: "icmp", Target: "198.51.100.10"},
	}
	bindings := buildUnlockQualityPathBindings(clients, pingTasks)
	if len(bindings) != 3 {
		t.Fatalf("binding count = %d, want 3", len(bindings))
	}
	if bindings[0].ExitNodeUUID != "node-a" || bindings[0].Family != 4 || bindings[0].PingTaskID != 1 {
		t.Fatalf("unexpected IPv4 binding: %+v", bindings[0])
	}
	if bindings[2].ExitNodeUUID != "node-a" || bindings[2].Family != 6 || bindings[2].PingTaskID != 2 {
		t.Fatalf("unexpected IPv6 binding: %+v", bindings[2])
	}
	if bindings[1].ExitNodeUUID != "node-a" || bindings[1].Family != 4 || bindings[1].PingTaskID != 5 {
		t.Fatalf("unexpected reachable-address binding: %+v", bindings[1])
	}
}

func TestBuildUnlockQualityPathBindingsSkipsAmbiguousAddress(t *testing.T) {
	clients := []models.Client{
		{UUID: "node-a", ReachableAddresses: models.StringArray{"198.51.100.20"}},
		{UUID: "node-b", IPv4: "198.51.100.20"},
	}
	bindings := buildUnlockQualityPathBindings(clients, []models.PingTask{
		{Id: 1, Type: "icmp", Target: "198.51.100.20"},
	})
	if len(bindings) != 0 {
		t.Fatalf("ambiguous address produced bindings: %+v", bindings)
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
