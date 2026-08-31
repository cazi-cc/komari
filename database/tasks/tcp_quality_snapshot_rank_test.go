package tasks

import (
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	v2 "github.com/komari-monitor/komari/protocol/v2"
	"github.com/komari-monitor/komari/utils"
)

func TestTCPQualityTrendUsesRobustMaximum(t *testing.T) {
	finishedAt := time.Now().UTC().Truncate(time.Minute)
	observations := make([]tcpQualityObservation, 0, 20)
	for index := 0; index < 20; index++ {
		maximum := 30.0
		if index == 19 {
			maximum = 1000
		}
		observations = append(observations, tcpQualityObservation{
			Client: "node-a", FinishedAt: finishedAt,
			Result: v2.TCPQualityTargetResult{
				Mode: "standard", SamplesSent: 1, SamplesReceived: 1,
				MinLatencyMS: 10, MaxLatencyMS: maximum, AverageLatencyMS: 20,
				P50LatencyMS: 20, P95LatencyMS: 28,
			},
		})
	}
	trend, _ := buildTCPQualityTrends(models.TCPQualityTask{Interval: 60}, 1, "node-a", observations)
	if len(trend) != 1 {
		t.Fatalf("trend points = %d, want 1", len(trend))
	}
	if trend[0].Max != 30 {
		t.Fatalf("robust maximum = %.1f, want 30.0", trend[0].Max)
	}
	if trend[0].SamplesSent != 20 || trend[0].SamplesReceived != 20 {
		t.Fatalf("sample counters = %d/%d, want 20/20", trend[0].SamplesReceived, trend[0].SamplesSent)
	}
}

func TestTCPQualityTrendSeparatesStandardAndLargeModes(t *testing.T) {
	finishedAt := time.Now().UTC().Truncate(time.Minute)
	observations := []tcpQualityObservation{
		{
			Client: "node-a", FinishedAt: finishedAt,
			Result: v2.TCPQualityTargetResult{
				Mode: "standard", SamplesSent: 10, SamplesReceived: 10,
				P50LatencyMS: 20, P95LatencyMS: 30,
			},
		},
		{
			Client: "node-a", FinishedAt: finishedAt,
			Result: v2.TCPQualityTargetResult{
				Mode: "large", SamplesSent: 10, SamplesReceived: 8,
				P50LatencyMS: 45, P95LatencyMS: 60,
			},
		},
	}

	standardTrend, largeTrend := buildTCPQualityTrends(models.TCPQualityTask{Interval: 60}, 1, "node-a", observations)
	if len(standardTrend) != 1 || standardTrend[0].SamplesReceived != 10 {
		t.Fatalf("standard trend was not preserved: %#v", standardTrend)
	}
	if len(largeTrend) != 1 {
		t.Fatalf("large trend points = %d, want 1", len(largeTrend))
	}
	if largeTrend[0].SamplesSent != 10 || largeTrend[0].SamplesReceived != 8 {
		t.Fatalf("large sample counters = %d/%d, want 8/10", largeTrend[0].SamplesReceived, largeTrend[0].SamplesSent)
	}
	if largeTrend[0].P50 != 45 || largeTrend[0].P95 != 60 {
		t.Fatalf("large latency = %.1f/%.1f, want 45/60", largeTrend[0].P50, largeTrend[0].P95)
	}
}

func TestRankTCPQualityNodesUsesIndependentRanks(t *testing.T) {
	score95 := 95.0
	score90A := 90.0
	score90B := 90.0
	tcp99 := 99.0
	nodes := []tcpQualitySnapshotNode{
		{Name: "unranked", Rankable: false, TCPScore: &tcp99},
		{Name: "third", Rankable: true, OverallScore: &score90B},
		{Name: "first", Rankable: true, OverallScore: &score95},
		{Name: "second", Rankable: true, OverallScore: &score90A},
	}

	rankTCPQualityNodes(nodes)

	wantNames := []string{"first", "second", "third", "unranked"}
	wantRanks := []*int{testRankPointer(1), testRankPointer(2), testRankPointer(2), nil}
	for index := range nodes {
		if nodes[index].Name != wantNames[index] {
			t.Fatalf("node %d name = %q, want %q", index, nodes[index].Name, wantNames[index])
		}
		if wantRanks[index] == nil {
			if nodes[index].Rank != nil {
				t.Fatalf("node %q rank = %d, want nil", nodes[index].Name, *nodes[index].Rank)
			}
			continue
		}
		if nodes[index].Rank == nil || *nodes[index].Rank != *wantRanks[index] {
			t.Fatalf("node %q rank = %v, want %d", nodes[index].Name, nodes[index].Rank, *wantRanks[index])
		}
	}
	if nodes[0].Rank == nodes[1].Rank || nodes[1].Rank == nodes[2].Rank {
		t.Fatal("rank pointers must be independent for every node")
	}
}

func TestStandardSYNScoreUsesAbsoluteExperienceThresholds(t *testing.T) {
	config := defaultTCPQualityScoreConfig()
	clients := []string{"fast", "still-good"}
	labels := []utils.TCPQualityTargetLabel{{Key: "target-a"}}
	data := map[string]map[string]map[string]*tcpQualityModeStats{
		"fast": {
			"target-a": {
				"standard": {LossPercent: 0, P50: 25, P95: 35, CoveragePercent: 100, Rankable: true},
			},
		},
		"still-good": {
			"target-a": {
				"standard": {LossPercent: 0, P50: 70, P95: 100, CoveragePercent: 100, Rankable: true},
			},
		},
	}

	scoreTCPQualityTargets(models.TCPQualityTask{}, clients, labels, data, config)
	score := data["still-good"]["target-a"]["standard"].Score
	if score == nil || *score < 95 {
		t.Fatalf("still-good absolute score = %v, want >= 95", score)
	}
	components := data["still-good"]["target-a"]["standard"].ScoreComponents
	if components["p50"] <= 0 || components["p95"] <= 0 {
		t.Fatalf("missing standard score components: %#v", components)
	}
}

func TestLargeSYNScoreMeasuresIncrementalImpact(t *testing.T) {
	config := defaultTCPQualityScoreConfig()
	labels := []utils.TCPQualityTargetLabel{{Key: "target-a"}}
	data := map[string]map[string]map[string]*tcpQualityModeStats{
		"node-a": {
			"target-a": {
				"standard": {LossPercent: 0, P50: 60, P95: 100, CoveragePercent: 100, Rankable: true},
				"large":    {LossPercent: 5, P50: 80, P95: 150, CoveragePercent: 100, Rankable: true},
			},
		},
	}

	scoreTCPQualityTargets(models.TCPQualityTask{LargeEnabled: true}, []string{"node-a"}, labels, data, config)
	large := data["node-a"]["target-a"]["large"]
	if large.Score == nil || *large.Score != 35.75 {
		t.Fatalf("large incremental score = %v, want 35.75", large.Score)
	}
	if large.ScoreInputs["extra_loss_percent"] != 5 || large.ScoreInputs["p95_degradation_ratio"] != 1.5 {
		t.Fatalf("large score inputs = %#v, want extra loss 5 and ratio 1.5", large.ScoreInputs)
	}
}

func TestTCPQualityDiagnosticsExplainLossGuardAndLargeImpact(t *testing.T) {
	capValue := 69.9
	node := tcpQualitySnapshotNode{
		LossGuardCap: &capValue,
		Standard: tcpQualityModeStats{
			LossPercent: 5.2, P95: 220, Rankable: true,
			ScoreComponents: map[string]float64{"target_mean": 90, "target_p20": 60},
		},
		Large: &tcpQualityModeStats{LossPercent: 8.5, P95: 440, Rankable: true},
	}

	diagnostics := buildTCPQualityDiagnostics(node)
	if len(diagnostics) != 3 {
		t.Fatalf("diagnostic count = %d, want 3: %#v", len(diagnostics), diagnostics)
	}
	if diagnostics[0] != "标准 SYN 首次响应丢失 5.20%，综合分最高 69.9" {
		t.Fatalf("guard diagnostic = %q", diagnostics[0])
	}
}

func testRankPointer(value int) *int {
	return &value
}
