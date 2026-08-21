package tasks

import (
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	v2 "github.com/komari-monitor/komari/protocol/v2"
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
	trend := buildTCPQualityTrend(models.TCPQualityTask{Interval: 60}, 1, "node-a", observations)
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

func testRankPointer(value int) *int {
	return &value
}
