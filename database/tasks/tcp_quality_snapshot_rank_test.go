package tasks

import "testing"

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
