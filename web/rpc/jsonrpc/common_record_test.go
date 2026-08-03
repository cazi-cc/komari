package jsonrpc

import (
	"reflect"
	"testing"
)

func TestSampleEvenly(t *testing.T) {
	input := []int{0, 1, 2, 3, 4}
	tests := []struct {
		name  string
		count int
		want  []int
	}{
		{name: "empty", count: 0, want: []int{}},
		{name: "latest only", count: 1, want: []int{4}},
		{name: "even selection", count: 3, want: []int{0, 2, 4}},
		{name: "all", count: len(input), want: input},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := sampleEvenly(input, test.count); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("sampleEvenly(%v, %d) = %v, want %v", input, test.count, got, test.want)
			}
		})
	}
}

func TestAllocateTargetsSupportsTypedKeys(t *testing.T) {
	groups := []allocationGroup[uint]{
		{key: 7, length: 6},
		{key: 9, length: 4},
	}
	got := allocateTargets(groups, 5)
	if got[7] != 3 || got[9] != 2 {
		t.Fatalf("allocateTargets() = %v, want map[7:3 9:2]", got)
	}
}

func TestAllocateTargetsWithMinimumPreservesEachSeries(t *testing.T) {
	groups := []allocationGroup[string]{
		{key: "fast", length: 1000},
		{key: "normal", length: 100},
		{key: "slow", length: 12},
	}

	got := allocateTargetsWithMinimum(groups, 60, 10)
	total := 0
	for _, group := range groups {
		if got[group.key] < 10 {
			t.Fatalf("series %q target = %d, want at least 10; all=%v", group.key, got[group.key], got)
		}
		if got[group.key] > group.length {
			t.Fatalf("series %q target = %d, exceeds length %d", group.key, got[group.key], group.length)
		}
		total += got[group.key]
	}
	if total != 60 {
		t.Fatalf("allocated total = %d, want 60; all=%v", total, got)
	}
}

func TestAllocateTargetsWithMinimumFallsBackWhenLimitIsTight(t *testing.T) {
	groups := []allocationGroup[string]{
		{key: "a", length: 20},
		{key: "b", length: 20},
	}

	got := allocateTargetsWithMinimum(groups, 10, 10)
	if got["a"]+got["b"] != 10 {
		t.Fatalf("allocated total = %d, want 10; all=%v", got["a"]+got["b"], got)
	}
}
