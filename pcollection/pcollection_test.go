package pcollection_test

import (
	"sort"
	"testing"

	"mini-flumejava/pcollection"
	"mini-flumejava/tester"
)

func TestParallelDo(t *testing.T) {
	tt := tester.NewTester()

	nums := tester.MockPCollection(tt, []int{1, 2, 3})
	doubled := pcollection.ParallelDo(nums, func(n int) int { return n * 2 })

	tt.Pipeline.Run()

	got := doubled.Elements()
	want := []int{2, 4, 6}

	if len(got) != len(want) {
		t.Fatalf("expected %d elements, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: expected %d, got %d", i, want[i], got[i])
		}
	}
}

func TestFlatten(t *testing.T) {
	tt := tester.NewTester()

	a := tester.MockPCollection(tt, []int{1, 2})
	b := tester.MockPCollection(tt, []int{3, 4})
	merged := pcollection.Flatten(a, b)

	tt.Pipeline.Run()

	got := append([]int{}, merged.Elements()...)
	sort.Ints(got) // Flatten doesn't guarantee source order between branches
	want := []int{1, 2, 3, 4}

	if len(got) != len(want) {
		t.Fatalf("expected %d elements, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: expected %d, got %d", i, want[i], got[i])
		}
	}
}

func TestFlattenRejectsDifferentPipelines(t *testing.T) {
	tt1 := tester.NewTester()
	tt2 := tester.NewTester()

	a := tester.MockPCollection(tt1, []int{1})
	b := tester.MockPCollection(tt2, []int{2})

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when flattening sources from different pipelines")
		}
	}()
	pcollection.Flatten(a, b)
}

func TestGroupByKey(t *testing.T) {
	tt := tester.NewTester()

	pairs := tester.MockPTable(tt, []pcollection.KV[string, int]{
		{Key: "apple", Value: 1},
		{Key: "apple", Value: 1},
		{Key: "banana", Value: 1},
	})
	grouped := pcollection.GroupByKey(pairs)

	tt.Pipeline.Run()

	got := map[string][]int{}
	for _, kv := range grouped.Items() {
		got[kv.Key] = kv.Value
	}

	if len(got["apple"]) != 2 {
		t.Errorf("expected 2 values for apple, got %v", got["apple"])
	}
	if len(got["banana"]) != 1 {
		t.Errorf("expected 1 value for banana, got %v", got["banana"])
	}
}

func TestCombineValues(t *testing.T) {
	tt := tester.NewTester()

	pairs := tester.MockPTable(tt, []pcollection.KV[string, int]{
		{Key: "apple", Value: 1},
		{Key: "apple", Value: 1},
		{Key: "banana", Value: 1},
	})
	grouped := pcollection.GroupByKey(pairs)
	summed := pcollection.CombineValues(grouped, func(vs []int) int {
		total := 0
		for _, v := range vs {
			total += v
		}
		return total
	})

	tt.Pipeline.Run()

	got := map[string]int{}
	for _, kv := range summed.Items() {
		got[kv.Key] = kv.Value
	}

	if got["apple"] != 2 {
		t.Errorf("expected apple=2, got %d", got["apple"])
	}
	if got["banana"] != 1 {
		t.Errorf("expected banana=1, got %d", got["banana"])
	}
}
