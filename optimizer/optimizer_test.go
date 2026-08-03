package optimizer_test

import (
	"fmt"
	o "mini-flumejava/optimizer"
	pc "mini-flumejava/pcollection"
	"mini-flumejava/tester"
	"testing"
)

func Init() *tester.Tester {
	t := tester.NewTester()
	pcItems := []int{1, 2, 3, 4, 5}
	pd := func(v int) int {
		return v + 2
	}
	pdToPairs := func(v int) pc.KV[int, int] {
		return pc.KV[int, int]{Key: v, Value: v + 3}
	}

	pc1 := tester.MockPCollection(t, pcItems)
	pc2 := tester.MockPCollection(t, pcItems)

	pc3 := tester.MockPCollection(t, pcItems)
	pc4 := tester.MockPCollection(t, pcItems)

	f1 := pc.Flatten(pc1, pc2)
	f2 := pc.Flatten(pc3, pc4)

	pd1 := pc.ParallelDo(f1, pd)
	pd2 := pc.ParallelDo(f2, pd)

	f3 := pc.Flatten(pd1, pd2)

	pd3 := pc.ParallelDo(f3, pdToPairs)

	pc.GroupByKey(pd3)

	t.Pipeline.Run()

	return t
}
func TestSinkFlattens(tt *testing.T) {
	t := Init()
	path := t.Pipeline.Path()
	for i := range path {
		fmt.Printf("%s ", path[i].Kind)
	}
	fmt.Println()

	o.SinkFlattens(t.Pipeline)
	path = t.Pipeline.Path()
	for i := range path {
		fmt.Printf("%s ", path[i].Kind)
	}
	fmt.Println()
}
