package optimizer_test

import (
	"fmt"
	o "mini-flumejava/optimizer"
	pc "mini-flumejava/pcollection"
	"mini-flumejava/pipeline"
	"mini-flumejava/tester"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var debug = os.Getenv("DEBUG") != ""

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
func TestSinkFlattens(t *testing.T) {
	tt := Init()
	path := tt.Pipeline.Path()

	if debug {
		fmt.Printf("BEFORE: ")
		for i := range path {
			fmt.Printf("%s ", path[i].Kind)
		}
		fmt.Println()
	}

	newP := o.SinkFlattens(tt.Pipeline)
	newP.Sort()
	path = newP.Path()

	if debug {
		fmt.Printf("AFTER: ")
		for i := range path {
			fmt.Printf("%s ", path[i].Kind)
		}
		fmt.Println()
	}

	out := []pipeline.OpKind{}
	for _, nw := range newP.Path() {
		out = append(out, nw.Kind)
	}
	// 4x Sources, 8x ParallelDos, 1x GroupByKey
	expected := []pipeline.OpKind{pipeline.OpSource, pipeline.OpSource, pipeline.OpSource, pipeline.OpSource,
		pipeline.OpParallelDo, pipeline.OpParallelDo, pipeline.OpParallelDo, pipeline.OpParallelDo, pipeline.OpParallelDo,
		pipeline.OpParallelDo, pipeline.OpParallelDo, pipeline.OpParallelDo, pipeline.OpGroupByKey}

	assert.ElementsMatch(t, expected, out)
}
