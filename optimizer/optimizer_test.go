package optimizer_test

import (
	"fmt"
	o "mini-flumejava/optimizer"
	"mini-flumejava/pipeline"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var debug = os.Getenv("DEBUG") != ""

// noopNode is a minimal pipeline.Node used to build synthetic chains directly
// at the NodeWrapper level. LiftCombineValues/FuseParallelDos/MscrFusion only
// read and write NodeWrapper metadata (Kind, Dependants, Dependencies,
// FusionBoundary, EstimatedSize, Combinber) — they never call RebuildWith —
// so a fake Node is sufficient and avoids the generic pcollection plumbing
// that SinkFlattens' tests need.
type noopNode struct{}

func (n *noopNode) Materialize() {}

func reg(p *pipeline.Pipeline, kind pipeline.OpKind, deps ...*pipeline.NodeWrapper) *pipeline.NodeWrapper {
	return p.Register(kind, &noopNode{}, deps...)
}

func kinds(nws []*pipeline.NodeWrapper) []pipeline.OpKind {
	out := make([]pipeline.OpKind, 0, len(nws))
	for _, nw := range nws {
		out = append(out, nw.Kind)
	}
	return out
}

// ---------------------------------------------------------------------------
// LiftCombineValues
// ---------------------------------------------------------------------------

func TestLiftCombineValues_Lifts(t *testing.T) {
	p := pipeline.NewPipeline()
	gbk := reg(p, pipeline.OpGroupByKey)
	cv := reg(p, pipeline.OpCombineValues, gbk)

	o.LiftCombineValues(p)

	assert.Equal(t, cv, gbk.Combinber, "GroupByKey with a single CombineValues dependant should record it")
}

func TestLiftCombineValues_SkipsMultipleDependants(t *testing.T) {
	p := pipeline.NewPipeline()
	gbk := reg(p, pipeline.OpGroupByKey)
	reg(p, pipeline.OpCombineValues, gbk)
	reg(p, pipeline.OpParallelDo, gbk)

	o.LiftCombineValues(p)

	assert.Nil(t, gbk.Combinber, "GroupByKey with more than one dependant should not lift")
}

func TestLiftCombineValues_SkipsNonCombineValuesDependant(t *testing.T) {
	p := pipeline.NewPipeline()
	gbk := reg(p, pipeline.OpGroupByKey)
	reg(p, pipeline.OpParallelDo, gbk)

	o.LiftCombineValues(p)

	assert.Nil(t, gbk.Combinber, "GroupByKey whose sole dependant isn't CombineValues should not lift")
}

// ---------------------------------------------------------------------------
// FuseParallelDos
// ---------------------------------------------------------------------------

// g1 -> c -> d -> g2, with c the smaller estimated size, so c should become
// the fusion boundary.
func TestFuseParallelDos_MarksSmallestNode(t *testing.T) {
	p := pipeline.NewPipeline()
	g1 := reg(p, pipeline.OpGroupByKey)
	c := reg(p, pipeline.OpParallelDo, g1)
	d := reg(p, pipeline.OpParallelDo, c)
	g2 := reg(p, pipeline.OpGroupByKey, d)

	c.EstimatedSize = 5
	d.EstimatedSize = 50

	o.FuseParallelDos(p)

	assert.True(t, c.FusionBoundary, "smaller node in the chain should be marked as the boundary")
	assert.False(t, d.FusionBoundary)
	assert.False(t, g1.FusionBoundary)
	assert.False(t, g2.FusionBoundary)
}

// A chain that never reaches a second GroupByKey (dead end) should not have
// any boundary marked — there's no competing pull from a second shuffle to
// force a decision.
func TestFuseParallelDos_NoBoundaryOnDeadEnd(t *testing.T) {
	p := pipeline.NewPipeline()
	g1 := reg(p, pipeline.OpGroupByKey)
	e := reg(p, pipeline.OpParallelDo, g1)
	e.EstimatedSize = 1

	o.FuseParallelDos(p)

	assert.False(t, e.FusionBoundary, "chain with no terminating GroupByKey should not be marked")
}

// A chain that branches (fan-out) before reaching a second GroupByKey should
// also leave no boundary marked, since the chain-walk can't find an
// unambiguous path to a second GroupByKey to compare against.
func TestFuseParallelDos_NoBoundaryOnFanOut(t *testing.T) {
	p := pipeline.NewPipeline()
	g1 := reg(p, pipeline.OpGroupByKey)
	f := reg(p, pipeline.OpParallelDo, g1)
	f.EstimatedSize = 1
	reg(p, pipeline.OpParallelDo, f) // first consumer of f
	reg(p, pipeline.OpGroupByKey, f) // second consumer of f (fan-out)

	o.FuseParallelDos(p)

	assert.False(t, f.FusionBoundary, "fan-out node should not be marked by the min-size comparison")
}

// ---------------------------------------------------------------------------
// MscrFusion
// ---------------------------------------------------------------------------

// source -> a -> b -> g1 -> c -> d -> g2 -> cv
// c is marked as the fusion boundary (simulating FuseParallelDos' output).
//
// Expected blocks:
//
//	B1: [source, a, b, g1]   (ends at GroupByKey g1)
//	B2: [c]                  (ends at FusionBoundary)
//	B3: [d, g2]              (ends at GroupByKey g2)
//	B4: [cv]                 (ends — no dependants)
//
// wired B1 -> B2 -> B3 -> B4.
func TestMscrFusion_LinearChain(t *testing.T) {
	p := pipeline.NewPipeline()
	src := reg(p, pipeline.OpSource)
	a := reg(p, pipeline.OpParallelDo, src)
	b := reg(p, pipeline.OpParallelDo, a)
	g1 := reg(p, pipeline.OpGroupByKey, b)
	c := reg(p, pipeline.OpParallelDo, g1)
	d := reg(p, pipeline.OpParallelDo, c)
	g2 := reg(p, pipeline.OpGroupByKey, d)
	cv := reg(p, pipeline.OpCombineValues, g2)

	c.FusionBoundary = true // simulate FuseParallelDos having picked c

	if debug {
		fmt.Println("BEFORE MSCR:")
		for _, nw := range p.Path() {
			fmt.Printf("  %s\n", nw.Kind)
		}
	}

	mg := o.MscrFusion(p)
	blocks := mg.Nodes()

	if debug {
		fmt.Println("AFTER MSCR (blocks):")
		for i, blk := range blocks {
			fmt.Printf("  block %d: %v\n", i, kinds(blk.Queue))
		}
	}

	if !assert.Len(t, blocks, 4, "expected exactly 4 execution blocks") {
		return
	}

	// Map each original node to the index of the block that contains it.
	ownerOf := map[*pipeline.NodeWrapper]int{}
	for i, blk := range blocks {
		for _, nw := range blk.Queue {
			ownerOf[nw] = i
		}
	}

	i1, i2, i3, i4 := ownerOf[src], ownerOf[c], ownerOf[d], ownerOf[cv]
	block1, block2, block3, block4 := blocks[i1], blocks[i2], blocks[i3], blocks[i4]

	assert.Equal(t, []pipeline.OpKind{
		pipeline.OpSource, pipeline.OpParallelDo, pipeline.OpParallelDo, pipeline.OpGroupByKey,
	}, kinds(block1.Queue))
	assert.Equal(t, []pipeline.OpKind{pipeline.OpParallelDo}, kinds(block2.Queue))
	assert.Equal(t, []pipeline.OpKind{
		pipeline.OpParallelDo, pipeline.OpGroupByKey,
	}, kinds(block3.Queue))
	assert.Equal(t, []pipeline.OpKind{pipeline.OpCombineValues}, kinds(block4.Queue))

	// Confirm the blocks were actually split (not silently merged into one).
	assert.NotEqual(t, i1, i2)
	assert.NotEqual(t, i2, i3)
	assert.NotEqual(t, i3, i4)

	// Wiring between blocks.
	assert.Contains(t, block1.Dependents(), block2, "block1 -> block2")
	assert.Contains(t, block2.Dependents(), block3, "block2 -> block3")
	assert.Contains(t, block3.Dependents(), block4, "block3 -> block4")

	assert.Contains(t, block2.Dependencies(), block1, "block2 depends on block1")
	assert.Contains(t, block3.Dependencies(), block2, "block3 depends on block2")
	assert.Contains(t, block4.Dependencies(), block3, "block4 depends on block3")
}

// An un-sunk Flatten mid-chain must terminate a block, same as a GroupByKey —
// it should never be silently absorbed into a fused chain with its consumer.
func TestMscrFusion_FlattenTerminatesBlock(t *testing.T) {
	p := pipeline.NewPipeline()
	src1 := reg(p, pipeline.OpSource)
	src2 := reg(p, pipeline.OpSource)
	a := reg(p, pipeline.OpParallelDo, src1)
	flt := reg(p, pipeline.OpFlatten, a, src2)
	after := reg(p, pipeline.OpParallelDo, flt)

	mg := o.MscrFusion(p)
	blocks := mg.Nodes()

	fltBlockIdx, afterBlockIdx := -1, -1
	for i, blk := range blocks {
		for _, nw := range blk.Queue {
			if nw == flt {
				fltBlockIdx = i
				assert.Equal(t, flt, blk.Queue[len(blk.Queue)-1],
					"Flatten must be the last node in its block")
			}
			if nw == after {
				afterBlockIdx = i
			}
		}
	}

	assert.NotEqual(t, -1, fltBlockIdx, "flatten node should be present in some block")
	assert.NotEqual(t, -1, afterBlockIdx, "consumer node should be present in some block")
	assert.NotEqual(t, fltBlockIdx, afterBlockIdx, "Flatten and its consumer must be in different blocks")
}

// A fan-out node (multiple dependants) must also terminate its block, even
// with no explicit FusionBoundary flag set — the fan-out itself forces it.
func TestMscrFusion_FanOutTerminatesBlock(t *testing.T) {
	p := pipeline.NewPipeline()
	src := reg(p, pipeline.OpSource)
	fanOut := reg(p, pipeline.OpParallelDo, src)
	left := reg(p, pipeline.OpParallelDo, fanOut)
	right := reg(p, pipeline.OpParallelDo, fanOut)

	mg := o.MscrFusion(p)
	blocks := mg.Nodes()

	fanOutBlockIdx, leftBlockIdx, rightBlockIdx := -1, -1, -1
	for i, blk := range blocks {
		for _, nw := range blk.Queue {
			switch nw {
			case fanOut:
				fanOutBlockIdx = i
				assert.Equal(t, fanOut, blk.Queue[len(blk.Queue)-1],
					"fan-out node must be the last node in its block")
			case left:
				leftBlockIdx = i
			case right:
				rightBlockIdx = i
			}
		}
	}

	assert.NotEqual(t, fanOutBlockIdx, leftBlockIdx, "fan-out node and its left consumer must be in different blocks")
	assert.NotEqual(t, fanOutBlockIdx, rightBlockIdx, "fan-out node and its right consumer must be in different blocks")
	assert.NotEqual(t, leftBlockIdx, rightBlockIdx, "the two fan-out branches must be in different blocks")
}
