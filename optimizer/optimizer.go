package optimizer

import (
	"math"
	"mini-flumejava/mscrgraph"
	"mini-flumejava/pipeline"
	u "mini-flumejava/util"
)

/*
Sinks all flattens within the pipeline if the flatten satisfies:
  - Is a Flatten OpKind
  - Has only one dependent
  - Its dependent is a single ParallelDo

It performs the following steps:
  - Rebuilds each nodes within the path recursively
  - Uses memoization to reduce Node exploration
  - Memo stores a mapping of Node to its rebuilt dependencies
  - Add each newly built edge to the pipeline

Base case:
  - The node is a source
  - Rebuilds the source with no dependencies and adds it to memo

Intermediate case:
  - Uses memo to capture rebuilt Node
  - Takes new or same dependencies and rebuilds the Node
    with each dependencies
  - e.g. a sink flatten produces four new dependencies for its immediate
    ParallelDo, when previously it was the one flatten
  - Maps node to its dependencies as is to memo (e.g. a Flatten that doesn't have a
    ParallelDo as its dependent/consumer)

Satisfies SinkFlatten:
  - Rebuilds each dependency within the flatten (can be
    any Node type e.g. a previous flatten sunk to > 1 dependencies)
  - Rebuilds a new edge mapping each captured dependency to the consumer (e.g. 6
    dependencies will create 6 edges)
  - Removes Flatten logically & maps consumer to its new rebuilt dependencies

Returns a new *Pipeline
*/
func SinkFlattens(p *pipeline.Pipeline) *pipeline.Pipeline {
	memo := map[*pipeline.NodeWrapper][]*pipeline.NodeWrapper{}

	var rebuild func(old *pipeline.NodeWrapper) []*pipeline.NodeWrapper
	rebuild = func(old *pipeline.NodeWrapper) []*pipeline.NodeWrapper {
		if built, ok := memo[old]; ok {
			return built
		}

		// Sink Flatten
		if old.Kind == pipeline.OpFlatten &&
			len(old.Dependants) == 1 &&
			old.Dependants[0].Kind == pipeline.OpParallelDo {

			consumer := old.Dependants[0]
			newEdges := []*pipeline.NodeWrapper{}
			for _, producer := range old.Dependencies {
				newProducers := rebuild(producer)
				for _, np := range newProducers {
					newEdge := consumer.RebuildWith(np)
					newEdges = append(newEdges, newEdge)
				}
			}

			memo[old] = nil
			memo[consumer] = newEdges
			return memo[old]
		}

		if len(old.Dependencies) == 0 {
			memo[old] = []*pipeline.NodeWrapper{old}
			return memo[old]
		}

		// base case & intermediate -- move along the path as is
		var newDeps []*pipeline.NodeWrapper
		for _, dep := range old.Dependencies {
			newDep := rebuild(dep)
			newDeps = append(newDeps, newDep...)
		}
		rebuilt := old.RebuildWith(newDeps...)
		memo[old] = []*pipeline.NodeWrapper{rebuilt}
		return memo[old]
	}

	newP := pipeline.NewPipeline()
	for _, old := range p.Path() {
		for _, nw := range rebuild(old) {
			newP.AddNode(nw)
		}
	}
	return newP
}

/*
Lifts CombineValues for MSCR fusion if it satisfies:
  - Is a GroupByKey OpKind
  - Has only one dependent
  - The depedent is a CombineValues

It performs the following steps:
  - Traverses graph for satisfiability
  - Set GBK's combiner to dependent if it satisfies all conditions

IN-PLACE
*/
func LiftCombineValues(p *pipeline.Pipeline) {
	for _, nw := range p.Path() {
		if nw.Kind != pipeline.OpGroupByKey {
			continue
		}
		if len(nw.Dependants) != 1 {
			continue
		}
		if nw.Dependants[0].Kind != pipeline.OpCombineValues {
			continue
		}

		nw.Combinber = nw.Dependants[0]
	}
}

/*
Generates a chain from a start node and marks the node with the smallest size to
be the fusion boundary for MSCR fusion.

Start a chain if:
  - Is a GroupByKey OpKind
  - Has only one dependent
  - The dependent is a ParallelDo OR CombineValues

End the chain if:
  - Is not the start node
  - Is not a ParallelDo or CombineValues

It performs the following steps:
  - Traverses graph for satisfiability
  - Separates and walks from the node to find a fusion boundary
  - Set NodeWrapper's FusionBoundary to true

IN-PLACE
*/
func FuseParallelDos(p *pipeline.Pipeline) {
	for _, nw := range p.Path() {
		if nw.Kind != pipeline.OpGroupByKey {
			continue
		}
		chain := walkChainToNextGroupByKey(nw)
		if len(chain) == 0 {
			continue
		}

		last := chain[len(chain)-1]
		if len(last.Dependants) != 1 || last.Dependants[0].Kind != pipeline.OpGroupByKey {
			continue
		}

		var minNode *pipeline.NodeWrapper
		var minSize int64 = math.MaxInt64
		for _, node := range chain {
			if node.EstimatedSize < minSize {
				minSize = node.EstimatedSize
				minNode = node
			}
		}
		minNode.FusionBoundary = true
	}
}

/*
Walks the dependents of a start node to find chains for fusion boundaries
*/
func walkChainToNextGroupByKey(start *pipeline.NodeWrapper) []*pipeline.NodeWrapper {
	var chain []*pipeline.NodeWrapper
	cur := start

	for {
		if len(cur.Dependants) != 1 {
			break
		}

		next := cur.Dependants[0]
		if next.Kind == pipeline.OpGroupByKey {
			return chain
		}

		if next.Kind != pipeline.OpParallelDo && next.Kind != pipeline.OpCombineValues {
			break
		}
		chain = append(chain, next)
		cur = next
	}
	return chain
}

func MscrFusion(p *pipeline.Pipeline) *mscrgraph.MscrGraph {
	mg := mscrgraph.NewMscrGraph()

	links := map[*pipeline.NodeWrapper]*mscrgraph.ExecutionBlock{}
	seen := map[*pipeline.NodeWrapper]bool{}

	for _, nw := range p.Path() {
		if ok := seen[nw]; ok {
			continue
		}
		chain := mscrgraph.NewExecutionBlock()
		// walk nodewrapper
		cur := nw
		for {
			seen[cur] = true
			chain.Queue = u.PushBack(chain.Queue, cur)
			links[cur] = chain

			if cur.FusionBoundary || cur.Kind == pipeline.OpFlatten || cur.Kind == pipeline.OpGroupByKey || len(cur.Dependants) != 1 {
				break
			}

			cur = cur.Dependants[0]
		}
		mg.AddNode(chain)
	}

	linkBlocks(mg, links)

	for _, node := range mg.Nodes() {
		node.Remaining = len(node.Dependencies())
	}
	mg.SetNodes(mg.Sort())

	return mg
}

func linkBlocks(mg *mscrgraph.MscrGraph, links map[*pipeline.NodeWrapper]*mscrgraph.ExecutionBlock) {
	for _, block := range mg.Nodes() { // however MscrGraph exposes its blocks
		if len(block.Queue) == 0 {
			continue
		}
		last := block.Queue[len(block.Queue)-1] // whatever type Queue holds

		for _, dependant := range last.Dependants {
			nextBlock, ok := links[dependant]
			if !ok {
				continue
			}
			if nextBlock == block {
				continue
			}
			mg.AddEdge(block, nextBlock)
		}
	}
}
