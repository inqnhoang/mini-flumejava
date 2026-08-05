package optimizer

import "mini-flumejava/pipeline"

/*
Sinks all flattens within the pipeline if the flatten satisfies:
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
