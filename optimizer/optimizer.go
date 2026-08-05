package optimizer

import "mini-flumejava/pipeline"

// TODO add comments before I forget what I cooked
func SinkFlattens(p *pipeline.Pipeline) *pipeline.Pipeline {
	memo := map[*pipeline.NodeWrapper][]*pipeline.NodeWrapper{}

	var rebuild func(old *pipeline.NodeWrapper) []*pipeline.NodeWrapper
	rebuild = func(old *pipeline.NodeWrapper) []*pipeline.NodeWrapper {
		if built, ok := memo[old]; ok {
			return built
		}

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

		// base case -- move along the path as is
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
