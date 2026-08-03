package optimizer

import "mini-flumejava/pipeline"

func SinkFlattens(p *pipeline.Pipeline) {
	for i, nw := range p.Path() {
		if nw.Kind != pipeline.OpFlatten {
			continue
		}

		if len(nw.Dependants) != 1 {
			continue
		}

		consumer := nw.Dependants[0]
		if consumer.Kind != pipeline.OpParallelDo {
			continue
		}

		for _, producer := range nw.Dependencies {
			consumer = consumer.Rebuild(producer)
		}

		p.RemoveNodeIdx(i)
	}
}

// After sorting you'll have an array to traverse, merge if parallelDo
