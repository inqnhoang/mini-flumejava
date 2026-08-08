package pcollection

import (
	"mini-flumejava/pipeline"
	u "mini-flumejava/util"
	"runtime"
	"sync"
)

// PCollections represents a deferrable collection of elements
type PCollection[T any] struct {
	elements    []T
	executed    bool
	materialize func()
	wrapper     *pipeline.NodeWrapper
}

func NewPCollection[T any](p *pipeline.Pipeline, elements []T) *PCollection[T] {
	pc := &PCollection[T]{elements: elements, executed: true}
	pc.wrapper = p.Register(pipeline.OpSource, pc)
	return pc
}

// Materializes this PCollection when it's ready for execution
func (pc *PCollection[T]) Materialize() {
	if pc.executed {
		return
	}
	pc.materialize()
	pc.executed = true
}

// Returns the elements of this PCollection
func (pc *PCollection[T]) Elements() []T {
	return pc.elements
}

// Returns the length of this PCollection
func (pc *PCollection[T]) length() int {
	return len(pc.elements)
}

// Returns the Pipeline this PCollection is apart of
func (pc *PCollection[T]) Pipeline() *pipeline.Pipeline {
	return pc.wrapper.Pipeline()
}

// Returns the NodeWrapper encapsulating this PCollection
func (pc *PCollection[T]) NodeWrapper() *pipeline.NodeWrapper {
	return pc.wrapper
}

// Allows the pipeline to be ran from any node
func (pc *PCollection[T]) Run() {
	pc.Pipeline().Run()
}

/*
Maps a PCollection[T], type T, with a mappingFn to produce PCollection[R], type R

It performs the following steps:
  - Construct materializing function using MapSlice (deferral)
  - Registers input & output to a pipeline sent down by the input param *pc
  - Construct a rebuild function to build new pipelines

Returns *NodeWrapper
*/
func ParallelDo[T, R any](pc *PCollection[T], mapFn func(T) R) *PCollection[R] {
	out := &PCollection[R]{}
	// out.NodeWrapper().SetEstimatedSize(int64(len(pc.elements) * int(u.TypeSize[T]())))

	// DEFER
	out.materialize = func() {
		n := len(pc.elements)
		out.elements = make([]R, n)

		numWorkers := runtime.GOMAXPROCS(0)
		chunkSize := (n + numWorkers - 1) / numWorkers

		var wg sync.WaitGroup
		for w := 0; w < numWorkers; w++ {
			start := w * chunkSize
			end := min(start+chunkSize, n)
			if start >= n {
				break
			}
			wg.Add(1)
			go func(start, end int) {
				defer wg.Done()
				for i := start; i < end; i++ {
					out.elements[i] = mapFn(pc.elements[i])
				}
			}(start, end)
		}
		wg.Wait()
	}
	p := pc.Pipeline()

	// Maps dependencies AND dependents relative to the output and dependencies
	out.wrapper = p.Register(pipeline.OpParallelDo, out, pc.NodeWrapper())

	// Rebuild by producing a new *NodeWrapper with the same OP, but new inputs
	out.wrapper.RebuildWith = func(newInputs ...*pipeline.NodeWrapper) *pipeline.NodeWrapper {
		newPc := newInputs[0].Ds.(*PCollection[T])
		dup := ParallelDo(newPc, mapFn)
		return dup.wrapper
	}

	return out
}

/*
Takes a slice of PCollection[T]s, type T, and produces a unified, flatten PCollection[T]
containing all the elements within the provided sources

It performs the following steps:
  - Construct materializing function by materializing each source, then
    appends all the elements from each source to a output slice
  - Registers each source as an input tied to the output to a pipeline
    sent down by the input param *sources
  - Construct a rebuild function to build new pipelines

Returns *NodeWrapper
*/
func Flatten[T any](sources ...*PCollection[T]) *PCollection[T] {
	// TODO not currency safe
	if len(sources) == 0 {
		panic("flatten: at least one source required")
	}

	p := sources[0].Pipeline()
	for _, src := range sources[1:] {
		if src.Pipeline() != p {
			panic("flatten: sources belong to different pipelines")
		}
	}

	out := &PCollection[T]{}

	// var size int64
	// for _, src := range sources {
	// 	size += int64(len(src.elements) * int(u.TypeSize[T]()))
	// }
	// out.NodeWrapper().SetEstimatedSize(size)

	// DEFER
	out.materialize = func() {
		offsets := make([]int, len(sources)+1)
		for i, src := range sources {
			offsets[i+1] = offsets[i] + src.length()
		}
		out.elements = make([]T, offsets[len(sources)])

		var wg sync.WaitGroup
		for i, src := range sources {
			wg.Add(1)
			go func(i int, src *PCollection[T]) {
				defer wg.Done()
				copy(out.elements[offsets[i]:offsets[i+1]], src.elements)
			}(i, src)
		}
		wg.Wait()
	}

	deps := u.MapSlice(sources, func(s *PCollection[T]) *pipeline.NodeWrapper {
		return s.wrapper
	})

	// Maps dependencies AND dependents relative to the output and dependencies
	out.wrapper = p.Register(pipeline.OpFlatten, out, deps...)

	// Rebuild by producing a new *NodeWrapper with the same OP, but new inputs
	out.wrapper.RebuildWith = func(newInputs ...*pipeline.NodeWrapper) *pipeline.NodeWrapper {
		newSources := u.MapSlice(newInputs, func(nw *pipeline.NodeWrapper) *PCollection[T] {
			return nw.Ds.(*PCollection[T])
		})
		dup := Flatten(newSources...)
		return dup.wrapper
	}

	return out
}
