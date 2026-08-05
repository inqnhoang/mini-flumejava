package pcollection

import (
	"mini-flumejava/pipeline"
	u "mini-flumejava/util"
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
	// TODO not currency safe

	// DEFER
	out := &PCollection[R]{}
	out.materialize = func() {
		out.elements = u.MapSlice(pc.elements, mapFn)
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
	out.materialize = func() {
		for _, src := range sources {
			src.Materialize()
		}

		length := 0
		for _, src := range sources {
			length += src.length()
		}

		out.elements = make([]T, 0, length)
		for _, src := range sources {
			out.elements = append(out.elements, src.elements...)
		}
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
