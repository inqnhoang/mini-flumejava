package pcollection

import (
	"mini-flumejava/pipeline"
	u "mini-flumejava/util"
)

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

func (pc *PCollection[T]) Materialize() {
	if pc.executed {
		return
	}
	pc.materialize()
	pc.executed = true
}

func (pc *PCollection[T]) Elements() []T {
	return pc.elements
}

func (pc *PCollection[T]) Pipeline() *pipeline.Pipeline {
	return pc.wrapper.Pipeline()
}

func (pc *PCollection[T]) NodeWrapper() *pipeline.NodeWrapper {
	return pc.wrapper
}

func (pc *PCollection[T]) Run() {
	pc.Pipeline().Run()
}

func (pc *PCollection[T]) length() int {
	return len(pc.elements)
}

// TODO: remove dependents & use nodewrappers
func ParallelDo[T, R any](pc *PCollection[T], mapFn func(T) R) *PCollection[R] {
	// TODO not currency safe
	out := &PCollection[R]{}
	out.materialize = func() {
		out.elements = u.MapSlice(pc.elements, mapFn)
	}

	// DEFER
	p := pc.Pipeline()
	out.wrapper = p.Register(pipeline.OpParallelDo, out, pc.NodeWrapper())

	out.wrapper.Rebuild = func(newInput *pipeline.NodeWrapper) *pipeline.NodeWrapper {
		newPc := newInput.Ds.(*PCollection[T])
		dup := ParallelDo(newPc, mapFn)
		return dup.wrapper
	}

	return out
}

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
	out.wrapper = p.Register(pipeline.OpFlatten, out, deps...)

	return out
}
