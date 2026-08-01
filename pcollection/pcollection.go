package pcollection

import (
	"mini-javaflume/pipeline"
)

type PCollection[T any] struct {
	elements    []T
	executed    bool
	materialize func()
	wrapper     *pipeline.NodeWrapper
}

func NewPCollection[T any](elements []T) *PCollection[T] {
	return &PCollection[T]{elements: elements}
}

func (pc *PCollection[T]) Materialize() {
	if pc.executed {
		return
	}
	pc.materialize()
	pc.executed = true
}

func (pc *PCollection[T]) Pipeline() *pipeline.Pipeline {
	return pc.wrapper.Pipeline()
}

func (pc *PCollection[T]) NodeWrapper() *pipeline.NodeWrapper {
	return pc.NodeWrapper()
}

func (pc *PCollection[T]) Length() int {
	return len(pc.elements)
}

// TODO: remove dependents & use nodewrappers
func parallelDo[T, R any](pc *PCollection[T], mapFn func(T) R) *PCollection[R] {
	// TODO not currency safe
	out := &PCollection[R]{}
	out.materialize = func() {
		out.elements = mapSlice(pc.elements, mapFn)
	}

	// DEFER
	p := pc.Pipeline()
	out.wrapper = p.Register(out, pc.NodeWrapper())

	return out
}

func flatten[T any](sources ...*PCollection[T]) *PCollection[T] {
	// TODO not currency safe
	out := &PCollection[T]{}
	out.materialize = func() {
		length := 0
		for _, src := range sources {
			length += src.Length()
		}

		out.elements = make([]T, 0, length)
		for _, src := range sources {
			out.elements = append(out.elements, src.elements...)
		}
	}

	p := sources[0].Pipeline()
	for _, src := range sources[1:] {
		if src.Pipeline() != p {
			panic("flatten: sources belong to different pipelines")
		}
	}

	out.wrapper = p.Register(out, mapSlice(sources, func(s *PCollection[T]) *pipeline.NodeWrapper {
		return s.wrapper
	})...)

	return out
}
