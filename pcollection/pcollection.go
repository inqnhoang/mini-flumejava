package pcollection

type PCollection[T any] struct {
	elements   []T
	dependents []func()
	executed   bool
}

func NewPCollection[T any](elements []T) *PCollection[T] {
	return &PCollection[T]{elements: elements}
}

func (pc *PCollection[T]) addDependency(fn func()) {
	pc.dependents = append(pc.dependents, fn)
}

func parallelDo[T, R any](pc *PCollection[T], fn func(T) R) *PCollection[R] {
	out := &PCollection[R]{}
	out.elements = make([]R, len(pc.elements))
	pc.addDependency(func() {
		for i, v := range pc.elements {
			out.elements[i] = fn(v)
		}
	})
	return out
}
