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
	// TODO not currency safe
	out := &PCollection[R]{}
	pc.addDependency(func() {
		for i, v := range pc.elements {
			if out.elements == nil {
				out.elements = make([]R, len(pc.elements))
			}
			out.elements[i] = fn(v)
		}
		out.executed = true
	})
	return out
}

func flatten[T any](sources ...*PCollection[T]) *PCollection[T] {
	// TODO not currency safe
	out := &PCollection[T]{}

	remaining := len(sources)
	for _, src := range sources {
		src.addDependency(func() {
			out.elements = append(out.elements, src.elements...)
			remaining--

			if remaining == 0 {
				out.executed = true
			}
		})
	}
	return out
}
