package pcollection

type PCollection[T any] struct {
	elements   []T
	dependents []func()
	executed   bool
}

func NewPCollection[T any](elements []T) *PCollection[T] {
	return &PCollection[T]{elements: elements}
}
