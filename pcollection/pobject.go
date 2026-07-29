package pcollection

type PObject[T comparable] struct {
	value      T
	dependents []func()
	executed   bool
}

func NewPObject[T comparable](value T) *PObject[T] {
	return &PObject[T]{value: value}
}
