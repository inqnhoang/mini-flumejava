package pcollection

type PTable[K comparable, V any] struct {
	items      map[K]V
	dependents []func()
	executed   bool
}

func NewPTable[K comparable, V any]() *PTable[K, V] {
	return &PTable[K, V]{items: make(map[K]V)}
}
