package pcollection

type KV[K comparable, V any] struct {
	Key   K
	Value V
}
type PTable[K comparable, V any] struct {
	items      []KV[K, V]
	dependents []func()
	executed   bool
}

func NewPTable[K comparable, V any](items []KV[K, V]) *PTable[K, V] {
	return &PTable[K, V]{items: items}
}

func (pt *PTable[K, V]) addDependency(fn func()) {
	pt.dependents = append(pt.dependents, fn)
}

func groupByKey[K comparable, V any](pt *PTable[K, V]) *PTable[K, []V] {
	out := &PTable[K, []V]{}

	pt.addDependency(func() {
		temp := make(map[K][]V)
		for _, kv := range pt.items {
			temp[kv.Key] = append(temp[kv.Key], kv.Value)
		}

		for k, v := range temp {
			out.items = append(out.items, KV[K, []V]{Key: k, Value: v})
		}
		out.executed = true
	})

	return out
}
