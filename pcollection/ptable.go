package pcollection

import (
	"mini-javaflume/pipeline"
)

type KV[K comparable, V any] struct {
	Key   K
	Value V
}
type PTable[K comparable, V any] struct {
	items       []KV[K, V]
	materialize func() // needs this to be materialize to run funcs within the array
	executed    bool
	wrapper     *pipeline.NodeWrapper
}

func NewPTable[K comparable, V any](items []KV[K, V]) *PTable[K, V] {
	return &PTable[K, V]{items: items}
}

func (pt *PTable[K, V]) Materialize() {
	if pt.executed {
		return
	}
	pt.materialize()
	pt.executed = true
}

func (pt *PTable[K, V]) Pipeline() *pipeline.Pipeline {
	return pt.wrapper.Pipeline()
}

func (pt *PTable[K, V]) NodeWrapper() *pipeline.NodeWrapper {
	return pt.NodeWrapper()
}

func (pt *PTable[K, V]) Length() int {
	return len(pt.items)
}

func groupByKey[K comparable, V any](pt *PTable[K, V]) *PTable[K, []V] {
	// TODO not currency safe
	out := &PTable[K, []V]{}
	out.materialize = func() {
		temp := make(map[K][]V)
		for _, pv := range pt.items {
			temp[pv.Key] = append(temp[pv.Key], pv.Value)
		}

		out.items = make([]KV[K, []V], 0, len(temp))
		for k, v := range temp {
			out.items = append(out.items, KV[K, []V]{Key: k, Value: v})
		}
	}

	p := pt.Pipeline()
	out.wrapper = p.Register(out, pt.wrapper)

	return out
}

func combineValues[K comparable, V any, R any](pt *PTable[K, []V], reducFn func([]V) R) *PTable[K, R] {
	// TODO not currency safe
	out := &PTable[K, R]{}
	out.materialize = func() {
		out.items = make([]KV[K, R], 0, pt.Length())
		for _, kv := range pt.items {
			out.items = append(out.items, KV[K, R]{Key: kv.Key, Value: reducFn(kv.Value)})
		}
	}

	p := pt.Pipeline()
	out.wrapper = p.Register(out, pt.wrapper)
	return out
}
