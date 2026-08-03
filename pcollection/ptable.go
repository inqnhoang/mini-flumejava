package pcollection

import (
	"mini-flumejava/pipeline"
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

func NewPTable[K comparable, V any](p *pipeline.Pipeline, items []KV[K, V]) *PTable[K, V] {
	pt := &PTable[K, V]{items: items, executed: true}
	pt.wrapper = p.Register(pipeline.OpSource, pt)
	return pt
}

func (pt *PTable[K, V]) Materialize() {
	if pt.executed {
		return
	}
	pt.materialize()
	pt.executed = true
}

func (pt *PTable[K, V]) Items() []KV[K, V] {
	return pt.items
}

func (pt *PTable[K, V]) Pipeline() *pipeline.Pipeline {
	return pt.wrapper.Pipeline()
}

func (pt *PTable[K, V]) NodeWrapper() *pipeline.NodeWrapper {
	return pt.wrapper
}

func (pt *PTable[K, V]) Run() {
	pt.Pipeline().Run()
}

func (pt *PTable[K, V]) length() int {
	return len(pt.items)
}

func GroupByKey[K comparable, V any](pc *PCollection[KV[K, V]]) *PTable[K, []V] {
	// TODO not concurrency safe
	out := &PTable[K, []V]{}
	out.materialize = func() {
		pc.Materialize()

		temp := make(map[K][]V)
		for _, pv := range pc.elements {
			temp[pv.Key] = append(temp[pv.Key], pv.Value)
		}

		out.items = make([]KV[K, []V], 0, len(temp))
		for k, v := range temp {
			out.items = append(out.items, KV[K, []V]{Key: k, Value: v})
		}
	}

	p := pc.Pipeline()
	out.wrapper = p.Register(pipeline.OpGroupByKey, out, pc.NodeWrapper())

	return out
}

func CombineValues[K comparable, V any, R any](pt *PTable[K, []V], reducFn func([]V) R) *PTable[K, R] {
	// TODO not currency safe
	out := &PTable[K, R]{}
	out.materialize = func() {
		pt.Materialize()

		out.items = make([]KV[K, R], 0, pt.length())
		for _, kv := range pt.items {
			out.items = append(out.items, KV[K, R]{Key: kv.Key, Value: reducFn(kv.Value)})
		}
	}

	p := pt.Pipeline()
	out.wrapper = p.Register(pipeline.OpCombineValues, out, pt.wrapper)
	return out
}
