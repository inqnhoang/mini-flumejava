package pcollection

import (
	"mini-flumejava/pipeline"
	u "mini-flumejava/util"
)

// Key Value pair
type KV[K comparable, V any] struct {
	Key   K
	Value V
}

// PCollections represents a deferrable collection of Key, Value pairs
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

// Materializes this PTable when it's ready for execution
func (pt *PTable[K, V]) Materialize() {
	if pt.executed {
		return
	}
	pt.materialize()
	pt.executed = true
}

// Returns the items of this PTable
func (pt *PTable[K, V]) Items() []KV[K, V] {
	return pt.items
}

// Returns the length of this PTable
func (pt *PTable[K, V]) length() int {
	return len(pt.items)
}

// Returns the Pipeline this PTable is apart of
func (pt *PTable[K, V]) Pipeline() *pipeline.Pipeline {
	return pt.wrapper.Pipeline()
}

// Returns the NodeWrapper encapsulating this PTable
func (pt *PTable[K, V]) NodeWrapper() *pipeline.NodeWrapper {
	return pt.wrapper
}

// Allows the pipeline to be ran from any node
func (pt *PTable[K, V]) Run() {
	pt.Pipeline().Run()
}

/*
Groups a PCollection[KV[K, V]], Key Value pairs of key type K, and value type V, into
a single PTable with KV pairs KV[K, []V], mapping one key to many values

It performs the following steps:
  - Construct materializing function by doing a single pass to making a map,
    then converts it to pair values (deferral)
  - Registers input & output to a pipeline sent down by the input param *pc
  - Construct a rebuild function to build new pipelines

Returns *NodeWrapper
*/
func GroupByKey[K comparable, V any](pc *PCollection[KV[K, V]]) *PTable[K, []V] {
	// TODO not concurrency safe
	out := &PTable[K, []V]{}

	out.NodeWrapper().SetEstimatedSize(int64(len(pc.elements) * int(u.TypeSize[V]())))
	// DEFER
	out.materialize = func() {
		pc.Materialize()

		// single pass
		temp := make(map[K][]V)
		for _, pv := range pc.elements {
			temp[pv.Key] = append(temp[pv.Key], pv.Value)
		}

		// construct KV pairs
		out.items = make([]KV[K, []V], 0, len(temp))
		for k, v := range temp {
			out.items = append(out.items, KV[K, []V]{Key: k, Value: v})
		}
	}

	p := pc.Pipeline()
	// Maps dependencies AND dependents relative to the output and dependencies
	out.wrapper = p.Register(pipeline.OpGroupByKey, out, pc.NodeWrapper())

	out.wrapper.RebuildWith = func(newInputs ...*pipeline.NodeWrapper) *pipeline.NodeWrapper {
		newPc := newInputs[0].Ds.(*PCollection[KV[K, V]])
		dup := GroupByKey(newPc)
		return dup.wrapper
	}

	return out
}

/*
Reduces a PTable of Key Value pairs that maps keys of type K, and values of type V as one-to-many,
to a PTable that maps one-to-one using a reducing function, reducFn, on the values of each key

It performs the following steps:
  - Construct materializing function by doing a single pass through the KV Pairs, and
    reducing the slice of values with the reducFn to a single value
  - Registers input & output to a pipeline sent down by the input param *pc
  - Construct a rebuild function to build new pipelines

Returns *NodeWrapper
*/
func CombineValues[K comparable, V any, R any](pt *PTable[K, []V], reducFn func([]V) R) *PTable[K, R] {
	// TODO not currency safe
	out := &PTable[K, R]{}

	out.NodeWrapper().SetEstimatedSize(int64(len(pt.items) * (int(u.TypeSize[K]()) + int(u.TypeSize[V]()))))
	// DEFER
	out.materialize = func() {
		pt.Materialize()

		out.items = make([]KV[K, R], 0, pt.length())
		for _, kv := range pt.items {
			out.items = append(out.items, KV[K, R]{Key: kv.Key, Value: reducFn(kv.Value)})
		}
	}

	p := pt.Pipeline()
	// Maps dependencies AND dependents relative to the output and dependencies
	out.wrapper = p.Register(pipeline.OpCombineValues, out, pt.wrapper)

	// Rebuild by producing a new *NodeWrapper with the same OP, but new inputs
	out.wrapper.RebuildWith = func(newInputs ...*pipeline.NodeWrapper) *pipeline.NodeWrapper {
		newPt := newInputs[0].Ds.(*PTable[K, []V])
		dup := CombineValues(newPt, reducFn)
		return dup.wrapper
	}
	return out
}
