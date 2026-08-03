package pipeline

import "fmt"

type OpKind int

const (
	OpSource OpKind = iota
	OpParallelDo
	OpFlatten
	OpGroupByKey
	OpCombineValues
)

type Node interface {
	Materialize()
}

type NodeWrapper struct {
	Ds           Node
	Kind         OpKind
	Rebuild      func(newInput *NodeWrapper) *NodeWrapper
	Dependants   []*NodeWrapper
	Dependencies []*NodeWrapper
	pipeline     *Pipeline
	remaining    int
}

// getters & setters
func (nw *NodeWrapper) Pipeline() *Pipeline {
	return nw.pipeline
}

func (nw *NodeWrapper) Remaining() int {
	return nw.remaining
}

func (nw *NodeWrapper) AddDependent(dependant *NodeWrapper) {
	nw.Dependants = append(nw.Dependants, dependant)
}

func (nw *NodeWrapper) AddDependencies(dependencies []*NodeWrapper) {
	for _, depedency := range dependencies {
		nw.Dependencies = append(nw.Dependencies, depedency)
	}
}

func (k OpKind) String() string {
	switch k {
	case OpSource:
		return "Source"
	case OpParallelDo:
		return "ParallelDo"
	case OpFlatten:
		return "Flatten"
	case OpGroupByKey:
		return "GroupByKey"
	case OpCombineValues:
		return "CombineValues"
	default:
		return fmt.Sprintf("OpKind(%d)", int(k))
	}
}
