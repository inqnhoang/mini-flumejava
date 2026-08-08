package mscrgraph

import u "mini-flumejava/util"

type MscrGraph struct {
	nodes []*ExecutionBlock
}

func NewMscrGraph() *MscrGraph {
	return &MscrGraph{}
}

func (mg *MscrGraph) Nodes() []*ExecutionBlock {
	return mg.nodes
}

func (mg *MscrGraph) SetNodes(nodes []*ExecutionBlock) {
	mg.nodes = nodes
}

func (mg *MscrGraph) AddNode(eb *ExecutionBlock) {
	mg.nodes = append(mg.nodes, eb)
}

func (mg *MscrGraph) AddEdge(dependency *ExecutionBlock, dependent *ExecutionBlock) {
	dependency.AddDependent(dependent)
	dependent.AddDependency(dependency)
}

// Returns the amount of nodes within the pipeline
func (mg *MscrGraph) length() int {
	return len(mg.nodes)
}

func (mg *MscrGraph) Sort() []*ExecutionBlock {
	var q []*ExecutionBlock
	var path []*ExecutionBlock

	for _, eb := range mg.nodes {
		if eb.Remaining == 0 {
			q = u.PushBack(q, eb)
		}
	}

	for len(q) != 0 {
		front := u.Front(q)
		_, q = u.Pop(q)
		path = u.PushBack(path, front)

		for _, dep := range front.Dependents() {
			dep.Remaining--

			if dep.Remaining == 0 {
				q = u.PushBack(q, dep)
			}
		}
	}
	return path
}
