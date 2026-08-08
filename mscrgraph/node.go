package mscrgraph

import "mini-flumejava/pipeline"

type ExecutionBlock struct {
	Queue        []*pipeline.NodeWrapper
	dependencies []*ExecutionBlock
	dependents   []*ExecutionBlock
	Remaining    int
}

func NewExecutionBlock() *ExecutionBlock {
	return &ExecutionBlock{}
}

func (eb *ExecutionBlock) AddDependency(dep *ExecutionBlock) {
	eb.dependencies = append(eb.dependencies, dep)
}

func (eb *ExecutionBlock) AddDependent(dep *ExecutionBlock) {
	eb.dependents = append(eb.dependents, dep)
}

func (eb *ExecutionBlock) Dependents() []*ExecutionBlock {
	return eb.dependents
}

func (eb *ExecutionBlock) Dependencies() []*ExecutionBlock {
	return eb.dependencies
}

func (e *ExecutionBlock) run() {
	for _, nw := range e.Queue {
		nw.Ds.Materialize()
	}
}
