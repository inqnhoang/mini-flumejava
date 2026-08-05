package pipeline

import u "mini-flumejava/util"

// Pipeline represents the graph structure of PCollection & PTable operations
type Pipeline struct {
	nodes []*NodeWrapper
}

func NewPipeline() *Pipeline {
	return &Pipeline{}
}

func (p *Pipeline) Register(opKind OpKind, new Node, dependencies ...*NodeWrapper) *NodeWrapper {
	nw := &NodeWrapper{
		Ds:           new,
		Kind:         opKind,
		Dependencies: dependencies,
		pipeline:     p,
		remaining:    len(dependencies),
	}

	for _, dep := range dependencies {
		dep.AddDependent(nw)
	}

	p.AddNode(nw)
	return nw
}

// Returns path of Node
func (p *Pipeline) Path() []*NodeWrapper {
	return p.nodes
}

// Runs the full pipeline, assumes they come in unsorted
func (p *Pipeline) Run() {
	p.Sort()

	for _, nw := range p.Sort() {
		nw.Ds.Materialize()
	}
}

// Remove Node at index, idx
func (p *Pipeline) RemoveNodeIdx(idx int) {
	if idx < 0 || idx >= p.length() {
		panic("RemoveNodeIdx: out of bounds")
	}

	if idx == p.length()-1 {
		p.nodes = p.nodes[:idx]
	} else {
		p.nodes = append(p.nodes[:idx], p.nodes[idx+1:]...)
	}
}

func (p *Pipeline) AddNode(nw *NodeWrapper) {
	p.nodes = append(p.nodes, nw)
}

// Returns the amount of nodes within the pipeline
func (p *Pipeline) length() int {
	return len(p.nodes)
}

// TopoSorts Path using Khan's Algorithm
func (p *Pipeline) Sort() []*NodeWrapper {
	n := p.length()
	q := make([]*NodeWrapper, 0, n)
	path := make([]*NodeWrapper, 0, n)

	for _, nw := range p.nodes {
		if nw.remaining == 0 {
			q = append(q, nw)
		}
	}

	for len(q) != 0 {
		var top *NodeWrapper
		top, q = u.Pop(q)
		path = u.PushBack(path, top)

		for _, next := range top.Dependants {
			next.remaining--
			if next.remaining == 0 {
				q = u.Push(next, q)
			}
		}

	}
	return path
}
