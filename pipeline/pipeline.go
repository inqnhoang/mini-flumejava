package pipeline

import u "mini-flumejava/util"

type Pipeline struct {
	nodes []*NodeWrapper
}

func NewPipeline() *Pipeline {
	return &Pipeline{}
}

func (p *Pipeline) Register(new Node, dependencies ...*NodeWrapper) *NodeWrapper {
	nw := &NodeWrapper{
		Ds:           new,
		Dependencies: dependencies,
		pipeline:     p,
		remaining:    len(dependencies),
	}

	for _, dep := range dependencies {
		dep.addDependent(nw)
	}

	p.addNode(nw)
	return nw
}

func (p *Pipeline) Run() {
	for _, nw := range p.sort() {
		nw.Ds.Materialize()
	}
}

func (p *Pipeline) sort() []*NodeWrapper {
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

func (p *Pipeline) addNode(nw *NodeWrapper) {
	p.nodes = append(p.nodes, nw)
}

func (p *Pipeline) length() int {
	return len(p.nodes)
}
