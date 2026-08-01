package pipeline

type Pipeline struct {
	nodes []*NodeWrapper
}

func (p *Pipeline) Register(new Node, dependencies ...*NodeWrapper) *NodeWrapper {
	nw := &NodeWrapper{
		Ds:           new,
		Dependencies: dependencies,
		pipeline:     p,
	}

	for _, dep := range dependencies {
		dep.addDependent(nw)
	}
	p.addNode(nw)
	return nw
}

func (p *Pipeline) addNode(nw *NodeWrapper) {
	p.nodes = append(p.nodes, nw)
}
