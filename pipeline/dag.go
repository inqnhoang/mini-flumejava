package pipeline

type Node interface {
	Materialize()
}

type NodeWrapper struct {
	Ds           Node
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

func (nw *NodeWrapper) addDependent(dependant *NodeWrapper) {
	nw.Dependants = append(nw.Dependants, dependant)
}

func (nw *NodeWrapper) addDependencies(dependencies []*NodeWrapper) {
	for _, depedency := range dependencies {
		nw.Dependencies = append(nw.Dependencies, depedency)
	}
}
