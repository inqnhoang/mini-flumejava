package tester

import (
	"mini-flumejava/pcollection"
	"mini-flumejava/pipeline"
)

type Tester struct {
	Pipeline *pipeline.Pipeline
}

func NewTester() *Tester {
	return &Tester{Pipeline: pipeline.NewPipeline()}
}

func MockPCollection[T any](t *Tester, items []T) *pcollection.PCollection[T] {
	return pcollection.NewPCollection(t.Pipeline, items)
}

func MockPTable[K comparable, V any](t *Tester, items []pcollection.KV[K, V]) *pcollection.PTable[K, V] {
	return pcollection.NewPTable(t.Pipeline, items)
}
