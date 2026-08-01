package pcollection

func mapSlice[T, R any](items []T, fn func(T) R) []R {
	out := make([]R, len(items))
	for i, v := range items {
		out[i] = fn(v)
	}
	return out
}
