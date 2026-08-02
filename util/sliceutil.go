package util

func MapSlice[T, R any](items []T, fn func(T) R) []R {
	out := make([]R, len(items))
	for i, v := range items {
		out[i] = fn(v)
	}
	return out
}

func Front[T any](arr []T) T {
	if len(arr) == 0 {
		panic("Front: slice is empty")
	}
	return arr[0]
}

func Pop[T any](arr []T) (T, []T) {
	if len(arr) == 0 {
		panic("Pop: slice is empty")
	}

	return arr[0], arr[1:]
}

func Push[T any](e T, arr []T) []T {
	return append([]T{e}, arr...)
}

func PushBack[T any](arr []T, e T) []T {
	return append(arr, e)
}
