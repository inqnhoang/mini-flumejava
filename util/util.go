package util

import "unsafe"

func TypeSize[T any]() uintptr {
	var zero T
	return unsafe.Sizeof(zero)
}
