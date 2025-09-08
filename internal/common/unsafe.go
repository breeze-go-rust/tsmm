package common

import "unsafe"

func UnsafeAdd(base unsafe.Pointer, offset uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(base) + offset)
}

func UnsafeIndex(base unsafe.Pointer, offset uintptr, elemsz uintptr, n int) unsafe.Pointer {
	return unsafe.Pointer(uintptr(base) + offset + uintptr(n)*elemsz)
}

func UnsafeByteSlice(base unsafe.Pointer, offset uintptr, i, j int) []byte {
	return (*[pageMaxAllocSize]byte)(UnsafeAdd(base, offset))[i:j:j]
}
