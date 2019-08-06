package vector

import (
	"unsafe"
)

// Vector represents a vector of values.
type Vector unsafe.Pointer

// VecInt64 represents a vector of int64 values.
type VecInt64 struct {
	length int
	values []int64
}

// NewVecInt64 creates a VecInt64.
func NewVecInt64(numVals int) *VecInt64 {
	return &VecInt64{
		length: numVals,
		values: make([]int64, numVals),
	}
}

func (b *VecInt64) GetLength() int {
	return b.length
}

func (b *VecInt64) GetValue(idx int) int64 {
	return b.values[idx]
}

func (b *VecInt64) GetValues() []int64 {
	return b.values
}

func (b *VecInt64) SetValue(idx int, value int64) {
	b.values[idx] = value
}
