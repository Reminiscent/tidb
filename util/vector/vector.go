// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"unsafe"

	"github.com/pingcap/tidb/types"
)

// Vector represents a vector of values.
type Vector unsafe.Pointer

// VecInt64 represents a vector of int64 values.
type VecInt64 struct {
	// NullBitmap
	Values []int64
}

// NewVecInt64 creates a VecInt64.
func NewVecInt64(numVals int) *VecInt64 {
	return &VecInt64{
		// NullBitmap: NewNullBitmap(numVals),
		Values: make([]int64, numVals),
	}
}

// VecUInt64 represents a vector of uint64 values.
type VecUInt64 struct {
	// NullBitmap
	Values []uint64
}

// NewVecUInt64 creates a VecUInt64.
func NewVecUInt64(numVals int) *VecUInt64 {
	return &VecUInt64{
		// NullBitmap: NewNullBitmap(numVals),
		Values: make([]uint64, numVals),
	}
}

// VecFloat64 represents a vector of float64 values.
type VecFloat64 struct {
	// NullBitmap
	Values []float64
}

// NewVecFloat64 creates a VecFloat64.
func NewVecFloat64(numVals int) *VecFloat64 {
	return &VecFloat64{
		// NullBitmap: NewNullBitmap(numVals),
		Values: make([]float64, numVals),
	}
}

// VecFloat32 represents a vector of float32 values.
type VecFloat32 struct {
	// NullBitmap
	Values []float32
}

// NewVecFloat32 creates a VecFloat32.
func NewVecFloat32(numVals int) *VecFloat32 {
	return &VecFloat32{
		// NullBitmap: NewNullBitmap(numVals),
		Values: make([]float32, numVals),
	}
}

// VecDecimal represents a vector of MyDecimal values.
type VecDecimal struct {
	// NullBitmap
	Values []types.MyDecimal
}

// NewVecDecimal creates a VecDecimal.
func NewVecDecimal(numVals int) *VecDecimal {
	return &VecDecimal{
		// NullBitmap: NewNullBitmap(numVals),
		Values: make([]types.MyDecimal, numVals),
	}
}

// VecDuration represents a vector of Duration values.
type VecDuration struct {
	// NullBitmap
	Values []types.Duration
}

// NewVecDuration creates a VecDuration.
func NewVecDuration(numVals int) *VecDuration {
	return &VecDuration{
		// NullBitmap: NewNullBitmap(numVals),
		Values: make([]types.Duration, numVals),
	}
}

// VecDatetime represents a vector of Datetime values.
type VecDatetime struct {
	// NullBitmap
	Values []types.Time
}

// NewVecDatetime creates a VecDatetime.
func NewVecDatetime(numVals int) *VecDatetime {
	return &VecDatetime{
		// NullBitmap: NewNullBitmap(numVals),
		Values: make([]types.Time, numVals),
	}
}

/*
// NullBitmap represents the NULL bitmap.
// 1: NULL
// 0: NOT NULL
type NullBitmap []byte

// NewNullBitmap creates a NULL bitmap.
func NewNullBitmap(numVals int) NullBitmap {
	return make([]byte, (numVals+7)/8)
}

// IsNull returns whether the i-th value is NULL.
func (b NullBitmap) IsNull(i int) bool {
	return b[i/8]&(1<<(uint(i)&7)) == 0
}

// SetNull sets the NULL flag for the i-th value.
func (b NullBitmap) SetNull(i int, isNull bool) {
	if isNull {
		b[i/8] |= (1 << (uint(i) & 7))
	} else {
		b[i/8] &= ^(1 << (uint(i) & 7))
	}
}

// Clear clears all the NULL flags.
func (b NullBitmap) Clear() {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
}

// Copy copies from another bitmap.
func (b NullBitmap) Copy(other NullBitmap) {
	for i := 0; i < len(b); i++ {
		b[i] = other[i]
	}
}

// Union unions with another bitmap.
func (b NullBitmap) Union(other NullBitmap) {
	for i := 0; i < len(b); i++ {
		b[i] = byte(uint8(b[i]) | uint8(other[i]))
	}
}
*/
