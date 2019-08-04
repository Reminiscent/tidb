// Copyright 2018 PingCAP, Inc.
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

package chunk

import (
	"reflect"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
)

// AppendDuration appends a duration value into this Column.
func (c *Column) AppendDuration(dur types.Duration) {
	c.AppendInt64(int64(dur.Duration))
}

// AppendMyDecimal appends a MyDecimal value into this Column.
func (c *Column) AppendMyDecimal(dec *types.MyDecimal) {
	*(*types.MyDecimal)(unsafe.Pointer(&c.ElemBuf[0])) = *dec
	c.finishAppendFixed()
}

func (c *Column) appendNameValue(name string, val uint64) {
	var buf [8]byte
	*(*uint64)(unsafe.Pointer(&buf[0])) = val
	c.Data = append(c.Data, buf[:]...)
	c.Data = append(c.Data, name...)
	c.finishAppendVar()
}

// AppendJSON appends a BinaryJSON value into this Column.
func (c *Column) AppendJSON(j json.BinaryJSON) {
	c.Data = append(c.Data, j.TypeCode)
	c.Data = append(c.Data, j.Value...)
	c.finishAppendVar()
}

// AppendSet appends a Set value into this Column.
func (c *Column) AppendSet(set types.Set) {
	c.appendNameValue(set.Name, set.Value)
}

// Column stores one column of data in Apache Arrow format.
// See https://arrow.apache.org/docs/memory_layout.html
type Column struct {
	Length     int
	nullCount  int
	NullBitmap []byte
	offsets    []int64
	Data       []byte
	ElemBuf    []byte
}

// NewColumn creates a new column with the specific length and capacity.
func NewColumn(ft *types.FieldType, cap int) *Column {
	typeSize := getFixedLen(ft)
	if typeSize == varElemLen {
		return newVarLenColumn(cap, nil)
	}
	return newFixedLenColumn(typeSize, cap)
}

func (c *Column) isFixed() bool {
	return c.ElemBuf != nil
}

// Reset resets this Column.
func (c *Column) Reset() {
	c.Length = 0
	c.nullCount = 0
	c.NullBitmap = c.NullBitmap[:0]
	if len(c.offsets) > 0 {
		// The first offset is always 0, it makes slicing the data easier, we need to keep it.
		c.offsets = c.offsets[:1]
	}
	c.Data = c.Data[:0]
}

// IsNull returns if this row is null.
func (c *Column) IsNull(rowIdx int) bool {
	nullByte := c.NullBitmap[rowIdx/8]
	return nullByte&(1<<(uint(rowIdx)&7)) == 0
}

func (c *Column) copyConstruct() *Column {
	newCol := &Column{Length: c.Length, nullCount: c.nullCount}
	newCol.NullBitmap = append(newCol.NullBitmap, c.NullBitmap...)
	newCol.offsets = append(newCol.offsets, c.offsets...)
	newCol.Data = append(newCol.Data, c.Data...)
	newCol.ElemBuf = append(newCol.ElemBuf, c.ElemBuf...)
	return newCol
}

func (c *Column) AppendNullBitmap(notNull bool) {
	idx := c.Length >> 3
	if idx >= len(c.NullBitmap) {
		c.NullBitmap = append(c.NullBitmap, 0)
	}
	if notNull {
		pos := uint(c.Length) & 7
		c.NullBitmap[idx] |= byte(1 << pos)
	} else {
		c.nullCount++
	}
}

// appendMultiSameNullBitmap appends multiple same bit value to `nullBitMap`.
// notNull means not null.
// num means the number of bits that should be appended.
func (c *Column) appendMultiSameNullBitmap(notNull bool, num int) {
	numNewBytes := ((c.Length + num + 7) >> 3) - len(c.NullBitmap)
	b := byte(0)
	if notNull {
		b = 0xff
	}
	for i := 0; i < numNewBytes; i++ {
		c.NullBitmap = append(c.NullBitmap, b)
	}
	if !notNull {
		c.nullCount += num
		return
	}
	// 1. Set all the remaining bits in the last slot of old c.numBitMap to 1.
	numRemainingBits := uint(c.Length % 8)
	bitMask := byte(^((1 << numRemainingBits) - 1))
	c.NullBitmap[c.Length/8] |= bitMask
	// 2. Set all the redundant bits in the last slot of new c.numBitMap to 0.
	numRedundantBits := uint(len(c.NullBitmap)*8 - c.Length - num)
	bitMask = byte(1<<(8-numRedundantBits)) - 1
	c.NullBitmap[len(c.NullBitmap)-1] &= bitMask
}

// AppendNull appends a null value into this Column.
func (c *Column) AppendNull() {
	c.AppendNullBitmap(false)
	if c.isFixed() {
		c.Data = append(c.Data, c.ElemBuf...)
	} else {
		c.offsets = append(c.offsets, c.offsets[c.Length])
	}
	c.Length++
}

func (c *Column) finishAppendFixed() {
	c.Data = append(c.Data, c.ElemBuf...)
	c.AppendNullBitmap(true)
	c.Length++
}

// AppendInt64 appends an int64 value into this Column.
func (c *Column) AppendInt64(i int64) {
	*(*int64)(unsafe.Pointer(&c.ElemBuf[0])) = i
	c.finishAppendFixed()
}

// AppendUint64 appends a uint64 value into this Column.
func (c *Column) AppendUint64(u uint64) {
	*(*uint64)(unsafe.Pointer(&c.ElemBuf[0])) = u
	c.finishAppendFixed()
}

// AppendFloat32 appends a float32 value into this Column.
func (c *Column) AppendFloat32(f float32) {
	*(*float32)(unsafe.Pointer(&c.ElemBuf[0])) = f
	c.finishAppendFixed()
}

// AppendFloat64 appends a float64 value into this Column.
func (c *Column) AppendFloat64(f float64) {
	*(*float64)(unsafe.Pointer(&c.ElemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *Column) finishAppendVar() {
	c.AppendNullBitmap(true)
	c.offsets = append(c.offsets, int64(len(c.Data)))
	c.Length++
}

// AppendString appends a string value into this Column.
func (c *Column) AppendString(str string) {
	c.Data = append(c.Data, str...)
	c.finishAppendVar()
}

// AppendBytes appends a byte slice into this Column.
func (c *Column) AppendBytes(b []byte) {
	c.Data = append(c.Data, b...)
	c.finishAppendVar()
}

// AppendTime appends a time value into this Column.
func (c *Column) AppendTime(t types.Time) {
	writeTime(c.ElemBuf, t)
	c.finishAppendFixed()
}

// AppendEnum appends a Enum value into this Column.
func (c *Column) AppendEnum(enum types.Enum) {
	c.appendNameValue(enum.Name, enum.Value)
}

const (
	sizeInt64     = int(unsafe.Sizeof(int64(0)))
	sizeUint64    = int(unsafe.Sizeof(uint64(0)))
	sizeFloat32   = int(unsafe.Sizeof(float32(0)))
	sizeFloat64   = int(unsafe.Sizeof(float64(0)))
	sizeMyDecimal = int(unsafe.Sizeof(types.MyDecimal{}))
)

func (c *Column) castSliceHeader(header *reflect.SliceHeader, typeSize int) {
	header.Data = uintptr(unsafe.Pointer(&c.Data[0]))
	header.Len = c.Length
	header.Cap = cap(c.Data) / typeSize
}

// Int64s returns an int64 slice stored in this Column.
func (c *Column) Int64s() []int64 {
	var res []int64
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeInt64)
	return res
}

// Uint64s returns a uint64 slice stored in this Column.
func (c *Column) Uint64s() []uint64 {
	var res []uint64
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeUint64)
	return res
}

// Float32s returns a float32 slice stored in this Column.
func (c *Column) Float32s() []float32 {
	var res []float32
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeFloat32)
	return res
}

// Float64s returns a float64 slice stored in this Column.
func (c *Column) Float64s() []float64 {
	var res []float64
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeFloat64)
	return res
}

// Decimals returns a MyDecimal slice stored in this Column.
func (c *Column) Decimals() []types.MyDecimal {
	var res []types.MyDecimal
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeMyDecimal)
	return res
}

// GetInt64 returns the int64 in the specific row.
func (c *Column) GetInt64(rowID int) int64 {
	return *(*int64)(unsafe.Pointer(&c.Data[rowID*8]))
}

// GetUint64 returns the uint64 in the specific row.
func (c *Column) GetUint64(rowID int) uint64 {
	return *(*uint64)(unsafe.Pointer(&c.Data[rowID*8]))
}

// GetFloat32 returns the float32 in the specific row.
func (c *Column) GetFloat32(rowID int) float32 {
	return *(*float32)(unsafe.Pointer(&c.Data[rowID*4]))
}

// GetFloat64 returns the float64 in the specific row.
func (c *Column) GetFloat64(rowID int) float64 {
	return *(*float64)(unsafe.Pointer(&c.Data[rowID*8]))
}

// GetDecimal returns the decimal in the specific row.
func (c *Column) GetDecimal(rowID int) *types.MyDecimal {
	return (*types.MyDecimal)(unsafe.Pointer(&c.Data[rowID*types.MyDecimalStructSize]))
}

// GetString returns the string in the specific row.
func (c *Column) GetString(rowID int) string {
	return string(hack.String(c.Data[c.offsets[rowID]:c.offsets[rowID+1]]))
}

// GetJSON returns the JSON in the specific row.
func (c *Column) GetJSON(rowID int) json.BinaryJSON {
	start := c.offsets[rowID]
	return json.BinaryJSON{TypeCode: c.Data[start], Value: c.Data[start+1 : c.offsets[rowID+1]]}
}

// GetBytes returns the byte slice in the specific row.
func (c *Column) GetBytes(rowID int) []byte {
	return c.Data[c.offsets[rowID]:c.offsets[rowID+1]]
}

// GetEnum returns the Enum in the specific row.
func (c *Column) GetEnum(rowID int) types.Enum {
	name, val := c.getNameValue(rowID)
	return types.Enum{Name: name, Value: val}
}

// GetSet returns the Set in the specific row.
func (c *Column) GetSet(rowID int) types.Set {
	name, val := c.getNameValue(rowID)
	return types.Set{Name: name, Value: val}
}

// GetTime returns the Time in the specific row.
func (c *Column) GetTime(rowID int) types.Time {
	return readTime(c.Data[rowID*16:])
}

// GetDuration returns the Duration in the specific row.
func (c *Column) GetDuration(rowID int, fillFsp int) types.Duration {
	dur := *(*int64)(unsafe.Pointer(&c.Data[rowID*8]))
	return types.Duration{Duration: time.Duration(dur), Fsp: fillFsp}
}

func (c *Column) getNameValue(rowID int) (string, uint64) {
	start, end := c.offsets[rowID], c.offsets[rowID+1]
	if start == end {
		return "", 0
	}
	return string(hack.String(c.Data[start+8 : end])), *(*uint64)(unsafe.Pointer(&c.Data[start]))
}

// reconstruct reconstructs this Column by removing all filtered rows in it according to sel.
func (c *Column) reconstruct(sel []int) {
	if sel == nil {
		return
	}
	nullCnt := 0
	if c.isFixed() {
		elemLen := len(c.ElemBuf)
		for dst, src := range sel {
			idx := dst >> 3
			pos := uint16(dst & 7)
			if c.IsNull(src) {
				nullCnt++
				c.NullBitmap[idx] &= ^byte(1 << pos)
			} else {
				copy(c.Data[dst*elemLen:dst*elemLen+elemLen], c.Data[src*elemLen:src*elemLen+elemLen])
				c.NullBitmap[idx] |= byte(1 << pos)
			}
		}
		c.Data = c.Data[:len(sel)*elemLen]
	} else {
		tail := 0
		for dst, src := range sel {
			idx := dst >> 3
			pos := uint(dst & 7)
			if c.IsNull(src) {
				nullCnt++
				c.NullBitmap[idx] &= ^byte(1 << pos)
				c.offsets[dst+1] = int64(tail)
			} else {
				start, end := c.offsets[src], c.offsets[src+1]
				copy(c.Data[tail:], c.Data[start:end])
				tail += int(end - start)
				c.offsets[dst+1] = int64(tail)
				c.NullBitmap[idx] |= byte(1 << pos)
			}
		}
		c.Data = c.Data[:tail]
		c.offsets = c.offsets[:len(sel)+1]
	}
	c.Length = len(sel)
	c.nullCount = nullCnt

	// clean nullBitmap
	c.NullBitmap = c.NullBitmap[:(len(sel)+7)>>3]
	idx := len(sel) >> 3
	if idx < len(c.NullBitmap) {
		pos := uint16(len(sel) & 7)
		c.NullBitmap[idx] &= byte((1 << pos) - 1)
	}
}
