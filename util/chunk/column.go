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
	"github.com/pingcap/tidb/util/vector"
	"unsafe"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func (c *Column) appendDuration(dur types.Duration) {
	c.appendInt64(int64(dur.Duration))
}

func (c *Column) appendMyDecimal(dec *types.MyDecimal) {
	*(*types.MyDecimal)(unsafe.Pointer(&c.elemBuf[0])) = *dec
	c.finishAppendFixed()
}

func (c *Column) appendNameValue(name string, val uint64) {
	var buf [8]byte
	*(*uint64)(unsafe.Pointer(&buf[0])) = val
	c.data = append(c.data, buf[:]...)
	c.data = append(c.data, name...)
	c.finishAppendVar()
}

func (c *Column) appendJSON(j json.BinaryJSON) {
	c.data = append(c.data, j.TypeCode)
	c.data = append(c.data, j.Value...)
	c.finishAppendVar()
}

type Column struct {
	length     int
	nullCount  int
	nullBitmap []byte
	offsets    []int64
	data       []byte
	elemBuf    []byte
}

func (c *Column) GetInt64(index int) int64 {
	return *(*int64)(unsafe.Pointer(&c.data[index*8]))
}

func (c *Column) SetInt64(index int, x int64) {
	*(*int64)(unsafe.Pointer(&c.data[index*8])) = x
}

// NewColumn creates a new Column with the specific length and capacity.
func NewColumn(ft *types.FieldType, cap int) *Column {
	typeSize := getFixedLen(ft)
	if typeSize == varElemLen {
		return newVarLenColumn(cap, nil)
	}
	return newFixedLenColumn(typeSize, cap)
}

func (c *Column) SetInt64Values(val int64) {
	c.reset()

	cnt := cap(c.data) / 8
	*(*int64)(unsafe.Pointer(&c.elemBuf[0])) = val
	for i := 0; i < cnt; i++ {
		c.data = append(c.data, c.elemBuf...)
	}
	for i := 0; i < cnt/8; i++ {
		c.nullBitmap = append(c.nullBitmap, byte(255))
	}
	c.nullBitmap = append(c.nullBitmap, byte(255)>>uint(8-cnt%8))
	c.length = cnt
}

func (c1 *Column) MergeNullBitMap(c2 *Column) {
	for i := range c1.nullBitmap {
		c1.nullBitmap[i] = c1.nullBitmap[i] & c2.nullBitmap[i]
	}
}

func (c *Column) GetLength() int {
	return c.length
}

func (c *Column) isFixed() bool {
	return c.elemBuf != nil
}

func (c *Column) reset() {
	c.length = 0
	c.nullCount = 0
	c.nullBitmap = c.nullBitmap[:0]
	if len(c.offsets) > 0 {
		// The first offset is always 0, it makes slicing the data easier, we need to keep it.
		c.offsets = c.offsets[:1]
	}
	c.data = c.data[:0]
}

func (c *Column) IsNull(rowIdx int) bool {
	nullByte := c.nullBitmap[rowIdx/8]
	return nullByte&(1<<(uint(rowIdx)&7)) == 0
}

func (c *Column) CopyConstruct() *Column {
	newCol := &Column{length: c.length, nullCount: c.nullCount}
	newCol.nullBitmap = append(newCol.nullBitmap, c.nullBitmap...)
	newCol.offsets = append(newCol.offsets, c.offsets...)
	newCol.data = append(newCol.data, c.data...)
	newCol.elemBuf = append(newCol.elemBuf, c.elemBuf...)
	return newCol
}

func (c *Column) CopyFrom(that *Column) {
	c.length, c.nullCount = that.length, that.nullCount

	c.nullBitmap = c.nullBitmap[:0]
	c.nullBitmap = append(c.nullBitmap, that.nullBitmap...)

	c.data = c.data[:0]
	c.data = append(c.data, that.data...)

	c.offsets = c.offsets[:0]
	c.offsets = append(c.offsets, that.offsets...)

	c.elemBuf = c.elemBuf[:0]
	c.elemBuf = append(c.elemBuf, that.elemBuf...)
}

func (c *Column) FillNulls(width int) {
	c.elemBuf = c.elemBuf[:0]
	for i := 0; i < width; i++ {
		c.elemBuf = append(c.elemBuf, 0)
	}

	cnt := cap(c.data) / width
	c.data = c.data[:0]
	for i := 0; i < cnt; i++ {
		c.data = append(c.data, c.elemBuf...)
	}

	c.nullBitmap = c.nullBitmap[:0]
	c.appendMultiSameNullBitmap(false, cnt)
}

// SetVectorInt copy the value from the Column.data to vector.VecInt64
func (c *Column) SetVectorInt(length int, vec vector.Vector) {
	res := (*vector.VecInt64)(vec)
	for i := 0; i < length; i++ {
		value := *(*int64)(unsafe.Pointer(&c.data[i*8]))
		res.SetValue(i, value)
	}
}

func (c *Column) appendNullBitmap(notNull bool) {
	idx := c.length >> 3
	if idx >= len(c.nullBitmap) {
		c.nullBitmap = append(c.nullBitmap, 0)
	}
	if notNull {
		pos := uint(c.length) & 7
		c.nullBitmap[idx] |= byte(1 << pos)
	} else {
		c.nullCount++
	}
}

// appendMultiSameNullBitmap appends multiple same bit value to `nullBitMap`.
// notNull means not null.
// num means the number of bits that should be appended.
func (c *Column) appendMultiSameNullBitmap(notNull bool, num int) {
	numNewBytes := ((c.length + num + 7) >> 3) - len(c.nullBitmap)
	b := byte(0)
	if notNull {
		b = 0xff
	}
	for i := 0; i < numNewBytes; i++ {
		c.nullBitmap = append(c.nullBitmap, b)
	}
	if !notNull {
		c.nullCount += num
		return
	}
	// 1. Set all the remaining bits in the last slot of old c.numBitMap to 1.
	numRemainingBits := uint(c.length % 8)
	bitMask := byte(^((1 << numRemainingBits) - 1))
	c.nullBitmap[c.length/8] |= bitMask
	// 2. Set all the redundant bits in the last slot of new c.numBitMap to 0.
	numRedundantBits := uint(len(c.nullBitmap)*8 - c.length - num)
	bitMask = byte(1<<(8-numRedundantBits)) - 1
	c.nullBitmap[len(c.nullBitmap)-1] &= bitMask
}

func (c *Column) appendNull() {
	c.appendNullBitmap(false)
	if c.isFixed() {
		c.data = append(c.data, c.elemBuf...)
	} else {
		c.offsets = append(c.offsets, c.offsets[c.length])
	}
	c.length++
}

func (c *Column) finishAppendFixed() {
	c.data = append(c.data, c.elemBuf...)
	c.appendNullBitmap(true)
	c.length++
}

func (c *Column) appendInt64(i int64) {
	*(*int64)(unsafe.Pointer(&c.elemBuf[0])) = i
	c.finishAppendFixed()
}

// AppendVectorInt64 appends an vector of int64 value into this Column.
func (c *Column) AppendVectorInt64(vec vector.Vector) {
	res := (*vector.VecInt64)(vec)
	length := res.GetLength()

	for i := 0; i < length; i++ {
		j := res.GetValue(i)
		*(*int64)(unsafe.Pointer(&c.elemBuf[0])) = j
		c.data = append(c.data, c.elemBuf...)
	}
	c.appendMultiSameNullBitmap(true, length)
	c.length += length
}

// AppendUint64 appends a uint64 value into this Column.
func (c *Column) appendUint64(u uint64) {
	*(*uint64)(unsafe.Pointer(&c.elemBuf[0])) = u
	c.finishAppendFixed()
}

func (c *Column) appendFloat32(f float32) {
	*(*float32)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *Column) appendFloat64(f float64) {
	*(*float64)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *Column) finishAppendVar() {
	c.appendNullBitmap(true)
	c.offsets = append(c.offsets, int64(len(c.data)))
	c.length++
}

func (c *Column) appendString(str string) {
	c.data = append(c.data, str...)
	c.finishAppendVar()
}

func (c *Column) appendBytes(b []byte) {
	c.data = append(c.data, b...)
	c.finishAppendVar()
}

func (c *Column) appendTime(t types.Time) {
	writeTime(c.elemBuf, t)
	c.finishAppendFixed()
}
