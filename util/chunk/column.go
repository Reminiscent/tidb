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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/util/hack"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

// Column stores data in Apache Arrow format.
type Column struct {
	length     int
	nullCount  int
	nullBitmap []byte
	offsets    []int64
	data       []byte
	elemBuf    []byte
}

// NewColumn creates a new Column with the specific length and capacity.
func NewColumn(ft *types.FieldType, cap int) *Column {
	typeSize := getFixedLen(ft)
	if typeSize == varElemLen {
		return newVarLenColumn(cap, nil)
	}
	return newFixedLenColumn(typeSize, cap)
}

// GetInt64 get an int64 from the column at specified index
func (c *Column) GetInt64(index int) int64 {
	return *(*int64)(unsafe.Pointer(&c.data[index*8]))
}

// SetInt64 set an int64 in the column at specified index
func (c *Column) SetInt64(index int, x int64) {
	*(*int64)(unsafe.Pointer(&c.data[index*8])) = x
}

// GetUInt64 get an uint64 from the column at specified index
func (c *Column) GetUInt64(index int) uint64 {
	return *(*uint64)(unsafe.Pointer(&c.data[index*8]))
}

// SetUInt64 set an uint64 in the column at specified index
func (c *Column) SetUInt64(index int, x uint64) {
	*(*uint64)(unsafe.Pointer(&c.data[index*8])) = x
}

// GetFloat32 get a float32 from the column at specified index
func (c *Column) GetFloat32(index int) float32 {
	return *(*float32)(unsafe.Pointer(&c.data[index*4]))
}

// SetFloat32 set a float32 in the column at specified index
func (c *Column) SetFloat32(index int, x float32) {
	*(*float32)(unsafe.Pointer(&c.data[index*4])) = x
}

// GetFloat64 get a float64 from the column at specified index
func (c *Column) GetFloat64(index int) float64 {
	return *(*float64)(unsafe.Pointer(&c.data[index*8]))
}

// SetFloat64 set a float64 in the column at specified index
func (c *Column) SetFloat64(index int, x float64) {
	*(*float64)(unsafe.Pointer(&c.data[index*8])) = x
}

// GetMyDecimal get a decimal from the column at specified index
func (c *Column) GetMyDecimal(index int) *types.MyDecimal {
	return (*types.MyDecimal)(unsafe.Pointer(&c.data[index*types.MyDecimalStructSize]))
}

// SetMyDecimal set a decimal in the column at specified index
func (c *Column) SetMyDecimal(index int, x *types.MyDecimal) {
	*(*types.MyDecimal)(unsafe.Pointer(&c.data[index*types.MyDecimalStructSize])) = *x
}

// GetBytes get bytes from the column at specified index
func (c *Column) GetBytes(index int) []byte {
	start, end := c.offsets[index], c.offsets[index+1]
	return c.data[start:end]
}

// GetTime get a time from the column at specified index
func (c *Column) GetTime(index int) types.Time {
	return readTime(c.data[index*16:])
}

// GetDuration get a duration from the column at specified index
func (c *Column) GetDuration(index, fillFsp int) types.Duration {
	return types.Duration{
		Duration: time.Duration(c.GetInt64(index)),
		Fsp:      fillFsp,
	}
}

// GetEnum get an enum from the column at specified index
func (c *Column) GetEnum(index int) types.Enum {
	name, value := c.getNameValue(index)
	return types.Enum{Name: name, Value: value}
}

// GetSet get a set from the column at specified index
func (c *Column) GetSet(index int) types.Set {
	name, value := c.getNameValue(index)
	return types.Set{Name: name, Value: value}
}

func (c *Column) getNameValue(index int) (string, uint64) {
	start, end := c.offsets[index], c.offsets[index+1]
	if start == end {
		return "", 0
	}
	name := string(hack.String(c.data[start+8 : end]))
	value := *(*uint64)(unsafe.Pointer(&c.data[start]))
	return name, value
}

// GetJSON get a json from the column at specified index
func (c *Column) GetJSON(index int) json.BinaryJSON {
	start, end := c.offsets[index], c.offsets[index+1]
	return json.BinaryJSON{
		TypeCode: c.data[start],
		Value:    c.data[start+1 : end],
	}
}

// MergeNullBitMap merge other's nullBitMap into this's (bit-and)
func (c *Column) MergeNullBitMap(other *Column) {
	for i := range c.nullBitmap {
		c.nullBitmap[i] = c.nullBitmap[i] & other.nullBitmap[i]
	}
}

// GetLength get this column's length
func (c *Column) GetLength() int {
	return c.length
}

func (c *Column) isFixed() bool {
	return c.elemBuf != nil
}

// Reset this column to empty
func (c *Column) Reset() {
	c.length = 0
	c.nullCount = 0
	c.nullBitmap = c.nullBitmap[:0]
	if len(c.offsets) > 0 {
		// The first offset is always 0, it makes slicing the data easier, we need to keep it.
		c.offsets = c.offsets[:1]
	}
	c.data = c.data[:0]
}

// IsNull indicate whether item at specified index is null
func (c *Column) IsNull(rowIdx int) bool {
	nullByte := c.nullBitmap[rowIdx/8]
	return nullByte&(1<<(uint(rowIdx)&7)) == 0
}

// SetNull set item at specified index to `isNull`
func (c *Column) SetNull(rowIdx int, isNull bool) {
	byteIdx := rowIdx >> 3
	mask := byte(1 << (uint(rowIdx) & 7))
	originIsNull := (c.nullBitmap[byteIdx] & mask) == 0

	if originIsNull != isNull {
		if isNull {
			c.nullBitmap[byteIdx] &^= mask
			c.nullCount++
		} else {
			c.nullBitmap[byteIdx] |= mask
			c.nullCount--
		}
	}
}

func (c *Column) copyConstruct() *Column {
	newCol := &Column{length: c.length, nullCount: c.nullCount}
	newCol.nullBitmap = append(newCol.nullBitmap, c.nullBitmap...)
	newCol.offsets = append(newCol.offsets, c.offsets...)
	newCol.data = append(newCol.data, c.data...)
	newCol.elemBuf = append(newCol.elemBuf, c.elemBuf...)
	return newCol
}

// CopyFrom deep copy from `that` column
func (c *Column) CopyFrom(that *Column) {
	c.length, c.nullCount = that.length, that.nullCount

	c.nullBitmap = c.nullBitmap[:0]
	c.nullBitmap = append(c.nullBitmap, that.nullBitmap...)

	c.data = c.data[:0]
	c.data = append(c.data, that.data...)

	if c.isFixed() {
		c.elemBuf = c.elemBuf[:0]
		c.elemBuf = append(c.elemBuf, that.elemBuf...)
	} else {
		c.offsets = c.offsets[:0]
		c.offsets = append(c.offsets, that.offsets...)
	}
}

// FillNulls fill this column with `cnt` nulls.
// `width` indicates fixed-width type's width, a varied-width type should use 0.
func (c *Column) FillNulls(cnt, width int) {
	c.nullCount, c.length = cnt, cnt
	c.fillSameNullBits(false, cnt)
	if c.isFixed() {
		c.elemBuf = c.elemBuf[:0]
		for i := 0; i < width; i++ {
			c.elemBuf = append(c.elemBuf, 0)
		}
		for i := 0; i < cnt; i++ {
			c.data = append(c.data, c.elemBuf...)
		}
	} else {
		for i := 0; i < cnt; i++ {
			c.offsets = append(c.offsets, 0)
		}
	}
}

// FillInt64 fill this column with `cnt` `value`s
func (c *Column) FillInt64(value int64, cnt int) {
	*(*int64)(unsafe.Pointer(&c.elemBuf[0])) = value
	c.finishFillFixedValue(cnt)
}

// FillReal fill this column with `cnt` `value`s
func (c *Column) FillReal(value float64, cnt int) {
	*(*float64)(unsafe.Pointer(&c.elemBuf[0])) = value
	c.finishFillFixedValue(cnt)
}

// FillDecimal fill this column with `cnt` `value`s
func (c *Column) FillDecimal(value *types.MyDecimal, cnt int) {
	*(*types.MyDecimal)(unsafe.Pointer(&c.elemBuf[0])) = *value
	c.finishFillFixedValue(cnt)
}

// FillString fill this column with `cnt` `value`s
func (c *Column) FillString(value string, cnt int) {
	length := int64(len(value))
	base := int64(0)
	for i := 0; i < cnt; i++ {
		base += length
		c.data = append(c.data, value...)
		c.offsets = append(c.offsets, base)
	}
	c.finishFillVarValue(cnt)
}

func (c *Column) finishFillFixedValue(cnt int) {
	c.nullCount, c.length = 0, cnt
	c.fillSameNullBits(true, cnt)
	for i := 0; i < cnt; i++ {
		c.data = append(c.data, c.elemBuf...)
	}
}

func (c *Column) finishFillVarValue(cnt int) {
	c.nullCount, c.length = 0, cnt
	c.fillSameNullBits(true, cnt)
}

func (c *Column) fillSameNullBits(exists bool, cnt int) {
	baseSliceLen, remain := (cnt-1)/8, cnt%8
	if exists {
		for i := 0; i < baseSliceLen; i++ {
			c.nullBitmap = append(c.nullBitmap, byte(255))
		}
		last := byte(255) >> uint(8-remain)
		c.nullBitmap = append(c.nullBitmap, last)
	} else {
		for i := 0; i <= baseSliceLen; i++ {
			c.nullBitmap = append(c.nullBitmap, 0)
		}
	}
}

// GetDatum get a datum at specified index and type
func (c *Column) GetDatum(rowIdx int, tp *types.FieldType) types.Datum {
	var d types.Datum
	switch tp.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if !c.IsNull(rowIdx) {
			if mysql.HasUnsignedFlag(tp.Flag) {
				d.SetUint64(c.GetUInt64(rowIdx))
			} else {
				d.SetInt64(c.GetInt64(rowIdx))
			}
		}
	case mysql.TypeYear:
		// FIXBUG: because insert type of TypeYear is definite int64, so we regardless of the unsigned flag.
		if !c.IsNull(rowIdx) {
			d.SetInt64(c.GetInt64(rowIdx))
		}
	case mysql.TypeFloat:
		if !c.IsNull(rowIdx) {
			d.SetFloat32(c.GetFloat32(rowIdx))
		}
	case mysql.TypeDouble:
		if !c.IsNull(rowIdx) {
			d.SetFloat64(c.GetFloat64(rowIdx))
		}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		if !c.IsNull(rowIdx) {
			d.SetBytes(c.GetBytes(rowIdx))
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		if !c.IsNull(rowIdx) {
			d.SetMysqlTime(c.GetTime(rowIdx))
		}
	case mysql.TypeDuration:
		if !c.IsNull(rowIdx) {
			duration := c.GetDuration(rowIdx, tp.Decimal)
			d.SetMysqlDuration(duration)
		}
	case mysql.TypeNewDecimal:
		if !c.IsNull(rowIdx) {
			d.SetMysqlDecimal(c.GetMyDecimal(rowIdx))
			d.SetLength(tp.Flen)
			// If tp.Decimal is unspecified(-1), we should set it to the real
			// fraction length of the decimal value, if not, the d.Frac will
			// be set to MAX_UINT16 which will cause unexpected BadNumber error
			// when encoding.
			if tp.Decimal == types.UnspecifiedLength {
				d.SetFrac(d.Frac())
			} else {
				d.SetFrac(tp.Decimal)
			}
		}
	case mysql.TypeEnum:
		if !c.IsNull(rowIdx) {
			d.SetMysqlEnum(c.GetEnum(rowIdx))
		}
	case mysql.TypeSet:
		if !c.IsNull(rowIdx) {
			d.SetMysqlSet(c.GetSet(rowIdx))
		}
	case mysql.TypeBit:
		if !c.IsNull(rowIdx) {
			d.SetMysqlBit(c.GetBytes(rowIdx))
		}
	case mysql.TypeJSON:
		if !c.IsNull(rowIdx) {
			d.SetMysqlJSON(c.GetJSON(rowIdx))
		}
	}
	return d
}

func (c *Column) appendDuration(dur types.Duration) {
	c.AppendInt64(int64(dur.Duration))
}

// AppendMyDecimal appends a MyDecimal at the end of this column
func (c *Column) AppendMyDecimal(dec *types.MyDecimal) {
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

// AppendNull append a null at the end of this column
func (c *Column) AppendNull() {
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

// AppendInt64 append an int64 at the end of this column
func (c *Column) AppendInt64(i int64) {
	*(*int64)(unsafe.Pointer(&c.elemBuf[0])) = i
	c.finishAppendFixed()
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

// AppendFloat64 append a float64 at the end of this column
func (c *Column) AppendFloat64(f float64) {
	*(*float64)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *Column) finishAppendVar() {
	c.appendNullBitmap(true)
	c.offsets = append(c.offsets, int64(len(c.data)))
	c.length++
}

// AppendString append a string at the end of this column
func (c *Column) AppendString(str string) {
	c.data = append(c.data, str...)
	c.finishAppendVar()
}

// AppendBytes append bytes at the end of this column
func (c *Column) AppendBytes(b []byte) {
	c.data = append(c.data, b...)
	c.finishAppendVar()
}

func (c *Column) appendTime(t types.Time) {
	writeTime(c.elemBuf, t)
	c.finishAppendFixed()
}
