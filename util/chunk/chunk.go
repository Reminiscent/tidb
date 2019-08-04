// Copyright 2017 PingCAP, Inc.
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
	"encoding/binary"
	"github.com/pingcap/tidb/util/vector"
	"reflect"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

var msgErrSelNotNil = "The selection vector of Chunk is not nil. Please file a bug to the TiDB Team"

// Chunk stores multiple rows of data in Apache Arrow format.
// See https://arrow.apache.org/docs/memory_layout.html
// Values are appended in compact format and can be directly accessed without decoding.
// When the chunk is done processing, we can reuse the allocated memory by resetting it.
type Chunk struct {
	// sel indicates which rows are selected.
	// If it is nil, all rows are selected.
	sel []int

	Columns []*Column
	// numVirtualRows indicates the number of virtual rows, which have zero Column.
	// It is used only when this Chunk doesn't hold any data, i.e. "len(columns)==0".
	numVirtualRows int
	// capacity indicates the max number of rows this chunk can hold.
	// TODO: replace all usages of capacity to requiredRows and remove this field
	capacity int

	// requiredRows indicates how many rows the parent executor want.
	requiredRows int
}

// Capacity constants.
const (
	InitialCapacity = 32
	ZeroCapacity    = 0
)

// NewChunkWithCapacity creates a new chunk with field types and capacity.
func NewChunkWithCapacity(fields []*types.FieldType, cap int) *Chunk {
	return New(fields, cap, cap) //FIXME: in following PR.
}

// New creates a new chunk.
//  cap: the limit for the max number of rows.
//  maxChunkSize: the max limit for the number of rows.
func New(fields []*types.FieldType, cap, maxChunkSize int) *Chunk {
	chk := &Chunk{
		Columns:  make([]*Column, 0, len(fields)),
		capacity: mathutil.Min(cap, maxChunkSize),
		// set the default value of requiredRows to maxChunkSize to let chk.IsFull() behave
		// like how we judge whether a chunk is full now, then the statement
		// "chk.NumRows() < maxChunkSize"
		// equals to "!chk.IsFull()".
		requiredRows: maxChunkSize,
	}

	for _, f := range fields {
		chk.Columns = append(chk.Columns, NewColumn(f, chk.capacity))
	}

	return chk
}

// Renew creates a new Chunk based on an existing Chunk. The newly created Chunk
// has the same data schema with the old Chunk. The capacity of the new Chunk
// might be doubled based on the capacity of the old Chunk and the maxChunkSize.
//  chk: old chunk(often used in previous call).
//  maxChunkSize: the limit for the max number of rows.
func Renew(chk *Chunk, maxChunkSize int) *Chunk {
	newChk := new(Chunk)
	if chk.Columns == nil {
		return newChk
	}
	newCap := reCalcCapacity(chk, maxChunkSize)
	newChk.Columns = renewColumns(chk.Columns, newCap)
	newChk.numVirtualRows = 0
	newChk.capacity = newCap
	newChk.requiredRows = maxChunkSize
	return newChk
}

// renewColumns creates the columns of a Chunk. The capacity of the newly
// created columns is equal to cap.
func renewColumns(oldCol []*Column, cap int) []*Column {
	columns := make([]*Column, 0, len(oldCol))
	for _, col := range oldCol {
		if col.isFixed() {
			columns = append(columns, newFixedLenColumn(len(col.ElemBuf), cap))
		} else {
			columns = append(columns, newVarLenColumn(cap, col))
		}
	}
	return columns
}

// MemoryUsage returns the total memory usage of a Chunk in B.
// We ignore the size of Column.length and Column.nullCount
// since they have little effect of the total memory usage.
func (c *Chunk) MemoryUsage() (sum int64) {
	for _, col := range c.Columns {
		curColMemUsage := int64(unsafe.Sizeof(*col)) + int64(cap(col.NullBitmap)) + int64(cap(col.offsets)*4) + int64(cap(col.Data)) + int64(cap(col.ElemBuf))
		sum += curColMemUsage
	}
	return
}

// newFixedLenColumn creates a fixed length Column with elemLen and initial data capacity.
func newFixedLenColumn(elemLen, cap int) *Column {
	return &Column{
		ElemBuf:    make([]byte, elemLen),
		Data:       make([]byte, 0, cap*elemLen),
		NullBitmap: make([]byte, 0, cap>>3),
	}
}

// newVarLenColumn creates a variable length Column with initial data capacity.
func newVarLenColumn(cap int, old *Column) *Column {
	estimatedElemLen := 8
	// For varLenColumn (e.g. varchar), the accurate length of an element is unknown.
	// Therefore, in the first executor.Next we use an experience value -- 8 (so it may make runtime.growslice)
	// but in the following Next call we estimate the length as AVG x 1.125 elemLen of the previous call.
	if old != nil && old.Length != 0 {
		estimatedElemLen = (len(old.Data) + len(old.Data)/8) / old.Length
	}
	return &Column{
		offsets:    make([]int64, 1, cap+1),
		Data:       make([]byte, 0, cap*estimatedElemLen),
		NullBitmap: make([]byte, 0, cap>>3),
	}
}

// RequiredRows returns how many rows is considered full.
func (c *Chunk) RequiredRows() int {
	return c.requiredRows
}

// SetRequiredRows sets the number of required rows.
func (c *Chunk) SetRequiredRows(requiredRows, maxChunkSize int) *Chunk {
	if requiredRows <= 0 || requiredRows > maxChunkSize {
		requiredRows = maxChunkSize
	}
	c.requiredRows = requiredRows
	return c
}

// IsFull returns if this chunk is considered full.
func (c *Chunk) IsFull() bool {
	return c.NumRows() >= c.requiredRows
}

// MakeRef makes Column in "dstColIdx" reference to Column in "srcColIdx".
func (c *Chunk) MakeRef(srcColIdx, dstColIdx int) {
	c.Columns[dstColIdx] = c.Columns[srcColIdx]
}

// MakeRefTo copies columns `src.columns[srcColIdx]` to `c.columns[dstColIdx]`.
func (c *Chunk) MakeRefTo(dstColIdx int, src *Chunk, srcColIdx int) error {
	if c.sel != nil || src.sel != nil {
		return errors.New(msgErrSelNotNil)
	}
	c.Columns[dstColIdx] = src.Columns[srcColIdx]
	return nil
}

// SwapColumn swaps Column "c.columns[colIdx]" with Column
// "other.columns[otherIdx]". If there exists columns refer to the Column to be
// swapped, we need to re-build the reference.
func (c *Chunk) SwapColumn(colIdx int, other *Chunk, otherIdx int) error {
	if c.sel != nil || other.sel != nil {
		return errors.New(msgErrSelNotNil)
	}
	// Find the leftmost Column of the reference which is the actual Column to
	// be swapped.
	for i := 0; i < colIdx; i++ {
		if c.Columns[i] == c.Columns[colIdx] {
			colIdx = i
		}
	}
	for i := 0; i < otherIdx; i++ {
		if other.Columns[i] == other.Columns[otherIdx] {
			otherIdx = i
		}
	}

	// Find the columns which refer to the actual Column to be swapped.
	refColsIdx := make([]int, 0, len(c.Columns)-colIdx)
	for i := colIdx; i < len(c.Columns); i++ {
		if c.Columns[i] == c.Columns[colIdx] {
			refColsIdx = append(refColsIdx, i)
		}
	}
	refColsIdx4Other := make([]int, 0, len(other.Columns)-otherIdx)
	for i := otherIdx; i < len(other.Columns); i++ {
		if other.Columns[i] == other.Columns[otherIdx] {
			refColsIdx4Other = append(refColsIdx4Other, i)
		}
	}

	// Swap columns from two chunks.
	c.Columns[colIdx], other.Columns[otherIdx] = other.Columns[otherIdx], c.Columns[colIdx]

	// Rebuild the reference.
	for _, i := range refColsIdx {
		c.MakeRef(colIdx, i)
	}
	for _, i := range refColsIdx4Other {
		other.MakeRef(otherIdx, i)
	}
	return nil
}

// SwapColumns swaps columns with another Chunk.
func (c *Chunk) SwapColumns(other *Chunk) {
	c.sel, other.sel = other.sel, c.sel
	c.Columns, other.Columns = other.Columns, c.Columns
	c.numVirtualRows, other.numVirtualRows = other.numVirtualRows, c.numVirtualRows
}

// SetNumVirtualRows sets the virtual row number for a Chunk.
// It should only be used when there exists no Column in the Chunk.
func (c *Chunk) SetNumVirtualRows(numVirtualRows int) {
	c.numVirtualRows = numVirtualRows
}

// Reset resets the chunk, so the memory it allocated can be reused.
// Make sure all the data in the chunk is not used anymore before you reuse this chunk.
func (c *Chunk) Reset() {
	c.sel = nil
	if c.Columns == nil {
		return
	}
	for _, col := range c.Columns {
		col.Reset()
	}
	c.numVirtualRows = 0
}

// CopyConstruct creates a new chunk and copies this chunk's data into it.
func (c *Chunk) CopyConstruct() *Chunk {
	newChk := &Chunk{numVirtualRows: c.numVirtualRows, capacity: c.capacity, Columns: make([]*Column, len(c.Columns))}
	for i := range c.Columns {
		newChk.Columns[i] = c.Columns[i].copyConstruct()
	}
	if c.sel != nil {
		newChk.sel = make([]int, len(c.sel))
		copy(newChk.sel, c.sel)
	}
	return newChk
}

// GrowAndReset resets the Chunk and doubles the capacity of the Chunk.
// The doubled capacity should not be larger than maxChunkSize.
// TODO: this method will be used in following PR.
func (c *Chunk) GrowAndReset(maxChunkSize int) {
	c.sel = nil
	if c.Columns == nil {
		return
	}
	newCap := reCalcCapacity(c, maxChunkSize)
	if newCap <= c.capacity {
		c.Reset()
		return
	}
	c.capacity = newCap
	c.Columns = renewColumns(c.Columns, newCap)
	c.numVirtualRows = 0
	c.requiredRows = maxChunkSize
}

// reCalcCapacity calculates the capacity for another Chunk based on the current
// Chunk. The new capacity is doubled only when the current Chunk is full.
func reCalcCapacity(c *Chunk, maxChunkSize int) int {
	if c.NumRows() < c.capacity {
		return c.capacity
	}
	return mathutil.Min(c.capacity*2, maxChunkSize)
}

// Capacity returns the capacity of the Chunk.
func (c *Chunk) Capacity() int {
	return c.capacity
}

// NumCols returns the number of columns in the chunk.
func (c *Chunk) NumCols() int {
	return len(c.Columns)
}

// NumRows returns the number of rows in the chunk.
func (c *Chunk) NumRows() int {
	if c.sel != nil {
		return len(c.sel)
	}
	if c.NumCols() == 0 {
		return c.numVirtualRows
	}
	return c.Columns[0].Length
}

// GetRow gets the Row in the chunk with the row index.
func (c *Chunk) GetRow(idx int) Row {
	if c.sel != nil {
		// mapping the logical RowIdx to the actual physical RowIdx;
		// for example, if the Sel is [1, 5, 6], then
		//	logical 0 -> physical 1,
		//	logical 1 -> physical 5,
		//	logical 2 -> physical 6.
		// Then when we iterate this Chunk according to Row, only selected rows will be
		// accessed while all filtered rows will be ignored.
		return Row{c: c, idx: int(c.sel[idx])}
	}
	return Row{c: c, idx: idx}
}

// AppendRow appends a row to the chunk.
func (c *Chunk) AppendRow(row Row) {
	c.AppendPartialRow(0, row)
	c.numVirtualRows++
}

// AppendPartialRow appends a row to the chunk.
func (c *Chunk) AppendPartialRow(colIdx int, row Row) {
	c.AppendSel(colIdx)
	for i, rowCol := range row.c.Columns {
		chkCol := c.Columns[colIdx+i]
		chkCol.AppendNullBitmap(!rowCol.IsNull(row.idx))
		if rowCol.isFixed() {
			elemLen := len(rowCol.ElemBuf)
			offset := row.idx * elemLen
			chkCol.Data = append(chkCol.Data, rowCol.Data[offset:offset+elemLen]...)
		} else {
			start, end := rowCol.offsets[row.idx], rowCol.offsets[row.idx+1]
			chkCol.Data = append(chkCol.Data, rowCol.Data[start:end]...)
			chkCol.offsets = append(chkCol.offsets, int64(len(chkCol.Data)))
		}
		chkCol.Length++
	}
}

// preAlloc pre-allocates the memory space in a Chunk to store the Row.
// NOTE: only used in test.
// 1. The Chunk must be empty or holds no useful data.
// 2. The schema of the Row must be the same with the Chunk.
// 3. This API is paired with the `Insert()` function, which inserts all the
//    rows data into the Chunk after the pre-allocation.
// 4. We set the null bitmap here instead of in the Insert() function because
//    when the Insert() function is called parallelly, the data race on a byte
//    can not be avoided although the manipulated bits are different inside a
//    byte.
func (c *Chunk) preAlloc(row Row) (rowIdx uint32) {
	rowIdx = uint32(c.NumRows())
	for i, srcCol := range row.c.Columns {
		dstCol := c.Columns[i]
		dstCol.AppendNullBitmap(!srcCol.IsNull(row.idx))
		elemLen := len(srcCol.ElemBuf)
		if !srcCol.isFixed() {
			elemLen = int(srcCol.offsets[row.idx+1] - srcCol.offsets[row.idx])
			dstCol.offsets = append(dstCol.offsets, int64(len(dstCol.Data)+elemLen))
		}
		dstCol.Length++
		needCap := len(dstCol.Data) + elemLen
		if needCap <= cap(dstCol.Data) {
			(*reflect.SliceHeader)(unsafe.Pointer(&dstCol.Data)).Len = len(dstCol.Data) + elemLen
			continue
		}
		// Grow the capacity according to golang.growslice.
		// Implementation differences with golang:
		// 1. We double the capacity when `dstCol.data < 1024*elemLen bytes` but
		// not `1024 bytes`.
		// 2. We expand the capacity to 1.5*originCap rather than 1.25*originCap
		// during the slow-increasing phase.
		newCap := cap(dstCol.Data)
		doubleCap := newCap << 1
		if needCap > doubleCap {
			newCap = needCap
		} else {
			avgElemLen := elemLen
			if !srcCol.isFixed() {
				avgElemLen = len(dstCol.Data) / len(dstCol.offsets)
			}
			// slowIncThreshold indicates the threshold exceeding which the
			// dstCol.data capacity increase fold decreases from 2 to 1.5.
			slowIncThreshold := 1024 * avgElemLen
			if len(dstCol.Data) < slowIncThreshold {
				newCap = doubleCap
			} else {
				for 0 < newCap && newCap < needCap {
					newCap += newCap / 2
				}
				if newCap <= 0 {
					newCap = needCap
				}
			}
		}
		dstCol.Data = make([]byte, len(dstCol.Data)+elemLen, newCap)
	}
	return
}

// insert inserts `row` on the position specified by `rowIdx`.
// NOTE: only used in test.
// Note: Insert will cover the origin data, it should be called after
// PreAlloc.
func (c *Chunk) insert(rowIdx int, row Row) {
	for i, srcCol := range row.c.Columns {
		if row.IsNull(i) {
			continue
		}
		dstCol := c.Columns[i]
		var srcStart, srcEnd, destStart, destEnd int
		if srcCol.isFixed() {
			srcElemLen, destElemLen := len(srcCol.ElemBuf), len(dstCol.ElemBuf)
			srcStart, destStart = row.idx*srcElemLen, rowIdx*destElemLen
			srcEnd, destEnd = srcStart+srcElemLen, destStart+destElemLen
		} else {
			srcStart, srcEnd = int(srcCol.offsets[row.idx]), int(srcCol.offsets[row.idx+1])
			destStart, destEnd = int(dstCol.offsets[rowIdx]), int(dstCol.offsets[rowIdx+1])
		}
		copy(dstCol.Data[destStart:destEnd], srcCol.Data[srcStart:srcEnd])
	}
}

// Append appends rows in [begin, end) in another Chunk to a Chunk.
func (c *Chunk) Append(other *Chunk, begin, end int) {
	for colID, src := range other.Columns {
		dst := c.Columns[colID]
		if src.isFixed() {
			elemLen := len(src.ElemBuf)
			dst.Data = append(dst.Data, src.Data[begin*elemLen:end*elemLen]...)
		} else {
			beginOffset, endOffset := src.offsets[begin], src.offsets[end]
			dst.Data = append(dst.Data, src.Data[beginOffset:endOffset]...)
			for i := begin; i < end; i++ {
				dst.offsets = append(dst.offsets, dst.offsets[len(dst.offsets)-1]+src.offsets[i+1]-src.offsets[i])
			}
		}
		for i := begin; i < end; i++ {
			c.AppendSel(colID)
			dst.AppendNullBitmap(!src.IsNull(i))
			dst.Length++
		}
	}
	c.numVirtualRows += end - begin
}

// TruncateTo truncates rows from tail to head in a Chunk to "numRows" rows.
func (c *Chunk) TruncateTo(numRows int) {
	c.Reconstruct()
	for _, col := range c.Columns {
		if col.isFixed() {
			elemLen := len(col.ElemBuf)
			col.Data = col.Data[:numRows*elemLen]
		} else {
			col.Data = col.Data[:col.offsets[numRows]]
			col.offsets = col.offsets[:numRows+1]
		}
		for i := numRows; i < col.Length; i++ {
			if col.IsNull(i) {
				col.nullCount--
			}
		}
		col.Length = numRows
		bitmapLen := (col.Length + 7) / 8
		col.NullBitmap = col.NullBitmap[:bitmapLen]
		if col.Length%8 != 0 {
			// When we append null, we simply increment the nullCount,
			// so we need to clear the unused bits in the last bitmap byte.
			lastByte := col.NullBitmap[bitmapLen-1]
			unusedBitsLen := 8 - uint(col.Length%8)
			lastByte <<= unusedBitsLen
			lastByte >>= unusedBitsLen
			col.NullBitmap[bitmapLen-1] = lastByte
		}
	}
	c.numVirtualRows = numRows
}

// AppendNull appends a null value to the chunk.
func (c *Chunk) AppendNull(colIdx int) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].AppendNull()
}

// AppendInt64 appends a int64 value to the chunk.
func (c *Chunk) AppendInt64(colIdx int, i int64) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].AppendInt64(i)
}

func (c *Chunk) AppendVectorInt64(colIdx int, vec *vector.VecInt64) {
	c.AppendSel(colIdx)
	length := len(vec.Values)
	for i := 0; i < length; i++ {
		c.Columns[colIdx].AppendInt64(vec.Values[i])
	}
}

// AppendUint64 appends a uint64 value to the chunk.
func (c *Chunk) AppendUint64(colIdx int, u uint64) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].AppendUint64(u)
}

// AppendFloat32 appends a float32 value to the chunk.
func (c *Chunk) AppendFloat32(colIdx int, f float32) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].AppendFloat32(f)
}

// AppendFloat64 appends a float64 value to the chunk.
func (c *Chunk) AppendFloat64(colIdx int, f float64) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].AppendFloat64(f)
}

// AppendString appends a string value to the chunk.
func (c *Chunk) AppendString(colIdx int, str string) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].AppendString(str)
}

// AppendBytes appends a bytes value to the chunk.
func (c *Chunk) AppendBytes(colIdx int, b []byte) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].AppendBytes(b)
}

// AppendTime appends a Time value to the chunk.
// TODO: change the time structure so it can be directly written to memory.
func (c *Chunk) AppendTime(colIdx int, t types.Time) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].AppendTime(t)
}

// AppendDuration appends a Duration value to the chunk.
func (c *Chunk) AppendDuration(colIdx int, dur types.Duration) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].AppendDuration(dur)
}

// AppendMyDecimal appends a MyDecimal value to the chunk.
func (c *Chunk) AppendMyDecimal(colIdx int, dec *types.MyDecimal) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].AppendMyDecimal(dec)
}

// AppendEnum appends an Enum value to the chunk.
func (c *Chunk) AppendEnum(colIdx int, enum types.Enum) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].appendNameValue(enum.Name, enum.Value)
}

// AppendSet appends a Set value to the chunk.
func (c *Chunk) AppendSet(colIdx int, set types.Set) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].appendNameValue(set.Name, set.Value)
}

// AppendJSON appends a JSON value to the chunk.
func (c *Chunk) AppendJSON(colIdx int, j json.BinaryJSON) {
	c.AppendSel(colIdx)
	c.Columns[colIdx].AppendJSON(j)
}

func (c *Chunk) AppendSel(colIdx int) {
	if colIdx == 0 && c.sel != nil { // use column 0 as standard
		c.sel = append(c.sel, c.Columns[0].Length)
	}
}

// AppendDatum appends a datum into the chunk.
func (c *Chunk) AppendDatum(colIdx int, d *types.Datum) {
	switch d.Kind() {
	case types.KindNull:
		c.AppendNull(colIdx)
	case types.KindInt64:
		c.AppendInt64(colIdx, d.GetInt64())
	case types.KindUint64:
		c.AppendUint64(colIdx, d.GetUint64())
	case types.KindFloat32:
		c.AppendFloat32(colIdx, d.GetFloat32())
	case types.KindFloat64:
		c.AppendFloat64(colIdx, d.GetFloat64())
	case types.KindString, types.KindBytes, types.KindBinaryLiteral, types.KindRaw, types.KindMysqlBit:
		c.AppendBytes(colIdx, d.GetBytes())
	case types.KindMysqlDecimal:
		c.AppendMyDecimal(colIdx, d.GetMysqlDecimal())
	case types.KindMysqlDuration:
		c.AppendDuration(colIdx, d.GetMysqlDuration())
	case types.KindMysqlEnum:
		c.AppendEnum(colIdx, d.GetMysqlEnum())
	case types.KindMysqlSet:
		c.AppendSet(colIdx, d.GetMysqlSet())
	case types.KindMysqlTime:
		c.AppendTime(colIdx, d.GetMysqlTime())
	case types.KindMysqlJSON:
		c.AppendJSON(colIdx, d.GetMysqlJSON())
	}
}

// Column returns the specific column.
func (c *Chunk) Column(colIdx int) *Column {
	return c.Columns[colIdx]
}

// Sel returns Sel of this Chunk.
func (c *Chunk) Sel() []int {
	return c.sel
}

// SetSel sets a Sel for this Chunk.
func (c *Chunk) SetSel(sel []int) {
	c.sel = sel
}

// Reconstruct removes all filtered rows in this Chunk.
func (c *Chunk) Reconstruct() {
	if c.sel == nil {
		return
	}
	for _, col := range c.Columns {
		col.reconstruct(c.sel)
	}
	c.numVirtualRows = len(c.sel)
	c.sel = nil
}

func writeTime(buf []byte, t types.Time) {
	binary.BigEndian.PutUint16(buf, uint16(t.Time.Year()))
	buf[2] = uint8(t.Time.Month())
	buf[3] = uint8(t.Time.Day())
	buf[4] = uint8(t.Time.Hour())
	buf[5] = uint8(t.Time.Minute())
	buf[6] = uint8(t.Time.Second())
	binary.BigEndian.PutUint32(buf[8:], uint32(t.Time.Microsecond()))
	buf[12] = t.Type
	buf[13] = uint8(t.Fsp)
}

func readTime(buf []byte) types.Time {
	year := int(binary.BigEndian.Uint16(buf))
	month := int(buf[2])
	day := int(buf[3])
	hour := int(buf[4])
	minute := int(buf[5])
	second := int(buf[6])
	microseconds := int(binary.BigEndian.Uint32(buf[8:]))
	tp := buf[12]
	fsp := int(buf[13])
	return types.Time{
		Time: types.FromDate(year, month, day, hour, minute, second, microseconds),
		Type: tp,
		Fsp:  fsp,
	}
}
