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

import "github.com/pingcap/errors"

// CopySelectedJoinRows copies the selected joined rows from the source Chunk
// to the destination Chunk.
// Return true if at least one joined row was selected.
//
// NOTE: All the outer rows in the source Chunk should be the same.
func CopySelectedJoinRows(src *Chunk, innerColOffset, outerColOffset int, selected []bool, dst *Chunk) (bool, error) {
	if src.NumRows() == 0 {
		return false, nil
	}
	if src.sel != nil || dst.sel != nil {
		return false, errors.New(msgErrSelNotNil)
	}

	numSelected := copySelectedInnerRows(innerColOffset, outerColOffset, src, selected, dst)
	copyOuterRows(innerColOffset, outerColOffset, src, numSelected, dst)
	dst.numVirtualRows += numSelected
	return numSelected > 0, nil
}

// copySelectedInnerRows copies the selected inner rows from the source Chunk
// to the destination Chunk.
// return the number of rows which is selected.
func copySelectedInnerRows(innerColOffset, outerColOffset int, src *Chunk, selected []bool, dst *Chunk) int {
	oldLen := dst.Columns[innerColOffset].Length
	var srcCols []*Column
	if innerColOffset == 0 {
		srcCols = src.Columns[:outerColOffset]
	} else {
		srcCols = src.Columns[innerColOffset:]
	}
	for j, srcCol := range srcCols {
		dstCol := dst.Columns[innerColOffset+j]
		if srcCol.isFixed() {
			for i := 0; i < len(selected); i++ {
				if !selected[i] {
					continue
				}
				dstCol.AppendNullBitmap(!srcCol.IsNull(i))
				dstCol.Length++

				elemLen := len(srcCol.ElemBuf)
				offset := i * elemLen
				dstCol.Data = append(dstCol.Data, srcCol.Data[offset:offset+elemLen]...)
			}
		} else {
			for i := 0; i < len(selected); i++ {
				if !selected[i] {
					continue
				}
				dstCol.AppendNullBitmap(!srcCol.IsNull(i))
				dstCol.Length++

				start, end := srcCol.offsets[i], srcCol.offsets[i+1]
				dstCol.Data = append(dstCol.Data, srcCol.Data[start:end]...)
				dstCol.offsets = append(dstCol.offsets, int64(len(dstCol.Data)))
			}
		}
	}
	return dst.Columns[innerColOffset].Length - oldLen
}

// copyOuterRows copies the continuous 'numRows' outer rows in the source Chunk
// to the destination Chunk.
func copyOuterRows(innerColOffset, outerColOffset int, src *Chunk, numRows int, dst *Chunk) {
	if numRows <= 0 {
		return
	}
	row := src.GetRow(0)
	var srcCols []*Column
	if innerColOffset == 0 {
		srcCols = src.Columns[outerColOffset:]
	} else {
		srcCols = src.Columns[:innerColOffset]
	}
	for i, srcCol := range srcCols {
		dstCol := dst.Columns[outerColOffset+i]
		dstCol.appendMultiSameNullBitmap(!srcCol.IsNull(row.idx), numRows)
		dstCol.Length += numRows
		if srcCol.isFixed() {
			elemLen := len(srcCol.ElemBuf)
			start := row.idx * elemLen
			end := start + numRows*elemLen
			dstCol.Data = append(dstCol.Data, srcCol.Data[start:end]...)
		} else {
			start, end := srcCol.offsets[row.idx], srcCol.offsets[row.idx+numRows]
			dstCol.Data = append(dstCol.Data, srcCol.Data[start:end]...)
			offsets := dstCol.offsets
			elemLen := srcCol.offsets[row.idx+1] - srcCol.offsets[row.idx]
			for j := 0; j < numRows; j++ {
				offsets = append(offsets, int64(offsets[len(offsets)-1]+elemLen))
			}
			dstCol.offsets = offsets
		}
	}
}
