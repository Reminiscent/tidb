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

package expression

// This file contains benchmarks of our expression evaluation.

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

type benchHelper struct {
	ctx   sessionctx.Context
	exprs []Expression

	inputTypes  []*types.FieldType
	outputTypes []*types.FieldType
	inputChunk  *chunk.Chunk
	outputChunk *chunk.Chunk
}

func (h *benchHelper) init() {
	numRows := 4 * 1024

	h.ctx = mock.NewContext()
	h.ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	h.ctx.GetSessionVars().InitChunkSize = 32
	h.ctx.GetSessionVars().MaxChunkSize = numRows

	h.inputTypes = make([]*types.FieldType, 0, 10)
	h.inputTypes = append(h.inputTypes, &types.FieldType{
		Tp:      mysql.TypeLonglong,
		Flen:    mysql.MaxIntWidth,
		Decimal: 0,
		Flag:    mysql.BinaryFlag,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	})
	h.inputTypes = append(h.inputTypes, &types.FieldType{
		Tp:      mysql.TypeDouble,
		Flen:    mysql.MaxRealWidth,
		Decimal: types.UnspecifiedLength,
		Flag:    mysql.BinaryFlag,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	})
	h.inputTypes = append(h.inputTypes, &types.FieldType{
		Tp:      mysql.TypeNewDecimal,
		Flen:    11,
		Decimal: 0,
		Flag:    mysql.BinaryFlag,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	})

	// Use 20 string columns to show the cache performance.
	for i := 0; i < 20; i++ {
		h.inputTypes = append(h.inputTypes, &types.FieldType{
			Tp:      mysql.TypeVarString,
			Flen:    0,
			Decimal: types.UnspecifiedLength,
			Charset: charset.CharsetUTF8,
			Collate: charset.CollationUTF8,
		})
	}

	h.inputChunk = chunk.NewChunkWithCapacity(h.inputTypes, numRows)
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		h.inputChunk.AppendInt64(0, 4)
		h.inputChunk.AppendFloat64(1, 2.019)
		h.inputChunk.AppendMyDecimal(2, types.NewDecFromFloatForTest(5.9101))
		for i := 0; i < 20; i++ {
			h.inputChunk.AppendString(3+i, `abcdefughasfjsaljal1321798273528791!&(*#&@&^%&%^&!)sadfashqwer`)
		}
	}

	cols := make([]*Column, 0, len(h.inputTypes))
	for i := 0; i < len(h.inputTypes); i++ {
		cols = append(cols, &Column{
			ColName: model.NewCIStr(fmt.Sprintf("col_%v", i)),
			RetType: h.inputTypes[i],
			Index:   i,
		})
	}

	h.exprs = make([]Expression, 0, 10)
	if expr, err := NewFunction(h.ctx, ast.Substr, h.inputTypes[3], []Expression{cols[3], cols[2]}...); err != nil {
		panic("create SUBSTR function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.Plus, h.inputTypes[0], []Expression{cols[1], cols[2]}...); err != nil {
		panic("create PLUS function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.GT, h.inputTypes[2], []Expression{cols[11], cols[8]}...); err != nil {
		panic("create GT function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.GT, h.inputTypes[2], []Expression{cols[19], cols[10]}...); err != nil {
		panic("create GT function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.GT, h.inputTypes[2], []Expression{cols[17], cols[4]}...); err != nil {
		panic("create GT function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.GT, h.inputTypes[2], []Expression{cols[18], cols[5]}...); err != nil {
		panic("create GT function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.LE, h.inputTypes[2], []Expression{cols[19], cols[4]}...); err != nil {
		panic("create LE function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.EQ, h.inputTypes[2], []Expression{cols[20], cols[3]}...); err != nil {
		panic("create EQ function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}
	h.exprs = append(h.exprs, cols[2])
	h.exprs = append(h.exprs, cols[2])

	h.outputTypes = make([]*types.FieldType, 0, len(h.exprs))
	for i := 0; i < len(h.exprs); i++ {
		h.outputTypes = append(h.outputTypes, h.exprs[i].GetType())
	}

	h.outputChunk = chunk.NewChunkWithCapacity(h.outputTypes, numRows)
}

/*
func BenchmarkVectorizedExecute(b *testing.B) {
	h := benchHelper{}
	h.init()
	inputIter := chunk.NewIterator4Chunk(h.inputChunk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.outputChunk.Reset()
		if err := VectorizedExecute(h.ctx, h.exprs, inputIter, h.outputChunk); err != nil {
			panic("errors happened during \"VectorizedExecute\"")
		}
	}
}

func BenchmarkScalarFunctionClone(b *testing.B) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
	con1 := One.Clone()
	con2 := Zero.Clone()
	add := NewFunctionInternal(mock.NewContext(), ast.Plus, types.NewFieldType(mysql.TypeLonglong), col, con1)
	sub := NewFunctionInternal(mock.NewContext(), ast.Plus, types.NewFieldType(mysql.TypeLonglong), add, con2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sub.Clone()
	}
	b.ReportAllocs()
}
*/

// test for int column plus int column
func BenchmarkScalarFuncIntPlus(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: mysql.MaxIntWidth},
		Index:   0,
	}
	col1 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: mysql.MaxIntWidth},
		Index:   1,
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcPlus, err := NewFunction(
		ctx,
		ast.Plus,
		&types.FieldType{Tp: mysql.TypeLonglong, Flen: mysql.MaxIntWidth},
		[]Expression{col0, col1}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType, col1.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		inputChunk.AppendInt64(0, int64(i))
		inputChunk.AppendInt64(1, int64(i))
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)
	inputIter := chunk.NewIterator4Chunk(inputChunk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := VectorizedExecute(ctx, []Expression{funcPlus}, inputIter, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkVectorizedScalarFuncIntPlus(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: mysql.MaxIntWidth},
		Index:   0,
	}
	col1 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: mysql.MaxIntWidth},
		Index:   1,
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcPlus, err := NewFunction(
		ctx,
		ast.Plus,
		&types.FieldType{Tp: mysql.TypeLonglong, Flen: mysql.MaxIntWidth},
		[]Expression{col0, col1}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType, col1.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		inputChunk.AppendInt64(0, int64(i))
		inputChunk.AppendInt64(1, int64(i))
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := RealVectorizedExecute(ctx, []Expression{funcPlus}, inputChunk, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

// test for int column plus int constant
func BenchmarkScalarFuncIntColumnPlusIntConstant(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: mysql.MaxIntWidth},
		Index:   0,
	}
	constant0 := &Constant{
		Value:   types.NewDatum(1),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcPlus, err := NewFunction(
		ctx,
		ast.Plus,
		&types.FieldType{Tp: mysql.TypeLonglong, Flen: mysql.MaxIntWidth},
		[]Expression{col0, constant0}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		inputChunk.AppendInt64(0, int64(i))
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)
	inputIter := chunk.NewIterator4Chunk(inputChunk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := VectorizedExecute(ctx, []Expression{funcPlus}, inputIter, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkVectorizedIntColumnPlusIntConstantFunc(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: mysql.MaxIntWidth},
		Index:   0,
	}
	constant0 := &Constant{
		Value:   types.NewDatum(1),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcPlus, err := NewFunction(
		ctx,
		ast.Plus,
		&types.FieldType{Tp: mysql.TypeLonglong, Flen: mysql.MaxIntWidth},
		[]Expression{col0, constant0}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		inputChunk.AppendInt64(0, int64(i))
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := RealVectorizedExecute(ctx, []Expression{funcPlus}, inputChunk, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

// test for real column multiply real column
func BenchmarkScalarFuncRealMul(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeDouble, Flen: mysql.MaxRealWidth},
		Index:   0,
	}
	col1 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeDouble, Flen: mysql.MaxRealWidth},
		Index:   1,
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcMul, err := NewFunction(
		ctx,
		ast.Mul,
		&types.FieldType{Tp: mysql.TypeDouble, Flen: mysql.MaxRealWidth},
		[]Expression{col0, col1}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType, col1.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		inputChunk.AppendFloat64(0, float64(i))
		inputChunk.AppendFloat64(1, float64(1024-i))
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)
	inputIter := chunk.NewIterator4Chunk(inputChunk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := VectorizedExecute(ctx, []Expression{funcMul}, inputIter, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkVectorizedScalarFuncRealMul(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeDouble, Flen: mysql.MaxRealWidth},
		Index:   0,
	}
	col1 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeDouble, Flen: mysql.MaxRealWidth},
		Index:   1,
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcMul, err := NewFunction(
		ctx,
		ast.Mul,
		&types.FieldType{Tp: mysql.TypeDouble, Flen: mysql.MaxRealWidth},
		[]Expression{col0, col1}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType, col1.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		inputChunk.AppendInt64(0, int64(i))
		inputChunk.AppendInt64(1, int64(i))
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := RealVectorizedExecute(ctx, []Expression{funcMul}, inputChunk, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

// test for real column multiply real constant
func BenchmarkRealColumnMulRealConstantFunc(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeDouble, Flen: mysql.MaxRealWidth},
		Index:   0,
	}
	constant0 := &Constant{
		Value:   types.NewFloat64Datum(2.0),
		RetType: types.NewFieldType(mysql.TypeDouble),
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcMul, err := NewFunction(
		ctx,
		ast.Mul,
		&types.FieldType{Tp: mysql.TypeDouble, Flen: mysql.MaxRealWidth},
		[]Expression{col0, constant0}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		inputChunk.AppendFloat64(0, float64(i))
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)
	inputIter := chunk.NewIterator4Chunk(inputChunk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := VectorizedExecute(ctx, []Expression{funcMul}, inputIter, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkVectorizedRealColumnMulRealConstantFunc(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeDouble, Flen: mysql.MaxRealWidth},
		Index:   0,
	}
	constant0 := &Constant{
		Value:   types.NewFloat64Datum(2.0),
		RetType: types.NewFieldType(mysql.TypeDouble),
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcMul, err := NewFunction(
		ctx,
		ast.Mul,
		&types.FieldType{Tp: mysql.TypeDouble, Flen: mysql.MaxRealWidth},
		[]Expression{col0, constant0}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		inputChunk.AppendInt64(0, int64(i))
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := RealVectorizedExecute(ctx, []Expression{funcMul}, inputChunk, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

// greatest function for three decimal columns
func BenchmarkScalarFuncDecimalGreatest(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		Index:   0,
	}
	col1 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		Index:   1,
	}
	col2 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		Index:   2,
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcGreatest, err := NewFunction(
		ctx,
		ast.Greatest,
		&types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		[]Expression{col0, col1, col2}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType, col1.RetType, col2.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		var d0, d1, d2 types.MyDecimal
		inputChunk.AppendMyDecimal(0, d0.FromInt(int64(i)))
		inputChunk.AppendMyDecimal(1, d1.FromInt(int64(i+1)))
		inputChunk.AppendMyDecimal(2, d2.FromInt(int64(i+2)))
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)
	inputIter := chunk.NewIterator4Chunk(inputChunk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := VectorizedExecute(ctx, []Expression{funcGreatest}, inputIter, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkVectorizedScalarFuncDecimalGreatest(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		Index:   0,
	}
	col1 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		Index:   1,
	}
	col2 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		Index:   2,
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcGreatest, err := NewFunction(
		ctx,
		ast.Greatest,
		&types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		[]Expression{col0, col1, col2}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType, col1.RetType, col2.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		var d0, d1, d2 types.MyDecimal
		inputChunk.AppendMyDecimal(0, d0.FromInt(int64(i)))
		inputChunk.AppendMyDecimal(1, d1.FromInt(int64(i+1)))
		inputChunk.AppendMyDecimal(2, d2.FromInt(int64(i+2)))
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := RealVectorizedExecute(ctx, []Expression{funcGreatest}, inputChunk, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

// greatest function for two decimal columns and one constant decimal
func BenchmarkScalarFuncDecimalGreatestForColumnAndConstant(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		Index:   0,
	}
	col1 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		Index:   1,
	}
	var d types.MyDecimal
	constant0 := &Constant{
		Value:   types.NewDecimalDatum(d.FromInt(int64(2))),
		RetType: types.NewFieldType(mysql.TypeNewDecimal),
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcGreatest, err := NewFunction(
		ctx,
		ast.Greatest,
		&types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		[]Expression{col0, col1, constant0}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType, col1.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		var d0, d1 types.MyDecimal
		inputChunk.AppendMyDecimal(0, d0.FromInt(int64(i)))
		inputChunk.AppendMyDecimal(1, d1.FromInt(int64(i+1)))
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)
	inputIter := chunk.NewIterator4Chunk(inputChunk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := VectorizedExecute(ctx, []Expression{funcGreatest}, inputIter, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkVectorizedScalarFuncDecimalGreatestForColumnAndConstant(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		Index:   0,
	}
	col1 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		Index:   1,
	}
	var d types.MyDecimal
	constant0 := &Constant{
		Value:   types.NewDecimalDatum(d.FromInt(int64(2))),
		RetType: types.NewFieldType(mysql.TypeNewDecimal),
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcGreatest, err := NewFunction(
		ctx,
		ast.Greatest,
		&types.FieldType{Tp: mysql.TypeNewDecimal, Flen: mysql.MaxDecimalWidth},
		[]Expression{col0, col1, constant0}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType, col1.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		var d0, d1 types.MyDecimal
		inputChunk.AppendMyDecimal(0, d0.FromInt(int64(i)))
		inputChunk.AppendMyDecimal(1, d1.FromInt(int64(i+1)))
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := RealVectorizedExecute(ctx, []Expression{funcGreatest}, inputChunk, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

// three string columns for string concat function
func BenchmarkScalarFuncStringConcat(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Flen: 0, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		Index:   0,
	}
	col1 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Flen: 0, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		Index:   1,
	}
	col2 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Flen: 0, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		Index:   2,
	}
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcConcat, err := NewFunction(
		ctx,
		ast.Concat,
		&types.FieldType{Tp: mysql.TypeVarString, Flen: 0, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		[]Expression{col0, col1, col2}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType, col1.RetType, col2.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		inputChunk.AppendString(0, "abc")
		inputChunk.AppendString(1, "def")
		inputChunk.AppendString(2, "ghi")
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)
	inputIter := chunk.NewIterator4Chunk(inputChunk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := VectorizedExecute(ctx, []Expression{funcConcat}, inputIter, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkVectorizedScalarFuncStringConcat(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Flen: 0, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		Index:   0,
	}
	col1 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Flen: 0, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		Index:   1,
	}
	col2 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Flen: 0, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		Index:   2,
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcConcat, err := NewFunction(
		ctx,
		ast.Concat,
		&types.FieldType{Tp: mysql.TypeVarString, Flen: 0, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		[]Expression{col0, col1, col2}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType, col1.RetType, col2.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		inputChunk.AppendString(0, "abc")
		inputChunk.AppendString(1, "def")
		inputChunk.AppendString(2, "ghi")
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := RealVectorizedExecute(ctx, []Expression{funcConcat}, inputChunk, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

// two string columns and one string constant for string concat function
func BenchmarkScalarFuncStringConcatForColumnAndConstant(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		Index:   0,
	}
	col1 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		Index:   1,
	}
	constant0 := &Constant{
		Value:   types.NewDatum("@pingcap"),
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
	}
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcConcat, err := NewFunction(
		ctx,
		ast.Concat,
		&types.FieldType{Tp: mysql.TypeVarString, Flen: 0, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		[]Expression{col0, col1, constant0}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType, col1.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		inputChunk.AppendString(0, "abc")
		inputChunk.AppendString(1, "def")
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)
	inputIter := chunk.NewIterator4Chunk(inputChunk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := VectorizedExecute(ctx, []Expression{funcConcat}, inputIter, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkVectorizedScalarFuncStringConcatForColumnAndConstant(b *testing.B) {
	col0 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		Index:   0,
	}
	col1 := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		Index:   1,
	}
	constant0 := &Constant{
		Value:   types.NewDatum("@pingcap"),
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
	}

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 1024

	funcConcat, err := NewFunction(
		ctx,
		ast.Concat,
		&types.FieldType{Tp: mysql.TypeVarString, Flen: 0, Decimal: types.UnspecifiedLength, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8},
		[]Expression{col0, col1, constant0}...,
	)
	if err != nil {
		panic(err)
	}

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType, col1.RetType}, 1024)
	for i := 0; i < 1024; i++ {
		inputChunk.AppendString(0, "abc")
		inputChunk.AppendString(1, "def")
	}

	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{col0.RetType}, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputChunk.Reset()

		err := RealVectorizedExecute(ctx, []Expression{funcConcat}, inputChunk, outputChunk)
		if err != nil {
			panic(err)
		}
	}
}
