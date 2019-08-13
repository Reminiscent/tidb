package expression

import (
	"errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"reflect"
	"strconv"
	"testing"
)

func TestColumnEvaluator(t *testing.T) {
	TestT(t)
}

var _ = Suite(&testColumnEvaluatorSuite{
	ctx: mock.NewContext(),
})

type testColumnEvaluatorSuite struct {
	ctx sessionctx.Context
}

var int64Type = types.NewFieldType(mysql.TypeLonglong)
var float64Type = types.NewFieldType(mysql.TypeDouble)
var newDecimalType = types.NewFieldType(mysql.TypeNewDecimal)
var varStringType = &types.FieldType{
	Tp:      mysql.TypeVarString,
	Flen:    0,
	Decimal: types.UnspecifiedLength,
	Charset: charset.CharsetUTF8,
	Collate: charset.CollationUTF8,
}

func (s *testColumnEvaluatorSuite) TestConstantEvalInt64(c *C) {
	s.testConstantColEval(c, int64Type, int64(1234))
}

func (s *testColumnEvaluatorSuite) TestConstantEvalReal(c *C) {
	s.testConstantColEval(c, float64Type, 123.456)
}

func (s *testColumnEvaluatorSuite) TestConstantEvalDecimal(c *C) {
	var d types.MyDecimal
	c.Assert(d.FromFloat64(123.456), IsNil)
	s.testConstantColEval(c, newDecimalType, &d)
}

func (s *testColumnEvaluatorSuite) TestConstantEvalVarString(c *C) {
	s.testConstantColEval(c, varStringType, "Hello World")
}

func (s *testColumnEvaluatorSuite) testConstantColEval(c *C, tp *types.FieldType, value interface{}) {
	testLen := 10

	inChk := chunk.NewChunkWithCapacity(nil, 0)
	inChk.SetNumVirtualRows(testLen)
	outCol := chunk.NewColumn(tp, testLen)

	constant := &Constant{
		Value:   types.NewDatum(value),
		RetType: tp,
	}

	// case 1: values
	err := s.constantColEval(constant, tp, inChk, outCol)

	c.Assert(err, IsNil)
	for i := 0; i < testLen; i++ {
		c.Assert(outCol.IsNull(i), IsFalse)
		testColumnValue(c, outCol, tp, i, value)
	}

	// case 2: nulls
	outCol.Reset()
	constant.Value = types.NewDatum(nil)

	err = s.constantColEval(constant, tp, inChk, outCol)

	c.Assert(err, IsNil)
	for i := 0; i < testLen; i++ {
		c.Assert(outCol.IsNull(i), IsTrue)
	}
}

func (s *testColumnEvaluatorSuite) constantColEval(constant *Constant, tp *types.FieldType, chk *chunk.Chunk, outCol *chunk.Column) error {
	switch tp {
	case int64Type:
		return constant.ColEvalInt(s.ctx, chk, outCol)
	case float64Type:
		return constant.ColEvalInt(s.ctx, chk, outCol)
	case newDecimalType:
		return constant.ColEvalDecimal(s.ctx, chk, outCol)
	case varStringType:
		return constant.ColEvalString(s.ctx, chk, outCol)
	default:
		return errors.New("error type")
	}
}

func testColumnValue(c *C, col *chunk.Column, tp *types.FieldType, index int, expectValue interface{}) {
	switch tp {
	case int64Type:
		c.Assert(col.GetInt64(index), Equals, expectValue.(int64))
	case float64Type:
		c.Assert(col.GetFloat64(index), Equals, expectValue.(float64))
	case newDecimalType:
		c.Assert(col.GetMyDecimal(index), DeepEquals, expectValue.(*types.MyDecimal))
	case varStringType:
		c.Assert(col.GetBytes(index), DeepEquals, []byte(expectValue.(string)))
	default:
		panic("error type")
	}
}

func (s *testColumnEvaluatorSuite) TestColumnEvalInt(c *C) {
	s.testColumnColEval(c, int64Type, int64(1234567890), nil)
}

func (s *testColumnEvaluatorSuite) TestColumnEvalReal(c *C) {
	s.testColumnColEval(c, float64Type, 12345.67890, float64(123456789), nil)
}

func (s *testColumnEvaluatorSuite) TestColumnEvalDecimal(c *C) {
	var d types.MyDecimal
	c.Assert(d.FromFloat64(12345.6789), IsNil)
	s.testColumnColEval(c, newDecimalType, &d, nil)
}

func (s *testColumnEvaluatorSuite) TestColumnEvalString(c *C) {
	s.testColumnColEval(c, varStringType, "Hello World", "", nil)
}

func (s *testColumnEvaluatorSuite) testColumnColEval(c *C, tp *types.FieldType, values ...interface{}) {
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{tp}, len(values))
	inCol := chk.GetColumn(0)
	for _, value := range values {
		columnAppendValue(inCol, tp, value)
	}
	outCol := chunk.NewColumn(tp, len(values))
	column := &Column{RetType: tp, Index: 0}

	err := columnColEvalValue(column, s.ctx, chk, outCol)
	c.Assert(err, IsNil)
	c.Assert(inCol.GetLength(), Equals, outCol.GetLength())

	inColData := reflect.ValueOf(*inCol).FieldByName("data").Bytes()
	outColData := reflect.ValueOf(*outCol).FieldByName("data").Bytes()
	c.Assert(inColData, DeepEquals, outColData)
}

func columnAppendValue(col *chunk.Column, tp *types.FieldType, value interface{}) {
	if value == nil {
		col.AppendNull()
	} else {
		switch tp {
		case int64Type:
			col.AppendInt64(value.(int64))
		case float64Type:
			col.AppendFloat64(value.(float64))
		case newDecimalType:
			col.AppendMyDecimal(value.(*types.MyDecimal))
		case varStringType:
			col.AppendString(value.(string))
		default:
			panic("invalid type")
		}
	}
}

func columnColEvalValue(col *Column, ctx sessionctx.Context, chk *chunk.Chunk, out *chunk.Column) error {
	switch col.RetType {
	case int64Type:
		return col.ColEvalInt(ctx, chk, out)
	case float64Type:
		return col.ColEvalReal(ctx, chk, out)
	case newDecimalType:
		return col.ColEvalDecimal(ctx, chk, out)
	case varStringType:
		return col.ColEvalString(ctx, chk, out)
	default:
		return errors.New("invalid type")
	}
}

func (s *testColumnEvaluatorSuite) TestInt64Plus(c *C) {
	// case 1: constant + constant
	args := []Expression{
		&Constant{
			Value:   types.NewDatum(123),
			RetType: int64Type,
		},
		&Constant{
			Value:   types.NewDatum(456),
			RetType: int64Type,
		},
	}

	fn, err := funcs[ast.Plus].getFunction(s.ctx, args)
	c.Assert(fn, NotNil)
	c.Assert(err, IsNil)
	plusFn, ok := fn.(*builtinArithmeticPlusIntSig)
	c.Assert(plusFn, NotNil)
	c.Assert(ok, IsTrue)

	inputChk := chunk.NewChunkWithCapacity(nil, 0)
	inputChk.SetNumVirtualRows(1)
	outputCol := chunk.NewColumn(int64Type, 1)

	err = plusFn.colEvalInt(inputChk, outputCol)
	c.Assert(err, IsNil)
	c.Assert(outputCol.GetLength(), Equals, 1)
	result := outputCol.GetInt64(0)
	c.Assert(result, Equals, int64(579))

	// case 2: column + column
	args = []Expression{
		&Column{
			RetType: int64Type,
			Index:   0,
		},
		&Column{
			RetType: int64Type,
			Index:   1,
		},
	}

	fn, err = funcs[ast.Plus].getFunction(s.ctx, args)
	c.Assert(fn, NotNil)
	c.Assert(err, IsNil)
	plusFn, ok = fn.(*builtinArithmeticPlusIntSig)
	c.Assert(plusFn, NotNil)
	c.Assert(ok, IsTrue)

	inputChk = chunk.NewChunkWithCapacity([]*types.FieldType{int64Type, int64Type}, 3)
	aCol := inputChk.GetColumn(0)
	aCol.AppendInt64(1234)
	aCol.AppendNull()
	aCol.AppendInt64(-1234)
	bCol := inputChk.GetColumn(1)
	bCol.AppendInt64(5678)
	bCol.AppendInt64(-1234)
	bCol.AppendNull()
	outputCol.Reset()

	err = plusFn.colEvalInt(inputChk, outputCol)
	c.Assert(err, IsNil)
	c.Assert(outputCol.GetLength(), Equals, 3)
	c.Assert(outputCol.IsNull(0), IsFalse)
	c.Assert(outputCol.GetInt64(0), Equals, int64(1234+5678))
	c.Assert(outputCol.IsNull(1), IsTrue)
	c.Assert(outputCol.IsNull(2), IsTrue)

	// case 3: column + constant
	args[1] = &Constant{
		Value:   types.NewDatum(5678),
		RetType: int64Type,
	}

	fn, err = funcs[ast.Plus].getFunction(s.ctx, args)
	c.Assert(fn, NotNil)
	c.Assert(err, IsNil)
	plusFn, ok = fn.(*builtinArithmeticPlusIntSig)
	c.Assert(plusFn, NotNil)
	c.Assert(ok, IsTrue)

	outputCol.Reset()
	err = plusFn.colEvalInt(inputChk, outputCol)
	c.Assert(err, IsNil)
	c.Assert(outputCol.GetLength(), Equals, 3)
	c.Assert(outputCol.IsNull(0), IsFalse)
	c.Assert(outputCol.GetInt64(0), Equals, int64(1234+5678))
	c.Assert(outputCol.IsNull(1), IsTrue)
	c.Assert(outputCol.IsNull(2), IsFalse)
	c.Assert(outputCol.GetInt64(2), Equals, int64(-1234+5678))
}

func (s *testColumnEvaluatorSuite) TestFloat64Multiply(c *C) {
	// case 1: constant * constant
	args := []Expression{
		&Constant{
			Value:   types.NewDatum(123.456),
			RetType: float64Type,
		},
		&Constant{
			Value:   types.NewDatum(789.012),
			RetType: float64Type,
		},
	}

	fn, err := funcs[ast.Mul].getFunction(s.ctx, args)
	c.Assert(fn, NotNil)
	c.Assert(err, IsNil)
	mulFn, ok := fn.(*builtinArithmeticMultiplyRealSig)
	c.Assert(mulFn, NotNil)
	c.Assert(ok, IsTrue)

	inputChk := chunk.NewChunkWithCapacity(nil, 0)
	inputChk.SetNumVirtualRows(1)
	outputCol := chunk.NewColumn(int64Type, 1)

	err = mulFn.colEvalReal(inputChk, outputCol)
	c.Assert(err, IsNil)
	c.Assert(outputCol.GetLength(), Equals, 1)
	result := outputCol.GetFloat64(0)
	c.Assert(result, Equals, 123.456*789.012)

	// case 2: column * column
	args = []Expression{
		&Column{
			RetType: float64Type,
			Index:   0,
		},
		&Column{
			RetType: float64Type,
			Index:   1,
		},
	}

	fn, err = funcs[ast.Mul].getFunction(s.ctx, args)
	c.Assert(fn, NotNil)
	c.Assert(err, IsNil)
	mulFn, ok = fn.(*builtinArithmeticMultiplyRealSig)
	c.Assert(mulFn, NotNil)
	c.Assert(ok, IsTrue)

	inputChk = chunk.NewChunkWithCapacity([]*types.FieldType{float64Type, float64Type}, 3)
	aCol := inputChk.GetColumn(0)
	aCol.AppendFloat64(1234.56789)
	aCol.AppendNull()
	aCol.AppendFloat64(-1234.56789)
	bCol := inputChk.GetColumn(1)
	bCol.AppendFloat64(9876.54321)
	bCol.AppendFloat64(-9876.54321)
	bCol.AppendNull()
	outputCol.Reset()

	err = mulFn.colEvalReal(inputChk, outputCol)
	c.Assert(err, IsNil)
	c.Assert(outputCol.GetLength(), Equals, 3)
	c.Assert(outputCol.IsNull(0), IsFalse)
	c.Assert(outputCol.GetFloat64(0), Equals, 1234.56789*9876.54321)
	c.Assert(outputCol.IsNull(1), IsTrue)
	c.Assert(outputCol.IsNull(2), IsTrue)

	// case 3: column + constant
	args[1] = &Constant{
		Value:   types.NewDatum(9876.54321),
		RetType: float64Type,
	}

	fn, err = funcs[ast.Mul].getFunction(s.ctx, args)
	c.Assert(fn, NotNil)
	c.Assert(err, IsNil)
	mulFn, ok = fn.(*builtinArithmeticMultiplyRealSig)
	c.Assert(mulFn, NotNil)
	c.Assert(ok, IsTrue)

	outputCol.Reset()
	err = mulFn.colEvalReal(inputChk, outputCol)
	c.Assert(err, IsNil)
	c.Assert(outputCol.GetLength(), Equals, 3)
	c.Assert(outputCol.IsNull(0), IsFalse)
	c.Assert(outputCol.GetFloat64(0), Equals, 1234.56789*9876.54321)
	c.Assert(outputCol.IsNull(1), IsTrue)
	c.Assert(outputCol.IsNull(2), IsFalse)
	c.Assert(outputCol.GetFloat64(2), Equals, -1234.56789*9876.54321)
}

func (s *testColumnEvaluatorSuite) TestDecimalGreatest(c *C) {
	// case 1: constants
	floats := []float64{123.456, 789.012, -23432.34}
	args := make([]Expression, len(floats))
	for i, float := range floats {
		d := new(types.MyDecimal)
		c.Assert(d.FromFloat64(float), IsNil)
		args[i] = &Constant{
			Value:   types.NewDecimalDatum(d),
			RetType: newDecimalType,
		}
	}

	inChk := chunk.NewChunkWithCapacity(nil, 0)
	inChk.SetNumVirtualRows(1)
	outCol := chunk.NewColumn(float64Type, 1)

	fn, err := funcs[ast.Greatest].getFunction(s.ctx, args)
	c.Assert(fn, NotNil)
	c.Assert(err, IsNil)
	maxFn, ok := fn.(*builtinGreatestDecimalSig)
	c.Assert(maxFn, NotNil)
	c.Assert(ok, IsTrue)

	err = maxFn.colEvalDecimal(inChk, outCol)

	c.Assert(err, IsNil)
	c.Assert(outCol.GetLength(), Equals, 1)
	res := outCol.GetMyDecimal(0)
	expect, isNull, err := args[1].EvalDecimal(s.ctx, chunk.Row{})
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)
	c.Assert(res.Compare(expect), Equals, 0)

	// case 2: columns
	columnFloats := [][]interface{}{
		{123.456, 789.012},
		{nil, 1234.456},
		{92312.3, 432.3432},
	}

	for i := range args {
		args[i] = &Column{
			RetType: newDecimalType,
			Index:   i,
		}
	}

	fts := make([]*types.FieldType, len(columnFloats))
	for i := range fts {
		fts[i] = newDecimalType
	}
	inChk = chunk.NewChunkWithCapacity(fts, len(columnFloats[0]))
	for i := range columnFloats {
		col := inChk.GetColumn(i)
		for _, value := range columnFloats[i] {
			if value == nil {
				col.AppendNull()
			} else {
				d := new(types.MyDecimal)
				c.Assert(d.FromFloat64(value.(float64)), IsNil)
				col.AppendMyDecimal(d)
			}
		}
	}

	outCol.Reset()

	fn, err = funcs[ast.Greatest].getFunction(s.ctx, args)
	c.Assert(fn, NotNil)
	c.Assert(err, IsNil)
	maxFn, ok = fn.(*builtinGreatestDecimalSig)
	c.Assert(maxFn, NotNil)
	c.Assert(ok, IsTrue)

	err = maxFn.colEvalDecimal(inChk, outCol)
	c.Assert(err, IsNil)
	c.Assert(outCol.GetLength(), Equals, len(columnFloats[0]))
	c.Assert(outCol.IsNull(0), IsTrue)
	c.Assert(outCol.IsNull(1), IsFalse)

	d := new(types.MyDecimal)
	c.Assert(d.FromFloat64(1234.456), IsNil)
	c.Assert(outCol.GetMyDecimal(1).Compare(d), Equals, 0)
}

func (s *testColumnEvaluatorSuite) TestConcatString(c *C) {
	inChk := chunk.NewChunkWithCapacity([]*types.FieldType{varStringType}, 4)
	inCol := inChk.GetColumn(0)
	for i := 0; i < 3; i++ {
		inCol.AppendString(strconv.Itoa(i) + " Hello")
	}
	inCol.AppendNull()
	outCol := chunk.NewColumn(varStringType, 4)

	args := []Expression{
		&Column{
			RetType: varStringType,
			Index:   0,
		},
		&Constant{
			Value:   types.NewStringDatum(" world"),
			RetType: varStringType,
		},
	}

	fn, err := funcs[ast.Concat].getFunction(s.ctx, args)
	c.Assert(fn, NotNil)
	c.Assert(err, IsNil)
	concatFn, ok := fn.(*builtinConcatSig)
	c.Assert(concatFn, NotNil)
	c.Assert(ok, IsTrue)

	err = concatFn.colEvalString(inChk, outCol)

	c.Assert(err, IsNil)
	for i := 0; i < 3; i++ {
		c.Assert(outCol.IsNull(i), IsFalse)
		gotString := outCol.GetString(i)
		expectString := strconv.Itoa(i) + " Hello world"
		c.Assert(gotString, Equals, expectString)
	}
	c.Assert(outCol.IsNull(3), IsTrue)
}
