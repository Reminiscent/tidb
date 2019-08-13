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
			RetType: types.NewFieldType(mysql.TypeLonglong),
		},
		&Constant{
			Value:   types.NewDatum(456),
			RetType: types.NewFieldType(mysql.TypeLonglong),
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
		RetType: types.NewFieldType(mysql.TypeLonglong),
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
