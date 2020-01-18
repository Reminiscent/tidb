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

// Code generated by go generate in expression/generator; DO NOT EDIT.

package expression

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

type inGener struct {
	defaultGener
}

func (g inGener) gen() interface{} {
	if rand.Float64() < g.nullRation {
		return nil
	}
	randNum := rand.Int63n(10)
	switch g.eType {
	case types.ETInt:
		if rand.Float64() < 0.5 {
			return -randNum
		}
		return randNum
	case types.ETReal:
		if rand.Float64() < 0.5 {
			return -float64(randNum)
		}
		return float64(randNum)
	case types.ETDecimal:
		d := new(types.MyDecimal)
		f := float64(randNum * 100000)
		if err := d.FromFloat64(f); err != nil {
			panic(err)
		}
		return d
	case types.ETDatetime, types.ETTimestamp:
		gt := types.FromDate(2019, 11, 2, 22, 00, int(randNum), rand.Intn(1000000))
		t := types.NewTime(gt, convertETType(g.eType), 0)
		return t
	case types.ETDuration:
		return types.Duration{Duration: time.Duration(randNum)}
	case types.ETJson:
		j := new(json.BinaryJSON)
		jsonStr := fmt.Sprintf("{\"key\":%v}", randNum)
		if err := j.UnmarshalJSON([]byte(jsonStr)); err != nil {
			panic(err)
		}
		return *j
	case types.ETString:
		return fmt.Sprint(randNum)
	}
	return randNum
}

var vecBuiltinOtherGeneratedCases = map[string][]vecExprBenchCase{
	ast.In: {
		// builtinInIntSig
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETInt,
				types.ETInt,
				types.ETInt,
				types.ETInt,
			},
			geners: []dataGenerator{
				inGener{*newDefaultGener(0.2, types.ETInt)},
				inGener{*newDefaultGener(0.2, types.ETInt)},
				inGener{*newDefaultGener(0.2, types.ETInt)},
				inGener{*newDefaultGener(0.2, types.ETInt)},
			},
		},
		// builtinInStringSig
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETString,
				types.ETString,
				types.ETString,
				types.ETString,
			},
			geners: []dataGenerator{
				inGener{*newDefaultGener(0.2, types.ETString)},
				inGener{*newDefaultGener(0.2, types.ETString)},
				inGener{*newDefaultGener(0.2, types.ETString)},
				inGener{*newDefaultGener(0.2, types.ETString)},
			},
		},
		// builtinInDecimalSig
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETDecimal,
				types.ETDecimal,
				types.ETDecimal,
				types.ETDecimal,
			},
			geners: []dataGenerator{
				inGener{*newDefaultGener(0.2, types.ETDecimal)},
				inGener{*newDefaultGener(0.2, types.ETDecimal)},
				inGener{*newDefaultGener(0.2, types.ETDecimal)},
				inGener{*newDefaultGener(0.2, types.ETDecimal)},
			},
		},
		// builtinInRealSig
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETReal,
				types.ETReal,
				types.ETReal,
				types.ETReal,
			},
			geners: []dataGenerator{
				inGener{*newDefaultGener(0.2, types.ETReal)},
				inGener{*newDefaultGener(0.2, types.ETReal)},
				inGener{*newDefaultGener(0.2, types.ETReal)},
				inGener{*newDefaultGener(0.2, types.ETReal)},
			},
		},
		// builtinInTimeSig
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETDatetime,
				types.ETDatetime,
				types.ETDatetime,
				types.ETDatetime,
			},
			geners: []dataGenerator{
				inGener{*newDefaultGener(0.2, types.ETDatetime)},
				inGener{*newDefaultGener(0.2, types.ETDatetime)},
				inGener{*newDefaultGener(0.2, types.ETDatetime)},
				inGener{*newDefaultGener(0.2, types.ETDatetime)},
			},
		},
		// builtinInDurationSig
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETDuration,
				types.ETDuration,
				types.ETDuration,
				types.ETDuration,
			},
			geners: []dataGenerator{
				inGener{*newDefaultGener(0.2, types.ETDuration)},
				inGener{*newDefaultGener(0.2, types.ETDuration)},
				inGener{*newDefaultGener(0.2, types.ETDuration)},
				inGener{*newDefaultGener(0.2, types.ETDuration)},
			},
		},
		// builtinInJSONSig
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETJson,
				types.ETJson,
				types.ETJson,
				types.ETJson,
			},
			geners: []dataGenerator{
				inGener{*newDefaultGener(0.2, types.ETJson)},
				inGener{*newDefaultGener(0.2, types.ETJson)},
				inGener{*newDefaultGener(0.2, types.ETJson)},
				inGener{*newDefaultGener(0.2, types.ETJson)},
			},
		},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinOtherEvalOneVecGenerated(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinOtherGeneratedCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinOtherFuncGenerated(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinOtherGeneratedCases)
}

func BenchmarkVectorizedBuiltinOtherEvalOneVecGenerated(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinOtherGeneratedCases)
}

func BenchmarkVectorizedBuiltinOtherFuncGenerated(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinOtherGeneratedCases)
}
