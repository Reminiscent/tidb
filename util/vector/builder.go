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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

// NewVector creates a Vector to store numVals data.
func NewVector(colType *types.FieldType, numVals int) Vector {
	switch colType.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		return Vector(NewVecInt64(numVals))
	case mysql.TypeDouble:
		return Vector(NewVecFloat64(numVals))
	case mysql.TypeFloat:
		return Vector(NewVecFloat32(numVals))
	case mysql.TypeDuration:
		return Vector(NewVecDuration(numVals))
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return Vector(NewVecDatetime(numVals))
	case mysql.TypeNewDecimal:
		return Vector(NewVecDecimal(numVals))
	case mysql.TypeEnum, mysql.TypeSet:
		return Vector(NewVecNamedValue(numVals))
	case mysql.TypeJSON:
		return Vector(NewVecJSON(numVals))
	default:
		return Vector(NewVecString(numVals))
	}
}
