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
	default:
		return Vector(NewVecInt64(numVals))
	}
}
