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
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

var _ = check.Suite(&poolTestSuite{})

type poolTestSuite struct{}

func (s *poolTestSuite) TestNewPool(c *check.C) {
	pool := NewPool(1024)
	c.Assert(pool.initCap, check.Equals, 1024)
	c.Assert(pool.varLenColPool, check.NotNil)
	c.Assert(pool.fixLenColPool4, check.NotNil)
	c.Assert(pool.fixLenColPool8, check.NotNil)
	c.Assert(pool.fixLenColPool16, check.NotNil)
	c.Assert(pool.fixLenColPool40, check.NotNil)
}

func (s *poolTestSuite) TestPoolGetChunk(c *check.C) {
	initCap := 1024
	pool := NewPool(initCap)

	fieldTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
		{Tp: mysql.TypeJSON},
		{Tp: mysql.TypeFloat},
		{Tp: mysql.TypeNewDecimal},
		{Tp: mysql.TypeDouble},
		{Tp: mysql.TypeLonglong},
		{Tp: mysql.TypeTimestamp},
		{Tp: mysql.TypeDatetime},
	}

	chk := pool.GetChunk(fieldTypes)
	c.Assert(chk, check.NotNil)
	c.Assert(chk.NumCols(), check.Equals, len(fieldTypes))
	c.Assert(chk.Columns[0].elemBuf, check.IsNil)
	c.Assert(chk.Columns[1].elemBuf, check.IsNil)
	c.Assert(len(chk.Columns[2].elemBuf), check.Equals, getFixedLen(fieldTypes[2]))
	c.Assert(len(chk.Columns[3].elemBuf), check.Equals, getFixedLen(fieldTypes[3]))
	c.Assert(len(chk.Columns[4].elemBuf), check.Equals, getFixedLen(fieldTypes[4]))
	c.Assert(len(chk.Columns[5].elemBuf), check.Equals, getFixedLen(fieldTypes[5]))
	c.Assert(len(chk.Columns[6].elemBuf), check.Equals, getFixedLen(fieldTypes[6]))
	c.Assert(len(chk.Columns[7].elemBuf), check.Equals, getFixedLen(fieldTypes[7]))

	c.Assert(cap(chk.Columns[2].Data), check.Equals, initCap*getFixedLen(fieldTypes[2]))
	c.Assert(cap(chk.Columns[3].Data), check.Equals, initCap*getFixedLen(fieldTypes[3]))
	c.Assert(cap(chk.Columns[4].Data), check.Equals, initCap*getFixedLen(fieldTypes[4]))
	c.Assert(cap(chk.Columns[5].Data), check.Equals, initCap*getFixedLen(fieldTypes[5]))
	c.Assert(cap(chk.Columns[6].Data), check.Equals, initCap*getFixedLen(fieldTypes[6]))
	c.Assert(cap(chk.Columns[7].Data), check.Equals, initCap*getFixedLen(fieldTypes[7]))
}

func (s *poolTestSuite) TestPoolPutChunk(c *check.C) {
	initCap := 1024
	pool := NewPool(initCap)

	fieldTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
		{Tp: mysql.TypeJSON},
		{Tp: mysql.TypeFloat},
		{Tp: mysql.TypeNewDecimal},
		{Tp: mysql.TypeDouble},
		{Tp: mysql.TypeLonglong},
		{Tp: mysql.TypeTimestamp},
		{Tp: mysql.TypeDatetime},
	}

	chk := pool.GetChunk(fieldTypes)
	pool.PutChunk(fieldTypes, chk)
	c.Assert(len(chk.Columns), check.Equals, 0)
}

func BenchmarkPoolChunkOperation(b *testing.B) {
	pool := NewPool(1024)

	fieldTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
		{Tp: mysql.TypeJSON},
		{Tp: mysql.TypeFloat},
		{Tp: mysql.TypeNewDecimal},
		{Tp: mysql.TypeDouble},
		{Tp: mysql.TypeLonglong},
		{Tp: mysql.TypeTimestamp},
		{Tp: mysql.TypeDatetime},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.PutChunk(fieldTypes, pool.GetChunk(fieldTypes))
		}
	})
}
