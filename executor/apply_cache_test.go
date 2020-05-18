// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"strconv"
)

var _ = SerialSuites(&testApplyCacheSuite{})

type testApplyCacheSuite struct {
}

func (s *testApplyCacheSuite) TestApplyCache(c *C) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().ApplyCacheQuota = 1
	applyCache, err := newApplyCache(ctx)
	c.Assert(err, IsNil)

	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
	}
	l := make([]*chunk.List, 3)
	value := make([]applyCacheValue, 3)
	key := make([]string, 3)
	for i := 0; i < 3; i++ {
		l[i] = chunk.NewList(fields, 1, 1)
		srcChunk := chunk.NewChunkWithCapacity(fields, 1)
		srcChunk.AppendInt64(0, int64(i))
		srcRow := srcChunk.GetRow(0)
		l[i].AppendRow(srcRow)
		key[i] = strconv.Itoa(i)
		value[i].Data = l[i]
	}

	evicted := applyCache.Set(key[0], &value[0])
	c.Assert(evicted, Equals, false)
	result := applyCache.Get(key[0])
	c.Assert(result, NotNil)

	evicted = applyCache.Set(key[1], &value[1])
	c.Assert(evicted, Equals, true)
	result = applyCache.Get(key[1])
	c.Assert(result, NotNil)

	evicted = applyCache.Set(key[2], &value[2])
	c.Assert(evicted, Equals, true)
	result = applyCache.Get(key[2])
	c.Assert(result, NotNil)

	// Both key[0] and key[1] are not in the cache
	result = applyCache.Get(key[0])
	c.Assert(result, IsNil)

	result = applyCache.Get(key[1])
	c.Assert(result, IsNil)
}
