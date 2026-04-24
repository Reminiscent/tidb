// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeV2SubBuildWorkerReleasesColumnCollectorMemoryImmediately(t *testing.T) {
	bytesWithoutExtStats, collectorMemWithoutExtStats := runAnalyzeV2SubBuildWorker(t, false)
	bytesWithExtStats, collectorMemWithExtStats := runAnalyzeV2SubBuildWorker(t, true)

	require.Zero(t, collectorMemWithoutExtStats)
	require.Positive(t, collectorMemWithExtStats)
	require.Equal(t, collectorMemWithExtStats, bytesWithExtStats-bytesWithoutExtStats)
}

func runAnalyzeV2SubBuildWorker(t *testing.T, needExtStats bool) (int64, int64) {
	t.Helper()

	ctx := mock.NewContext()
	ctx.GetSessionVars().EnableExtendedStats = needExtStats

	ft := types.NewFieldType(mysql.TypeVarchar)
	ft.SetCharset(mysql.UTF8MB4Charset)
	ft.SetCollate("utf8mb4_general_ci")
	col := &model.ColumnInfo{FieldType: *ft}

	rootRowCollector := statistics.NewReservoirRowSampleCollector(4, 1)
	rootRowCollector.Base().FMSketches = append(rootRowCollector.Base().FMSketches, statistics.NewFMSketch(statistics.MaxSketchSize))
	rootRowCollector.Base().NullCount = []int64{0}
	rootRowCollector.Base().TotalSizes = make([]int64, 1)

	values := []string{"Alpha", "beta", "beta", "Gamma"}
	for _, value := range values {
		datum := types.NewStringDatum(value)
		require.NoError(t, rootRowCollector.Base().FMSketches[0].InsertValue(ctx.GetSessionVars().StmtCtx, datum))
		rootRowCollector.Base().Samples = append(rootRowCollector.Base().Samples, &statistics.ReservoirRowSampleItem{
			Columns: []types.Datum{datum},
		})
		rootRowCollector.Base().Count++
		rootRowCollector.Base().TotalSizes[0] += int64(len(datum.GetBytes()))
	}

	exec := &AnalyzeColumnsExecV2{
		AnalyzeColumnsExec: &AnalyzeColumnsExec{
			baseAnalyzeExec: baseAnalyzeExec{
				ctx: ctx,
				opts: map[ast.AnalyzeOptionType]uint64{
					ast.AnalyzeOptNumBuckets: 2,
					ast.AnalyzeOptNumTopN:    1,
				},
			},
			colsInfo:   []*model.ColumnInfo{col},
			memTracker: memory.NewTracker(1, -1),
		},
	}

	hists := make([]*statistics.Histogram, 1)
	topns := make([]*statistics.TopN, 1)
	collectors := make([]*statistics.SampleCollector, 1)
	resultCh := make(chan error, 1)
	taskCh := make(chan *samplingBuildTask, 1)
	taskCh <- &samplingBuildTask{
		id:               1,
		rootRowCollector: rootRowCollector,
		tp:               ft,
		isColumn:         true,
		slicePos:         0,
	}
	close(taskCh)

	exec.subBuildWorker(resultCh, taskCh, hists, topns, collectors, needExtStats, make(chan struct{}))
	require.NoError(t, <-resultCh)
	require.NotNil(t, hists[0])
	require.NotNil(t, topns[0])

	bytesConsumed := exec.memTracker.BytesConsumed()
	collectorMem := int64(0)
	if needExtStats {
		require.NotNil(t, collectors[0])
		collectorMem = collectors[0].MemSize
		require.Positive(t, collectorMem)
		exec.memTracker.Release(collectorMem)
		collectors[0].Destroy()
		collectors[0] = nil
	} else {
		require.Nil(t, collectors[0])
	}

	return bytesConsumed, collectorMem
}
