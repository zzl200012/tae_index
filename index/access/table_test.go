package access

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"tae/index/common"
	"tae/mock"
	"testing"
	"time"
)

func TestTableIndexHolder(t *testing.T) {
	table := mock.NewTable()
	tableHolder := NewTableIndexHolder(table.Resource)
	segmentCount := 10
	blockPerSegment := 40
	batchPerBlock := 4
	rowsPerBatch := 10000

	typ := types.Type{Oid: types.T_int32}
	//colIdx := uint16(0)
	//cTyp := common.Plain

	var err error
	var res bool

	counter := 0
	for i := 0; i < segmentCount; i++ {
		segment := table.NewSegment()
		err = tableHolder.RegisterSegment(segment.Resource)
		require.NoError(t, err)
		for j := 0; j < blockPerSegment; j++ {
			block := segment.NewBlock()
			err = tableHolder.RegisterBlock(block.Resource)
			require.NoError(t, err)
			segment.AddChild(block.Resource)
			for k := 0; k < batchPerBlock; k++ {
				offset := counter * rowsPerBatch
				batch := mock.MockVec(typ, rowsPerBatch, offset)
				counter++
				err = tableHolder.BatchInsert(batch, 0, rowsPerBatch, uint32(offset), true)
				require.NoError(t, err)
				block.AppendData(batch)
			}
			//if j == blockPerSegment - 1 && i == segmentCount - 1 {
			//	break
			//}
			err = tableHolder.CloseCurrentActiveBlock()
			require.NoError(t, err)
		}
		//if i == segmentCount - 1 {
		//	break
		//}
		err = tableHolder.CloseCurrentActiveSegment()
		require.NoError(t, err)
		err = tableHolder.UpgradeSegment(segment.GetSegmentId())
		require.NoError(t, err)
	}

	//t.Log(tableHolder.ContainsKey(int32(2333)))
	//t.Log(tableHolder.ContainsKey(int32(segmentCount * blockPerSegment * batchPerBlock * rowsPerBatch)))

	total := segmentCount * blockPerSegment * batchPerBlock * rowsPerBatch
	batchSize := 30000
	batches := make([]*vector.Vector, 0)
	batchCount := 10
	for i := 0; i < batchCount; i++ {
		batch := mock.MockVec(typ, batchSize, total + batchSize * i)
		batches = append(batches, batch)
	}
	t.Log(tableHolder.PrintShort())

	common.ZoneMapConsulted = 0
	common.StaticFilterConsulted = 0
	common.ARTIndexConsulted = 0
	start := time.Now()
	for _, batch := range batches {
		res, err = tableHolder.ContainsAnyKeys(batch)
		require.NoError(t, err)
		require.False(t, res)
	}
	t.Log("total rows: ", total)
	t.Log("op: de-duplicating a batch of 30000 rows")
	t.Log(time.Since(start).Milliseconds() / 10, " ms/op")

	t.Log("filter: ", common.StaticFilterConsulted / float32(batchCount), " times consulted")
	t.Log("zone map: ", common.ZoneMapConsulted / float32(batchCount), " times consulted")
	t.Log("art: ", common.ARTIndexConsulted / float32(batchCount), " times consulted")

	//data := make([]int32, 0)
	//for i := 0; i < 100000; i++ {
	//	data = append(data, int32(i))
	//}
	//start = time.Now()
	//for _, d := range data {
	//	res, err = tableHolder.ContainsKey(d)
	//	require.NoError(t, err)
	//	require.True(t, res)
	//}
	//t.Log(time.Since(start).Milliseconds(), " ms/op")
	////t.Log(table.FetchBufferManager().String())
	////t.Log(tableHolder.Print())
	//
	//t.Log("filter: ", common.StaticFilterConsulted, " times consulted")
	//t.Log("zone map: ", common.ZoneMapConsulted, " times consulted")
}
