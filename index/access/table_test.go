package access

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"tae/mock"
	"testing"
	"time"
)

func TestTableIndexHolder(t *testing.T) {
	table := mock.NewResource()
	tableHolder := NewTableIndexHolder(table)
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
		segment := mock.NewResource()
		err = tableHolder.RegisterSegment(segment)
		require.NoError(t, err)
		for j := 0; j < blockPerSegment; j++ {
			block := mock.NewResource()
			err = tableHolder.RegisterBlock(block)
			require.NoError(t, err)
			segment.AddChild(block)
			for k := 0; k < batchPerBlock; k++ {
				offset := counter * rowsPerBatch
				batch := mock.MockVec(typ, rowsPerBatch, offset)
				counter++
				err = tableHolder.BatchInsert(batch, 0, rowsPerBatch, uint32(offset), true)
				require.NoError(t, err)
				block.AppendData(batch)
			}
			//segment.AppendData(block.GetData())
			err = tableHolder.CloseCurrentActiveBlock()
			require.NoError(t, err)
		}
		err = tableHolder.CloseCurrentActiveSegment()
		require.NoError(t, err)
	}

	//t.Log(tableHolder.ContainsKey(int32(2333)))
	//t.Log(tableHolder.ContainsKey(int32(segmentCount * blockPerSegment * batchPerBlock * rowsPerBatch)))

	total := segmentCount * blockPerSegment * batchPerBlock * rowsPerBatch
	batchSize := 30000
	batches := make([]*vector.Vector, 0)
	for i := 0; i < 10; i++ {
		batch := mock.MockVec(typ, batchSize, total + batchSize * i)
		batches = append(batches, batch)
	}
	t.Log(tableHolder.Print())

	start := time.Now()
	for _, batch := range batches {
		res, err = tableHolder.ContainsAnyKeys(batch)
		require.NoError(t, err)
		require.False(t, res)
	}
	t.Log("total rows: ", total)
	t.Log("op: de-duplicating a batch of 30000 rows")
	t.Log(time.Since(start).Milliseconds() / 10, " ms/op")

	//t.Log(table.FetchBufferManager().String())
	t.Log(tableHolder.Print())
}
