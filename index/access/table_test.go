package access

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"tae/mock"
	"testing"
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

	counter := 0
	for i := 0; i < segmentCount; i++ {
		segment := mock.NewResource()
		err = tableHolder.RegisterSegment(segment)
		require.NoError(t, err)
		for j := 0; j < blockPerSegment; j++ {
			block := mock.NewResource()
			err = tableHolder.RegisterBlock(block)
			require.NoError(t, err)
			for k := 0; k < batchPerBlock; k++ {
				offset := counter * rowsPerBatch
				batch := mock.MockVec(typ, rowsPerBatch, offset)
				counter++
				err = tableHolder.BatchInsert(batch, 0, rowsPerBatch, uint32(offset), true)
				require.NoError(t, err)
			}
			err = tableHolder.CloseCurrentActiveBlock()
			require.NoError(t, err)
		}
		err = tableHolder.CloseCurrentActiveSegment()
		require.NoError(t, err)
	}

	t.Log(tableHolder.ContainsKey(int32(2333)))
	t.Log(tableHolder.ContainsKey(int32(segmentCount * blockPerSegment * batchPerBlock * rowsPerBatch)))
}
