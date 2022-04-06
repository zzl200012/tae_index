package access

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"tae/index/utils/mock/holder"
	"tae/mock"
	"testing"
)

func TestNonAppendableBlockIndexHolder(t *testing.T) {
	var err error
	var res bool
	//var meta *common.IndexMeta
	//blockCount := 40
	rowsPerBatch := 10000
	block := mock.NewSegment()
	holder := holder.NewMockAppendableBlockIndexHolder(block)

	batches := make([]*vector.Vector, 0)
	for i := 0; i < 4; i++ {
		batch := mock.MockVec(block.GetPrimaryKeyType(), rowsPerBatch, i * rowsPerBatch)
		batches = append(batches, batch)
	}

	for i, batch := range batches {
		err = holder.BatchInsert(batch, 0, rowsPerBatch, uint32(rowsPerBatch*i), false)
		require.NoError(t, err)
		t.Log(holder.Search(batch.Col.([]int32)[rowsPerBatch / 2]))
		res, err = holder.ContainsKey(int32(50000))
		require.NoError(t, err)
		require.False(t, res)
		query := mock.MockVec(block.GetPrimaryKeyType(), rowsPerBatch, (i + 1) * rowsPerBatch)
		res, err = holder.ContainsAnyKeys(query)
		require.NoError(t, err)
		require.False(t, res)
	}

	t.Log(holder.Print())

	newHolder, err := holder.Freeze()
	require.NoError(t, err)

	t.Log(newHolder.Print())
}
