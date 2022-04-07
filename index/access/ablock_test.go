package access

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"tae/mock"
	"testing"
)

func TestNonAppendableBlockIndexHolder(t *testing.T) {
	var err error
	var res bool
	rowsPerBatch := 10000
	block := mock.NewSegment()
	indexHolder := NewAppendableBlockIndexHolder(block)

	batches := make([]*vector.Vector, 0)
	for i := 0; i < 4; i++ {
		batch := mock.MockVec(block.GetPrimaryKeyType(), rowsPerBatch, i * rowsPerBatch)
		batches = append(batches, batch)
	}

	for i, batch := range batches {
		err = indexHolder.BatchInsert(batch, 0, rowsPerBatch, uint32(rowsPerBatch*i), false)
		require.NoError(t, err)
		//t.Log(indexHolder.Search(batch.Col.([]int32)[rowsPerBatch / 2]))
		res, err = indexHolder.ContainsKey(int32(50000))
		require.NoError(t, err)
		require.False(t, res)
		query := mock.MockVec(block.GetPrimaryKeyType(), rowsPerBatch, (i + 1) * rowsPerBatch)
		res, err = indexHolder.ContainsAnyKeys(query)
		require.NoError(t, err)
		require.False(t, res)
	}
	//t.Log(indexHolder.Print())

	newHolder, err := indexHolder.Upgrade()
	require.NoError(t, err)

	t.Log(newHolder.Print())
}
