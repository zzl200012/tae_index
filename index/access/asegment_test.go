package access

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"tae/mock"
	"testing"
)

func TestAppendableSegmentIndexHolder(t *testing.T) {
	var err error
	var res bool
	typ := types.Type{Oid: types.T_int32}
	rowsPerBatch := 10000
	batchesPerBlock :=  4
	blockCount := 10
	segment := mock.NewSegment()
	indexHolder := NewAppendableSegmentIndexHolder(segment)

	counter := 0
	for i := 0; i < blockCount; i++ {
		for j := 0; j < batchesPerBlock; j++ {
			offset := rowsPerBatch * counter
			batch := mock.MockVec(typ, rowsPerBatch, offset)
			res, err = indexHolder.ContainsAnyKeys(batch)
			require.NoError(t, err)
			require.False(t, res)
			err = indexHolder.BatchInsert(batch, 0, rowsPerBatch, uint32(offset), false)
			require.NoError(t, err)
			counter++
		}
		err = indexHolder.CloseCurrentActiveBlock()
		require.NoError(t, err)
	}
	err = indexHolder.MarkAsImmutable()
	require.NoError(t, err)
	require.True(t, indexHolder.ReadyToUpgrade())

	//t.Log(indexHolder.Print())

	upgraded, err := indexHolder.Upgrade()
	require.NoError(t, err)

	t.Log(upgraded.Print())
}
