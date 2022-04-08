package access

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"tae/index/common"
	"tae/index/io"
	"tae/mock"
	"testing"
)

func TestNonAppendableBlockIndexHolder(t *testing.T) {
	var err error
	var res bool
	var meta *common.IndexMeta
	typ := types.Type{Oid: types.T_int32}
	cTyp := common.Plain
	colIdx := uint16(0)
	batchPerBlock := 4
	rowsPerBatch := 10000
	block := mock.NewResource()
	indexHolder := NewNonAppendableBlockIndexHolder(block)
	zmWriter := io.NewBlockZoneMapIndexWriter()
	err = zmWriter.Init(indexHolder, cTyp, colIdx)
	require.NoError(t, err)
	sfWriter := io.NewStaticFilterIndexWriter()
	err = sfWriter.Init(indexHolder, cTyp, colIdx)
	require.NoError(t, err)

	metas := make([]*common.IndexMeta, 0)
	for i := 0; i < batchPerBlock; i++ {
		batch := mock.MockVec(typ, rowsPerBatch, rowsPerBatch * i)
		err = zmWriter.AddValues(batch)
		require.NoError(t, err)
		err = sfWriter.AddValues(batch)
	}

	meta, err = zmWriter.Finalize()
	require.NoError(t, err)
	metas = append(metas, meta)
	err = sfWriter.Finish()
	require.NoError(t, err)
	meta ,err = sfWriter.Finalize()
	require.NoError(t, err)
	metas = append(metas, meta)

	zmReader := io.NewBlockZoneMapIndexReader()
	err = zmReader.Init(indexHolder, metas[0])
	require.NoError(t, err)
	sfReader := io.NewStaticFilterIndexReader()
	err = sfReader.Init(indexHolder, metas[1])
	require.NoError(t, err)

	indexHolder.SetZoneMapReader(zmReader)
	indexHolder.SetFilterReader(sfReader)

	query := mock.MockVec(typ, 1000, batchPerBlock * rowsPerBatch)
	res, err = indexHolder.ContainsAnyKeys(query)
	require.NoError(t, err)
	require.False(t, res)

	query = mock.MockVec(typ, 20000, batchPerBlock * rowsPerBatch - 100)
	res, err = indexHolder.ContainsAnyKeys(query)
	require.NoError(t, err)
	require.True(t, res)
}
