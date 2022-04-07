package access

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"tae/index/common"
	"tae/index/io"
	"tae/mock"
	"testing"
)

func TestSegment(t *testing.T) {
	var err error
	var res bool
	var meta *common.IndexMeta
	typ := types.Type{Oid: types.T_int32}
	cTyp := common.Plain
	colIdx := uint16(0)
	blockCount := 40
	rowsPerBlock := 20000
	segment := mock.NewSegment()
	indexHolder := NewNonAppendableSegmentIndexHolder(segment)
	writer := io.NewSegmentZoneMapIndexWriter()
	err = writer.Init(indexHolder, cTyp, colIdx)
	require.NoError(t, err)

	var metas []*common.IndexMeta

	var blocks []*vector.Vector
	for i := 0; i < blockCount; i++ {
		data := mock.MockVec(typ, rowsPerBlock, i * rowsPerBlock)
		blocks = append(blocks, data)
	}

	for _, block := range blocks {
		err = writer.AddValues(block)
		require.NoError(t, err)
		err = writer.FinishBlock()
		require.NoError(t, err)
	}

	meta, err = writer.Finalize()
	metas = append(metas, meta)

	for _, block := range blocks {
		writer := io.NewStaticFilterIndexWriter()
		err = writer.Init(indexHolder, cTyp, colIdx)
		require.NoError(t, err)
		err = writer.AddValues(block)
		require.NoError(t, err)
		err = writer.Finish()
		require.NoError(t, err)
		meta, err = writer.Finalize()
		require.NoError(t, err)
		metas = append(metas, meta)
	}

	indexHolder.SetZoneMapReader(io.NewSegmentZoneMapIndexReader())
	zoneMapReader := indexHolder.GetZoneMapReader()
	err = zoneMapReader.Init(indexHolder, metas[0])
	require.NoError(t, err)

	zoneMapReader.Load()

	sfReaders := indexHolder.GetFilterReaders()
	for _, meta := range metas[1:] {

		reader := io.NewStaticFilterIndexReader()
		err = reader.Init(indexHolder, meta)
		require.NoError(t, err)
		sfReaders = append(sfReaders, reader)
	}
	indexHolder.SetFilterReaders(sfReaders)

	batch := mock.MockVec(typ, rowsPerBlock / 2, rowsPerBlock * blockCount)
	res, err = indexHolder.ContainsAnyKeys(batch)
	require.NoError(t, err)
	require.False(t, res)

	batch = mock.MockVec(typ, rowsPerBlock / 2, rowsPerBlock * blockCount - 10)
	res, err = indexHolder.ContainsAnyKeys(batch)
	require.NoError(t, err)
	require.True(t, res)
}

