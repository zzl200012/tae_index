package access

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"tae/index/common"
	"tae/index/utils/mock/holder"
	"tae/index/utils/mock/io"
	"tae/mock"
	"testing"
)

func TestSegment(t *testing.T) {
	var err error
	var res bool
	//var ans *roaring.Bitmap
	var meta *common.IndexMeta
	typ := types.Type{Oid: types.T_int32}
	cTyp := common.Plain
	colIdx := uint16(0)
	blockCount := 40
	rowsPerBlock := 20000
	segment := mock.NewSegment()
	holder := holder.NewMockNonAppendableSegmentIndexHolder(segment)
	writer := io.NewMockSegmentZoneMapIndexWriter()
	err = writer.Init(holder.GetIndexAppender(), cTyp, colIdx)
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
		writer := io.NewMockStaticFilterIndexWriter()
		err = writer.Init(holder.GetIndexAppender(), cTyp, colIdx)
		require.NoError(t, err)
		err = writer.SetValues(block)
		require.NoError(t, err)
		meta, err = writer.Finalize()
		require.NoError(t, err)
		metas = append(metas, meta)
	}

	//helper.SetZoneMapReader(holder, io.NewMockSegmentZoneMapIndexReader())
	//zoneMapReader := helper.GetZoneMapReader(holder)
	holder.SetZoneMapReader(io.NewMockSegmentZoneMapIndexReader())
	zoneMapReader := holder.GetZoneMapReader()
	err = zoneMapReader.Init(holder.GetHost(), metas[0])
	require.NoError(t, err)

	zoneMapReader.Load()

	//t.Log(holder.zoneMapReader.Print())

	//sfReaders := helper.GetFilterReaders(holder)
	sfReaders := holder.(*NonAppendableSegmentIndexHolder).GetFilterReaders()
	for _, meta := range metas[1:] {
		reader := io.NewMockStaticFilterIndexReader()
		err = reader.Init(holder.GetHost(), meta)
		require.NoError(t, err)
		sfReaders = append(sfReaders, reader)
	}
	//helper.SetFilterReaders(holder, sfReaders)
	holder.(*NonAppendableSegmentIndexHolder).SetFilterReaders(sfReaders)

	batch := mock.MockVec(typ, rowsPerBlock / 2, rowsPerBlock * blockCount)
	res, err = holder.ContainsAnyKeys(batch)
	require.NoError(t, err)
	require.False(t, res)

	batch = mock.MockVec(typ, rowsPerBlock / 2, rowsPerBlock * blockCount - 10)
	res, err = holder.ContainsAnyKeys(batch)
	require.NoError(t, err)
	require.True(t, res)
}

