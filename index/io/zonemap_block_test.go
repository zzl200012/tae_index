package io

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"tae/index/common"
	"tae/index/utils/mock/holder"
	"tae/mock"
	"testing"
)

func TestBlockZoneMapIndex(t *testing.T) {
	seg := mock.NewSegment()
	var err error
	var res bool
	var meta *common.IndexMeta
	var ans *roaring.Bitmap
	cType := common.Plain
	typ := types.Type{Oid: types.T_int32}
	colIdx := uint16(0)
	writer := BlockZoneMapIndexWriter{}
	indexHolder := holder.MockPersistentIndexHolder(seg)
	err = writer.Init(indexHolder.GetIndexAppender(), cType, colIdx)
	require.NoError(t, err)

	keys := mock.MockVec(typ, 1000, 0)
	err = writer.AddValues(keys)
	require.NoError(t, err)

	meta, err = writer.Finalize()
	require.NoError(t, err)

	reader := BlockZoneMapIndexReader{}
	err = reader.Init(indexHolder.GetHost(), meta)
	require.NoError(t, err)

	err = reader.Load()
	require.NoError(t, err)

	res, err = reader.MayContainsKey(int32(500))
	require.NoError(t, err)
	require.True(t, res)

	res, err = reader.MayContainsKey(int32(1000))
	require.NoError(t, err)
	require.False(t, res)

	keys = mock.MockVec(typ, 100, 1000)
	res, ans, err = reader.MayContainsAnyKeys(keys)
	require.NoError(t, err)
	require.False(t, res)
	require.Equal(t, uint64(0), ans.GetCardinality())

	keys = mock.MockVec(typ, 100, 0)
	res, ans, err = reader.MayContainsAnyKeys(keys)
	require.NoError(t, err)
	require.True(t, res)
	require.Equal(t, uint64(100), ans.GetCardinality())

	t.Log(indexHolder.GetBufferManager().String())

	err = reader.Unload()
	require.NoError(t, err)

	t.Log(indexHolder.GetBufferManager().String())
}
