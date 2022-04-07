package io

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	mock2 "tae/index/access/mock"
	"tae/index/common"
	"tae/mock"
	"testing"
)

func TestStaticFilterIndex(t *testing.T) {
	seg := mock.NewSegment()
	var err error
	var res bool
	var ans *roaring.Bitmap
	var meta *common.IndexMeta
	cType := common.Plain
	typ := types.Type{Oid: types.T_int32}
	colIdx := uint16(0)
	writer := StaticFilterIndexWriter{}
	indexHolder := mock2.NewMockPersistentIndexHolder(seg)
	err = writer.Init(indexHolder, cType, colIdx)
	require.NoError(t, err)

	keys := mock.MockVec(typ, 1000, 0)
	err = writer.AddValues(keys)
	require.NoError(t, err)
	err = writer.Finish()
	require.NoError(t, err)

	//t.Log(writer.inner.Print())

	meta, err = writer.Finalize()
	require.NoError(t, err)

	reader := StaticFilterIndexReader{}
	err = reader.Init(indexHolder, meta)
	require.NoError(t, err)

	err = reader.Load()
	require.NoError(t, err)

	//t.Log(reader.Print())

	res, err = reader.MayContainsKey(int32(500))
	require.NoError(t, err)
	require.True(t, res)

	res, err = reader.MayContainsKey(int32(2000))
	require.NoError(t, err)
	require.False(t, res)

	query := mock.MockVec(typ, 1000, 1500)
	ans, err = reader.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	require.True(t, ans.GetCardinality() < uint64(10))
	//t.Log(ans.GetCardinality())

	err = reader.Unload()
	require.NoError(t, err)
}
