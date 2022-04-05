package basic

import (
	"github.com/RoaringBitmap/roaring"
	bloom2 "github.com/bits-and-blooms/bloom/v3"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"strconv"
	"tae/mock"
	"testing"
)

func TestStaticFilterNumeric(t *testing.T) {
	typ := types.Type{Oid: types.T_int32}
	data := mock.MockVec(typ, 40000, 0)
	sf, err := NewBinaryFuseFilter(data)
	require.NoError(t, err)
	var res bool
	var positive *roaring.Bitmap

	res, err = sf.MayContainsKey(int32(1209))
	require.NoError(t, err)
	require.True(t, res)

	res, err = sf.MayContainsKey(int32(5555))
	require.NoError(t, err)
	require.True(t, res)

	res, err = sf.MayContainsKey(int32(40000))
	require.NoError(t, err)
	require.False(t, res)

	res, err = sf.MayContainsKey(int16(0))
	require.ErrorIs(t, err, mock.ErrTypeMismatch)

	query := mock.MockVec(typ, 2000, 1000)
	positive, err = sf.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2000), positive.GetCardinality())

	visibility := roaring.NewBitmap()
	visibility.AddRange(uint64(0), uint64(1000))
	positive, err = sf.MayContainsAnyKeys(query, visibility)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), positive.GetCardinality())

	query = mock.MockVec(typ, 20000, 40000)
	positive, err = sf.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	fpRate := float32(positive.GetCardinality()) / float32(20000)
	require.True(t, fpRate < float32(0.01))

	var buf []byte
	buf, err = sf.Marshal()
	require.NoError(t, err)

	sf1, err := NewBinaryFuseFilter(mock.MockVec(typ, 0, 0))
	require.NoError(t, err)
	err = sf1.Unmarshal(buf)
	require.NoError(t, err)

	query = mock.MockVec(typ, 40000, 0)
	positive, err = sf.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(40000), positive.GetCardinality())
}

func TestStaticFilterString(t *testing.T) {
	typ := types.Type{Oid: types.T_varchar}
	data := mock.MockVec(typ, 40000, 0)
	sf, err := NewBinaryFuseFilter(data)
	require.NoError(t, err)
	var res bool
	var positive *roaring.Bitmap

	res, err = sf.MayContainsKey([]byte(strconv.Itoa(1209)))
	require.NoError(t, err)
	require.True(t, res)

	res, err = sf.MayContainsKey([]byte(strconv.Itoa(40000)))
	require.NoError(t, err)
	require.False(t, res)

	query := mock.MockVec(typ, 2000, 1000)
	positive, err = sf.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2000), positive.GetCardinality())

	query = mock.MockVec(typ, 20000, 40000)
	positive, err = sf.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	fpRate := float32(positive.GetCardinality()) / float32(20000)
	require.True(t, fpRate < float32(0.01))

	var buf []byte
	buf, err = sf.Marshal()
	require.NoError(t, err)

	sf1, err := NewBinaryFuseFilter(mock.MockVec(typ, 0, 0))
	require.NoError(t, err)
	err = sf1.Unmarshal(buf)
	require.NoError(t, err)

	query = mock.MockVec(typ, 40000, 0)
	positive, err = sf.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(40000), positive.GetCardinality())
}


func TestBloom(t *testing.T) {
	bloom := bloom2.NewWithEstimates(400000, 0.00001)
	for i := 0; i < 400000; i++ {
		bloom.Add([]byte(strconv.Itoa(i)))
	}
	batch := make([][]byte, 0)
	for i := 400000; i < 600000; i++ {
		batch = append(batch, []byte(strconv.Itoa(i)))
	}

	total := float32(200000)
	pos := float32(0)
	for _, e := range batch {
		if bloom.Test(e) {
			pos++
			t.Log(string(e))
		}
	}
	t.Log(pos / total)
}