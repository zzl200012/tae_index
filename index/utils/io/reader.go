package io

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"tae/index/access/access_iface"
	"tae/index/common"
)

type mockSegmentZoneMapIndexReader struct {

}

func NewMockSegmentZoneMapIndexReader() *mockSegmentZoneMapIndexReader {
	return &mockSegmentZoneMapIndexReader{}
}

func (reader *mockSegmentZoneMapIndexReader) Init(holder access_iface.PersistentIndexHolder, indexMeta *common.IndexMeta) error {
	panic("implement me")
}

func (reader *mockSegmentZoneMapIndexReader) Load() error {
	panic("implement me")
}

func (reader *mockSegmentZoneMapIndexReader) Unload() error {
	panic("implement me")
}

func (reader *mockSegmentZoneMapIndexReader) MayContainsKey(key interface{}) (bool, uint32, error) {
	panic("implement me")
}

func (reader *mockSegmentZoneMapIndexReader) MayContainsAnyKeys(keys *vector.Vector) (bool, []*roaring.Bitmap, error) {
	panic("implement me")
}

func (reader *mockSegmentZoneMapIndexReader) Print() string {
	panic("implement me")
}

type mockBlockZoneMapIndexReader struct {

}

func NewMockBlockZoneMapIndexReader() *mockBlockZoneMapIndexReader {
	return &mockBlockZoneMapIndexReader{}
}

func (reader *mockBlockZoneMapIndexReader) Init(holder access_iface.PersistentIndexHolder, indexMeta *common.IndexMeta) error {
	panic("implement me")
}

func (reader *mockBlockZoneMapIndexReader) Load() error {
	panic("implement me")
}

func (reader *mockBlockZoneMapIndexReader) Unload() error {
	panic("implement me")
}

func (reader *mockBlockZoneMapIndexReader) MayContainsKey(key interface{}) (bool, error) {
	panic("implement me")
}

func (reader *mockBlockZoneMapIndexReader) MayContainsAnyKeys(keys *vector.Vector) (bool, *roaring.Bitmap, error) {
	panic("implement me")
}

func (reader *mockBlockZoneMapIndexReader) Print() string {
	panic("implement me")
}

type mockStaticFilterIndexReader struct {

}

func NewMockStaticFilterIndexReader() *mockStaticFilterIndexReader {
	return &mockStaticFilterIndexReader{}
}

func (reader *mockStaticFilterIndexReader) Init(holder access_iface.PersistentIndexHolder, indexMeta *common.IndexMeta) error {
	panic("implement me")
}

func (reader *mockStaticFilterIndexReader) Load() error {
	panic("implement me")
}

func (reader *mockStaticFilterIndexReader) Unload() error {
	panic("implement me")
}

func (reader *mockStaticFilterIndexReader) MayContainsKey(key interface{}) (bool, error) {
	panic("implement me")
}

func (reader *mockStaticFilterIndexReader) MayContainsAnyKeys(keys *vector.Vector, visibility *roaring.Bitmap) (*roaring.Bitmap, error) {
	panic("implement me")
}

func (reader *mockStaticFilterIndexReader) Print() string {
	panic("implement me")
}

