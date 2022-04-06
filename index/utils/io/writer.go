package io

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"tae/index/access/access_iface"
	"tae/index/common"
)

type mockSegmentZoneMapIndexWriter struct {

}

func NewMockSegmentZoneMapIndexWriter() *mockSegmentZoneMapIndexWriter {
	return &mockSegmentZoneMapIndexWriter{}
}

func (writer *mockSegmentZoneMapIndexWriter) Init(holder access_iface.PersistentIndexHolder, cType common.CompressType, colIdx uint16) error {
	panic("implement me")
}

func (writer *mockSegmentZoneMapIndexWriter) Finalize() (*common.IndexMeta, error) {
	panic("implement me")
}

func (writer *mockSegmentZoneMapIndexWriter) AddValues(values *vector.Vector) error {
	panic("implement me")
}

func (writer *mockSegmentZoneMapIndexWriter) FinishBlock() error {
	panic("implement me")
}

type mockBlockZoneMapIndexWriter struct {

}

func NewMockBlockZoneMapIndexWriter() *mockBlockZoneMapIndexWriter {
	return &mockBlockZoneMapIndexWriter{}
}

func (writer *mockBlockZoneMapIndexWriter) Init(holder access_iface.PersistentIndexHolder, cType common.CompressType, colIdx uint16) error {
	panic("implement me")
}

func (writer *mockBlockZoneMapIndexWriter) Finalize() (*common.IndexMeta, error) {
	panic("implement me")
}

func (writer *mockBlockZoneMapIndexWriter) AddValues(values *vector.Vector) error {
	panic("implement me")
}

type mockStaticFilterIndexWriter struct {

}

func NewMockStaticFilterIndexWriter() *mockStaticFilterIndexWriter {
	return &mockStaticFilterIndexWriter{}
}

func (writer *mockStaticFilterIndexWriter) Init(holder access_iface.PersistentIndexHolder, cType common.CompressType, colIdx uint16) error {
	panic("implement me")
}

func (writer *mockStaticFilterIndexWriter) Finalize() (*common.IndexMeta, error) {
	panic("implement me")
}

func (writer *mockStaticFilterIndexWriter) SetValues(values *vector.Vector) error {
	panic("implement me")
}

