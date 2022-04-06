package io

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"tae/index/common"
	"tae/mock"
)

type mockSegmentZoneMapIndexWriter struct {

}

func NewMockSegmentZoneMapIndexWriter() *mockSegmentZoneMapIndexWriter {
	return &mockSegmentZoneMapIndexWriter{}
}

func (writer *mockSegmentZoneMapIndexWriter) Init(appender *mock.Part, cType common.CompressType, colIdx uint16) error {
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

func (writer *mockBlockZoneMapIndexWriter) Init(appender *mock.Part, cType common.CompressType, colIdx uint16) error {
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

func (writer *mockStaticFilterIndexWriter) Init(appender *mock.Part, cType common.CompressType, colIdx uint16) error {
	panic("implement me")
}

func (writer *mockStaticFilterIndexWriter) Finalize() (*common.IndexMeta, error) {
	panic("implement me")
}

func (writer *mockStaticFilterIndexWriter) SetValues(values *vector.Vector) error {
	panic("implement me")
}

