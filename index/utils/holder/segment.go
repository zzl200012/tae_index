package holder

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"tae/index/access/access_iface"
	"tae/index/common"
	m "tae/mock"
)

type mockAppendableSegmentIndexHolder struct {
	host *m.Segment
}

func NewMockAppendableSegmentIndexHolder(host *m.Segment) access_iface.IAppendableSegmentIndexHolder {
	return &mockAppendableSegmentIndexHolder{host: host}
}

func (holder *mockAppendableSegmentIndexHolder) Insert(key interface{}, offset uint32) error {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify bool) error {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) Delete(key interface{}) error {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) Search(key interface{}) (uint32, error) {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) ContainsKey(key interface{}) (bool, error) {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) ContainsAnyKeys(keys *vector.Vector) (bool, error) {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) GetSegmentId() uint32 {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) Print() string {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) Freeze() (access_iface.INonAppendableSegmentIndexHolder, error) {
	panic("implement me")
}

type mockNonAppendableSegmentIndexHolder struct {
	host *m.Segment
}

func NewMockNonAppendableSegmentIndexHolder(host *m.Segment) access_iface.INonAppendableSegmentIndexHolder {
	return &mockNonAppendableSegmentIndexHolder{host: host}
}

func (holder *mockNonAppendableSegmentIndexHolder) GetBufferManager() iface.IBufferManager {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) GetWriter() *m.Part {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) MakeVirtualIndexFile(indexMeta *common.IndexMeta) *common.VirtualIndexFile {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) Search(key interface{}) (uint32, error) {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) ContainsKey(key interface{}) (bool, error) {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) ContainsAnyKeys(keys *vector.Vector) (bool, error) {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) GetSegmentId() uint32 {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) Print() string {
	panic("implement me")
}

