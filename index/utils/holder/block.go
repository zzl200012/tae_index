package holder

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"tae/index/access/access_iface"
	"tae/index/common"
	m "tae/mock"
)

type mockAppendableBlockIndexHolder struct {
	host *m.Segment
}

func NewMockAppendableBlockIndexHolder(host *m.Segment) access_iface.IAppendableBlockIndexHolder {
	return &mockAppendableBlockIndexHolder{host: host}
}

func (holder *mockAppendableBlockIndexHolder) Insert(key interface{}, offset uint32) error {
	panic("implement me")
}

func (holder *mockAppendableBlockIndexHolder) BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify bool) error {
	panic("implement me")
}

func (holder *mockAppendableBlockIndexHolder) Delete(key interface{}) error {
	panic("implement me")
}

func (holder *mockAppendableBlockIndexHolder) Search(key interface{}) (uint32, error) {
	panic("implement me")
}

func (holder *mockAppendableBlockIndexHolder) ContainsKey(key interface{}) (bool, error) {
	panic("implement me")
}

func (holder *mockAppendableBlockIndexHolder) ContainsAnyKeys(keys *vector.Vector) (bool, error) {
	panic("implement me")
}

func (holder *mockAppendableBlockIndexHolder) GetBlockId() uint32 {
	panic("implement me")
}

func (holder *mockAppendableBlockIndexHolder) Print() string {
	panic("implement me")
}

func (holder *mockAppendableBlockIndexHolder) Freeze() (access_iface.INonAppendableBlockIndexHolder, error) {
	panic("implement me")
}

type mockNonAppendableBlockIndexHolder struct {
	host *m.Segment
}

func NewMockNonAppendableBlockIndexHolder(host *m.Segment) access_iface.INonAppendableBlockIndexHolder {
	return &mockNonAppendableBlockIndexHolder{host: host}
}

func (holder *mockNonAppendableBlockIndexHolder) GetBufferManager() iface.IBufferManager {
	panic("implement me")
}

func (holder *mockNonAppendableBlockIndexHolder) GetWriter() *m.Part {
	panic("implement me")
}

func (holder *mockNonAppendableBlockIndexHolder) MakeVirtualIndexFile(indexMeta *common.IndexMeta) *common.VirtualIndexFile {
	panic("implement me")
}

func (holder *mockNonAppendableBlockIndexHolder) Search(key interface{}) (uint32, error) {
	panic("implement me")
}

func (holder *mockNonAppendableBlockIndexHolder) ContainsKey(key interface{}) (bool, error) {
	panic("implement me")
}

func (holder *mockNonAppendableBlockIndexHolder) ContainsAnyKeys(keys *vector.Vector) (bool, error) {
	panic("implement me")
}

func (holder *mockNonAppendableBlockIndexHolder) GetBlockId() uint32 {
	panic("implement me")
}

func (holder *mockNonAppendableBlockIndexHolder) Print() string {
	panic("implement me")
}

