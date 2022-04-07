package mock

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"tae/index/common"
	m "tae/mock"
)

type mockPersistentIndexHolder struct {
	host *m.Resource
}

func (holder *mockPersistentIndexHolder) GetHost() *m.Resource {
	panic("implement me")
}

func NewMockPersistentIndexHolder(host *m.Resource) *mockPersistentIndexHolder {
	return &mockPersistentIndexHolder{host: host}
}

func (holder *mockPersistentIndexHolder) GetBufferManager() iface.IBufferManager {
	return holder.host.FetchBufferManager()
}

func (holder *mockPersistentIndexHolder) GetIndexAppender() *m.Part {
	return holder.host.FetchIndexWriter()
}

func (holder *mockPersistentIndexHolder) MakeVirtualIndexFile(indexMeta *common.IndexMeta) *common.VirtualIndexFile {
	return common.MakeVirtualIndexFile(holder.host, indexMeta)
}
