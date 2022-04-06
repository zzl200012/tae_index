package holder

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"tae/index/access/access_iface"
	"tae/index/common"
	m "tae/mock"
)

type mockPersistentIndexHolder struct {
	host *m.Segment
}

func MockPersistentIndexHolder(host *m.Segment) access_iface.PersistentIndexHolder {
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
