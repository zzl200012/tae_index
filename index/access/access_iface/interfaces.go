package access_iface

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"tae/index/common"
	"tae/mock"
)

type ITableIndexHolder interface {
	dynamicPrimaryKeyResolver
	GetTableId() uint32
	GetHost() *mock.Resource
	Print() string
}

type INonAppendableSegmentIndexHolder interface {
	PersistentIndexHolder
	staticPrimaryKeyResolver
	ISegmentIndexHolder
}

type INonAppendableBlockIndexHolder interface {
	PersistentIndexHolder
	staticPrimaryKeyResolver
	IBlockIndexHolder
}

type IAppendableBlockIndexHolder interface {
	InMemoryIndexHolder
	dynamicPrimaryKeyResolver
	IBlockIndexHolder
	Upgrade() (INonAppendableBlockIndexHolder, error)
}

type IAppendableSegmentIndexHolder interface {
	dynamicPrimaryKeyResolver
	ISegmentIndexHolder
	Upgrade() (INonAppendableSegmentIndexHolder, error)
}

type PersistentIndexHolder interface {
	GetBufferManager() iface.IBufferManager
	GetIndexAppender() *mock.Part
	GetHost() *mock.Resource
	MakeVirtualIndexFile(indexMeta *common.IndexMeta) *common.VirtualIndexFile
}

type InMemoryIndexHolder interface {

}

type ISegmentIndexHolder interface {
	GetSegmentId() uint32
	GetHost() *mock.Resource
	Print() string
}

type IBlockIndexHolder interface {
	GetBlockId() uint32
	GetHost() *mock.Resource
	Print() string
}

type dynamicPrimaryKeyResolver interface {
	Insert(key interface{}, offset uint32) error
	BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify bool) error
	Delete(key interface{}) error
	staticPrimaryKeyResolver
}

type staticPrimaryKeyResolver interface {
	Search(key interface{}) (uint32, error)
	ContainsKey(key interface{}) (bool, error)
	ContainsAnyKeys(keys *vector.Vector) (bool, error)
}
