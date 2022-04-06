package access_iface

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"tae/index/common"
	"tae/index/io/io_iface"
	"tae/mock"
)

type INonAppendableSegmentIndexHolder interface {
	PersistentIndexHolder
	staticPrimaryKeyResolver
	ISegmentIndexHolder
	GetZoneMapReader() io_iface.ISegmentZoneMapIndexReader
	SetZoneMapReader(reader io_iface.ISegmentZoneMapIndexReader)
	GetFilterReaders() []io_iface.IStaticFilterIndexReader
	SetFilterReaders(readers []io_iface.IStaticFilterIndexReader)
}

type INonAppendableBlockIndexHolder interface {
	PersistentIndexHolder
	staticPrimaryKeyResolver
	IBlockIndexHolder
	GetZoneMapReader() io_iface.IBlockZoneMapIndexReader
	SetZoneMapReader(reader io_iface.IBlockZoneMapIndexReader)
	GetFilterReader() io_iface.IStaticFilterIndexReader
	SetFilterReader(readers io_iface.IStaticFilterIndexReader)
}

type IAppendableBlockIndexHolder interface {
	InMemoryIndexHolder
	dynamicPrimaryKeyResolver
	IBlockIndexHolder
	Freeze() (INonAppendableBlockIndexHolder, error)
}

type IAppendableSegmentIndexHolder interface {
	dynamicPrimaryKeyResolver
	ISegmentIndexHolder
	Freeze() (INonAppendableSegmentIndexHolder, error)
}

type PersistentIndexHolder interface {
	GetBufferManager() iface.IBufferManager
	GetIndexAppender() *mock.Part
	MakeVirtualIndexFile(indexMeta *common.IndexMeta) *common.VirtualIndexFile
}

type InMemoryIndexHolder interface {

}

type ISegmentIndexHolder interface {
	GetSegmentId() uint32
	GetHost() *mock.Segment
	Print() string
}

type IBlockIndexHolder interface {
	GetBlockId() uint32
	GetHost() *mock.Segment
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
