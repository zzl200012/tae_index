package io_iface

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"tae/index/access/access_iface"
	"tae/index/common"
)

type IndexWriter interface {
	Init(holder access_iface.PersistentIndexHolder, cType common.CompressType, colIdx uint16) error
	Finalize() (*common.IndexMeta, error)
}

type IndexReader interface {
	Init(holder access_iface.PersistentIndexHolder, indexMeta *common.IndexMeta) error
	Load() error
	Unload() error
}

type IStaticFilterIndexReader interface {
	IndexReader
	MayContainsKey(key interface{}) (bool, error)
	MayContainsAnyKeys(keys *vector.Vector, visibility *roaring.Bitmap) (*roaring.Bitmap, error)
	Print() string
}

type ISegmentZoneMapIndexReader interface {
	IndexReader
	MayContainsKey(key interface{}) (bool, uint32, error)
	MayContainsAnyKeys(keys *vector.Vector) (bool, []*roaring.Bitmap, error)
	Print() string
}

type IBlockZoneMapIndexReader interface {
	IndexReader
	MayContainsKey(key interface{}) (bool, error)
	MayContainsAnyKeys(keys *vector.Vector) (bool, *roaring.Bitmap, error)
	Print() string
}

type ISegmentZoneMapIndexWriter interface {
	IndexWriter
	AddValues(values *vector.Vector) error
	FinishBlock() error
}

type IBlockZoneMapIndexWriter interface {
	IndexWriter
	AddValues(values *vector.Vector) error
	SetMinMax(min, max interface{}, typ types.Type)
}

type IStaticFilterIndexWriter interface {
	IndexWriter
	SetValues(values *vector.Vector) error
}

type IndexMemNode interface {
	buf.IMemoryNode
}
