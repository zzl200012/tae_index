package io

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	comm "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"io"
	"tae/index/access/access_iface"
	"tae/index/basic"
	"tae/index/common"
	"tae/mock"
)

type BlockZoneMapIndexWriter struct {
	cType  common.CompressType
	holder access_iface.PersistentIndexHolder
	inner  *basic.ZoneMap
	colIdx uint16
}

func (writer *BlockZoneMapIndexWriter) Init(holder access_iface.PersistentIndexHolder, cType common.CompressType, colIdx uint16) error {
	writer.holder = holder
	writer.cType = cType
	writer.colIdx = colIdx
	return nil
}

func (writer *BlockZoneMapIndexWriter) Finalize() (*common.IndexMeta, error) {
	if writer.inner == nil {
		panic("unexpected error")
	}
	appender := writer.holder.GetWriter()
	meta := common.NewEmptyIndexMeta()
	meta.SetIndexType(common.BlockZoneMapIndex)
	meta.SetCompressType(writer.cType)
	meta.SetIndexedColumn(writer.colIdx)
	partOffset := appender.GetOffset()
	meta.SetPartOffset(partOffset)
	startOffset := appender.SeekCurrentOffset()
	meta.SetStartOffset(startOffset)
	buffer, err := writer.inner.Marshal()
	if err != nil {
		return nil, err
	}
	rawSize := uint32(len(buffer))
	compressed := common.Compress(buffer, writer.cType)
	exactSize := uint32(len(compressed))
	meta.SetSize(rawSize, exactSize)
	if _, err = appender.Write(compressed); err != nil {
		return nil, err
	}
	return meta, nil
}

func (writer *BlockZoneMapIndexWriter) AddValues(values *vector.Vector) error {
	typ := values.Typ
	if writer.inner == nil {
		writer.inner = basic.NewZoneMap(typ, nil)
	} else {
		if writer.inner.GetType() != typ {
			return mock.ErrTypeMismatch
		}
	}
	if err := writer.inner.BatchUpdate(values); err != nil {
		return err
	}
	return nil
}

type BlockZoneMapIndexReader struct {
	handle *common.IndexBufferNode
	inner  iface.MangaedNode
}

func (reader *BlockZoneMapIndexReader) Init(holder access_iface.PersistentIndexHolder, indexMeta *common.IndexMeta) error {
	bufferManager := holder.GetBufferManager()
	vFile := holder.MakeVirtualIndexFile(indexMeta)
	reader.handle = common.NewIndexBufferNode(bufferManager, vFile, indexMeta.CompType != common.Plain, BlockZoneMapIndexConstructor)
	return nil
}

func (reader *BlockZoneMapIndexReader) Load() error {
	if reader.inner.DataNode != nil {
		return nil
	}
	reader.inner = reader.handle.GetInnerNode()
	return nil
}

func (reader *BlockZoneMapIndexReader) Unload() error {
	if reader.inner.DataNode == nil {
		return nil
	}
	err := reader.inner.Close()
	return err
}

func (reader *BlockZoneMapIndexReader) MayContainsKey(key interface{}) (bool, error) {
	return reader.inner.DataNode.(*BlockZoneMapIndexMemNode).inner.MayContainsKey(key)
}

func (reader *BlockZoneMapIndexReader) MayContainsAnyKeys(keys *vector.Vector) (bool, *roaring.Bitmap, error) {
	return reader.inner.DataNode.(*BlockZoneMapIndexMemNode).inner.MayContainsAnyKeys(keys)
}

func (reader *BlockZoneMapIndexReader) Print() string {
	panic("todo")
}

func BlockZoneMapIndexConstructor(vf comm.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewBlockZoneMapEmptyNode(vf, useCompress, freeFunc)
}

func NewBlockZoneMapEmptyNode(vf comm.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) *BlockZoneMapIndexMemNode {
	return &BlockZoneMapIndexMemNode{
		FreeFunc:    freeFunc,
		File:        vf,
		UseCompress: useCompress,
	}
}

type BlockZoneMapIndexMemNode struct {
	FreeFunc buf.MemoryFreeFunc
	File comm.IVFile
	UseCompress bool
	inner *basic.ZoneMap
}

func (node *BlockZoneMapIndexMemNode) ReadFrom(r io.Reader) (n int64, err error) {
	buffer := make([]byte, node.GetMemoryCapacity())
	nr, err := r.Read(buffer)
	if err != nil {
		return 0, err
	}
	err = node.Unmarshal(buffer)
	return int64(nr), err
}

func (node *BlockZoneMapIndexMemNode) WriteTo(w io.Writer) (n int64, err error) {
	buffer, err := node.Marshal()
	if err != nil {
		return 0, err
	}
	nw, err := w.Write(buffer)
	return int64(nw), err
}

func (node *BlockZoneMapIndexMemNode) Marshal() ([]byte, error) {
	return node.inner.Marshal()
}

func (node *BlockZoneMapIndexMemNode) Unmarshal(data []byte) error {
	node.inner = &basic.ZoneMap{}
	err := node.inner.Unmarshal(data)
	return err
}

func (node *BlockZoneMapIndexMemNode) FreeMemory() {
	if node.FreeFunc != nil {
		node.FreeFunc(node)
	}
}

func (node *BlockZoneMapIndexMemNode) Reset() {
	// no-op
}

func (node *BlockZoneMapIndexMemNode) GetMemorySize() uint64 {
	if node.UseCompress {
		return uint64(node.File.Stat().Size())
	} else {
		return uint64(node.File.Stat().OriginSize())
	}
}

func (node *BlockZoneMapIndexMemNode) GetMemoryCapacity() uint64 {
	if node.UseCompress {
		return uint64(node.File.Stat().Size())
	} else {
		return uint64(node.File.Stat().OriginSize())
	}
}