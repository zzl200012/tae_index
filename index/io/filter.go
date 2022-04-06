package io

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	bufferInterface "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	comm "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/sirupsen/logrus"
	"io"
	"tae/index/basic"
	"tae/index/common"
	"tae/index/io/io_iface"
	"tae/mock"
)

type StaticFilterIndexWriter struct {
	cType  common.CompressType
	//holder access_iface.PersistentIndexHolder
	appender *mock.Part
	inner  basic.StaticFilter
	colIdx uint16
}

func NewStaticFilterIndexWriter() io_iface.IStaticFilterIndexWriter {
	return &StaticFilterIndexWriter{}
}

func (writer *StaticFilterIndexWriter) Init(appender *mock.Part, cType common.CompressType, colIdx uint16) error {
	//writer.holder = holder
	writer.appender = appender
	writer.cType = cType
	writer.colIdx = colIdx
	return nil
}

func (writer *StaticFilterIndexWriter) Finalize() (*common.IndexMeta, error) {
	if writer.inner == nil {
		panic("unexpected error")
	}
	appender := writer.appender
	meta := common.NewEmptyIndexMeta()
	meta.SetIndexType(common.StaticFilterIndex)
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
	//logrus.Info(writer.inner.GetMemoryUsage())
	return meta, nil
}

func (writer *StaticFilterIndexWriter) SetValues(values *vector.Vector) error {
	if writer.inner != nil {
		panic("updating immutable filter")
	}
	sf, err := basic.NewBinaryFuseFilter(values)
	if err != nil {
		return err
	}
	writer.inner = sf
	return nil
}

func (writer *StaticFilterIndexWriter) Query(key interface{}) (bool, error) {
	logrus.Info(writer.inner.Print())
	return writer.inner.MayContainsKey(key)
}

type StaticFilterIndexReader struct {
	handle *common.IndexBufferNode
	inner  bufferInterface.MangaedNode
}

func NewStaticFilterIndexReader() io_iface.IStaticFilterIndexReader {
	return &StaticFilterIndexReader{}
}

func (reader *StaticFilterIndexReader) Init(host *mock.Segment, indexMeta *common.IndexMeta) error {
	//bufferManager := holder.GetBufferManager()
	//vFile := holder.MakeVirtualIndexFile(indexMeta)
	bufferManager := host.FetchBufferManager()
	vFile := common.MakeVirtualIndexFile(host, indexMeta)
	reader.handle = common.NewIndexBufferNode(bufferManager, vFile, indexMeta.CompType != common.Plain, StaticFilterIndexConstructor)
	return nil
}

func (reader *StaticFilterIndexReader) Load() error {
	if reader.inner.DataNode != nil {
		return nil
	}
	reader.inner = reader.handle.GetInnerNode()
	//logrus.Info(reader.inner.DataNode.(*StaticFilterIndexMemNode).inner.GetMemoryUsage())
	return nil
}

func (reader *StaticFilterIndexReader) Unload() error {
	if reader.inner.DataNode == nil {
		return nil
	}
	err := reader.inner.Close()
	return err
}

func (reader *StaticFilterIndexReader) MayContainsKey(key interface{}) (bool, error) {
	//logrus.Infof("%s", reader.inner.DataNode.(*StaticFilterIndexMemNode).inner.Print())
	return reader.inner.DataNode.(*StaticFilterIndexMemNode).inner.MayContainsKey(key)
}

func (reader *StaticFilterIndexReader) MayContainsAnyKeys(keys *vector.Vector, visibility *roaring.Bitmap) (*roaring.Bitmap, error) {
	return reader.inner.DataNode.(*StaticFilterIndexMemNode).inner.MayContainsAnyKeys(keys, visibility)
}

func (reader *StaticFilterIndexReader) Print() string {
	reader.Load()
	defer reader.Unload()
	return reader.inner.DataNode.(*StaticFilterIndexMemNode).inner.Print()
}

func StaticFilterIndexConstructor(vf comm.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewStaticFilterEmptyNode(vf, useCompress, freeFunc)
}

func NewStaticFilterEmptyNode(vf comm.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) *StaticFilterIndexMemNode {
	return &StaticFilterIndexMemNode{
		FreeFunc:    freeFunc,
		File:        vf,
		UseCompress: useCompress,
	}
}

type StaticFilterIndexMemNode struct {
	FreeFunc buf.MemoryFreeFunc
	File comm.IVFile
	UseCompress bool
	inner basic.StaticFilter
}

func (node *StaticFilterIndexMemNode) ReadFrom(r io.Reader) (n int64, err error) {
	buffer := make([]byte, node.GetMemoryCapacity())
	nr, err := r.Read(buffer)
	if err != nil {
		return 0, err
	}
	err = node.Unmarshal(buffer)
	return int64(nr), err
}

func (node *StaticFilterIndexMemNode) WriteTo(w io.Writer) (n int64, err error) {
	buffer, err := node.Marshal()
	if err != nil {
		return 0, err
	}
	nw, err := w.Write(buffer)
	return int64(nw), err
}

func (node *StaticFilterIndexMemNode) Marshal() ([]byte, error) {
	return node.inner.Marshal()
}

func (node *StaticFilterIndexMemNode) Unmarshal(data []byte) error {
	node.inner = basic.GetEmptyFilter()
	err := node.inner.Unmarshal(data)
	return err
}

func (node *StaticFilterIndexMemNode) FreeMemory() {
	if node.FreeFunc != nil {
		node.FreeFunc(node)
	}
}

func (node *StaticFilterIndexMemNode) Reset() {
	// no-op
}

func (node *StaticFilterIndexMemNode) GetMemorySize() uint64 {
	if node.UseCompress {
		return uint64(node.File.Stat().Size())
	} else {
		return uint64(node.File.Stat().OriginSize())
	}
}

func (node *StaticFilterIndexMemNode) GetMemoryCapacity() uint64 {
	if node.UseCompress {
		return uint64(node.File.Stat().Size())
	} else {
		return uint64(node.File.Stat().OriginSize())
	}
}
