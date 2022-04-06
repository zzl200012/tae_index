package io

import (
	"bytes"
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	comm "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"io"
	"tae/index/basic"
	"tae/index/common"
	"tae/index/io/io_iface"
	"tae/mock"
)

type SegmentZoneMapIndexWriter struct {
	cType          common.CompressType
	//holder         access_iface.PersistentIndexHolder
	appender *mock.Part
	segmentZoneMap *basic.ZoneMap
	blockZoneMap *basic.ZoneMap
	blockZoneMapBuffer [][]byte
	colIdx uint16
}

func NewSegmentZoneMapIndexWriter() io_iface.ISegmentZoneMapIndexWriter {
	return &SegmentZoneMapIndexWriter{}
}

func (writer *SegmentZoneMapIndexWriter) Init(appender *mock.Part, cType common.CompressType, colIdx uint16) error {
	//writer.holder = holder
	writer.appender = appender
	writer.cType = cType
	writer.colIdx = colIdx
	return nil
}

func (writer *SegmentZoneMapIndexWriter) Finalize() (*common.IndexMeta, error) {
	if writer.segmentZoneMap == nil {
		panic("unexpected error")
	}
	if writer.blockZoneMap.Initialized() {
		if err := writer.FinishBlock(); err != nil {
			return nil, err
		}
	}
	appender := writer.appender
	meta := common.NewEmptyIndexMeta()
	meta.SetIndexType(common.SegmentZoneMapIndex)
	meta.SetCompressType(writer.cType)
	meta.SetIndexedColumn(writer.colIdx)
	partOffset := appender.GetOffset()
	meta.SetPartOffset(partOffset)
	startOffset := appender.SeekCurrentOffset()
	meta.SetStartOffset(startOffset)
	segBuffer, err := writer.segmentZoneMap.Marshal()
	if err != nil {
		return nil, err
	}
	blockCount := uint32(len(writer.blockZoneMapBuffer))
	var buffer bytes.Buffer
	buffer.Write(encoding.EncodeUint32(blockCount))
	for _, blockBuf := range writer.blockZoneMapBuffer {
		buffer.Write(encoding.EncodeUint16(uint16(len(blockBuf))))
		buffer.Write(blockBuf)
	}
	buffer.Write(encoding.EncodeUint16(uint16(len(segBuffer))))
	buffer.Write(segBuffer)
	finalBuf := buffer.Bytes()
	rawSize := uint32(len(finalBuf))
	compressed := common.Compress(finalBuf, writer.cType)
	exactSize := uint32(len(compressed))
	meta.SetSize(rawSize, exactSize)
	if _, err = appender.Write(compressed); err != nil {
		return nil, err
	}
	return meta, nil
}

func (writer *SegmentZoneMapIndexWriter) AddValues(values *vector.Vector) error {
	typ := values.Typ
	if writer.blockZoneMap == nil {
		writer.blockZoneMap = basic.NewZoneMap(typ, nil)
		writer.segmentZoneMap = basic.NewZoneMap(typ, nil)
	} else {
		if writer.blockZoneMap.GetType() != typ {
			return mock.ErrTypeMismatch
		}
	}
	if err := writer.blockZoneMap.BatchUpdate(values); err != nil {
		return err
	}
	return nil
}

func (writer *SegmentZoneMapIndexWriter) FinishBlock() error {
	if writer.blockZoneMap == nil {
		panic("unexpected error")
	}
	buffer, err := writer.blockZoneMap.Marshal()
	if err != nil {
		return err
	}
	writer.blockZoneMapBuffer = append(writer.blockZoneMapBuffer, buffer)
	if err = writer.segmentZoneMap.Update(writer.blockZoneMap.GetMax()); err != nil {
		return err
	}
	if err = writer.segmentZoneMap.Update(writer.blockZoneMap.GetMin()); err != nil {
		return err
	}
	writer.blockZoneMap = basic.NewZoneMap(writer.segmentZoneMap.GetType(), nil)
	return nil
}

type SegmentZoneMapIndexReader struct {
	handle *common.IndexBufferNode
	inner  iface.MangaedNode
}

func NewSegmentZoneMapIndexReader() io_iface.ISegmentZoneMapIndexReader {
	return &SegmentZoneMapIndexReader{}
}

func (reader *SegmentZoneMapIndexReader) Init(host *mock.Segment, indexMeta *common.IndexMeta) error {
	//bufferManager := holder.GetBufferManager()
	//vFile := holder.MakeVirtualIndexFile(indexMeta)
	bufferManager := host.FetchBufferManager()
	vFile := common.MakeVirtualIndexFile(host, indexMeta)
	reader.handle = common.NewIndexBufferNode(bufferManager, vFile, indexMeta.CompType != common.Plain, SegmentZoneMapIndexConstructor)
	return nil
}

func (reader *SegmentZoneMapIndexReader) Load() error {
	if reader.inner.DataNode != nil {
		return nil
	}
	reader.inner = reader.handle.GetInnerNode()
	return nil
}

func (reader *SegmentZoneMapIndexReader) Unload() error {
	if reader.inner.DataNode == nil {
		return nil
	}
	err := reader.inner.Close()
	return err
}

func (reader *SegmentZoneMapIndexReader) MayContainsKey(key interface{}) (bool, uint32, error) {
	node := reader.inner.DataNode.(*SegmentZoneMapIndexMemNode)
	var res bool
	var err error
	if res, err = node.segmentZoneMap.MayContainsKey(key); err != nil {
		return false, 0, err
	}
	if !res {
		return false, 0, nil
	}
	var ans int
	start, end := uint32(0), uint32(len(node.blockZoneMaps) - 1)
	for start <= end {
		mid := start + (end - start) / 2
		blockZoneMap := node.blockZoneMaps[mid]
		if ans, err = blockZoneMap.Query(key); err != nil {
			return false, 0, err
		}
		if ans == 0 {
			return true, mid, nil
		}
		if ans > 0 {
			start = mid + 1
			continue
		}
		if ans < 0 {
			end = mid - 1
			continue
		}
	}
	return false, 0, nil
}

func (reader *SegmentZoneMapIndexReader) MayContainsAnyKeys(keys *vector.Vector) (bool, []*roaring.Bitmap, error) {
	var ans []*roaring.Bitmap
	var res bool
	var err error
	node := reader.inner.DataNode.(*SegmentZoneMapIndexMemNode)
	for i := 0; i < len(node.blockZoneMaps); i++ {
		ans = append(ans, nil)
	}
	//logrus.Info(node.segmentZoneMap.GetMax(), " ", node.segmentZoneMap.GetMin())
	row := uint32(0)
	process := func(key interface{}) error {
		if res, err = node.segmentZoneMap.MayContainsKey(key); err != nil {
			return err
		}
		if res {
			deeper, blockOffset, err := reader.MayContainsKey(key)
			if err != nil {
				return err
			}
			if deeper {
				if ans[blockOffset] == nil {
					ans[blockOffset] = roaring.NewBitmap()
				}
				ans[blockOffset].Add(row)
			}
		}
		row++
		return nil
	}
	err = mock.ProcessVector(keys, process, nil)
	if err != nil {
		return false, nil, err
	}
	for _, v := range ans {
		if v != nil {
			return true, ans, nil
		}
	}
	return false, nil, nil
}

func (reader *SegmentZoneMapIndexReader) Print() string {
	reader.Load()
	s := "<SEG_ZM_READER>"
	node := reader.inner.DataNode.(*SegmentZoneMapIndexMemNode)
	s += node.segmentZoneMap.Print()
	s += "\n"
	for _, blk := range node.blockZoneMaps {
		s += blk.Print()
		s += "\n"
	}
	reader.Unload()
	return s
}

func SegmentZoneMapIndexConstructor(vf comm.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewSegmentZoneMapEmptyNode(vf, useCompress, freeFunc)
}

func NewSegmentZoneMapEmptyNode(vf comm.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) *SegmentZoneMapIndexMemNode {
	return &SegmentZoneMapIndexMemNode{
		FreeFunc:    freeFunc,
		File:        vf,
		UseCompress: useCompress,
	}
}

type SegmentZoneMapIndexMemNode struct {
	FreeFunc buf.MemoryFreeFunc
	File comm.IVFile
	UseCompress bool
	segmentZoneMap *basic.ZoneMap
	blockZoneMaps []*basic.ZoneMap
}

func (node *SegmentZoneMapIndexMemNode) ReadFrom(r io.Reader) (n int64, err error) {
	buffer := make([]byte, node.GetMemoryCapacity())
	nr, err := r.Read(buffer)
	if err != nil {
		return 0, err
	}
	err = node.Unmarshal(buffer)
	return int64(nr), err
}

func (node *SegmentZoneMapIndexMemNode) WriteTo(w io.Writer) (n int64, err error) {
	buffer, err := node.Marshal()
	if err != nil {
		return 0, err
	}
	nw, err := w.Write(buffer)
	return int64(nw), err
}

func (node *SegmentZoneMapIndexMemNode) Marshal() ([]byte, error) {
	panic("not needed")
}

func (node *SegmentZoneMapIndexMemNode) Unmarshal(data []byte) error {
	var err error
	node.blockZoneMaps = make([]*basic.ZoneMap, 0)
	blockCount := encoding.DecodeUint32(data[:4])
	data = data[4:]
	for i := uint32(0); i < blockCount; i++ {
		bufLen := encoding.DecodeUint16(data[:2])
		data = data[2:]
		blockBuffer := data[:bufLen]
		data = data[bufLen:]
		var blockZoneMap basic.ZoneMap
		if err = blockZoneMap.Unmarshal(blockBuffer); err != nil {
			return err
		}
		node.blockZoneMaps = append(node.blockZoneMaps, &blockZoneMap)
	}
	bufLen := encoding.DecodeUint16(data[:2])
	data = data[2:]
	segmentBuffer := data[:bufLen]
	data = data[bufLen:]
	node.segmentZoneMap = &basic.ZoneMap{}
	if err = node.segmentZoneMap.Unmarshal(segmentBuffer); err != nil {
		return err
	}
	return nil
}

func (node *SegmentZoneMapIndexMemNode) FreeMemory() {
	if node.FreeFunc != nil {
		node.FreeFunc(node)
	}
}

func (node *SegmentZoneMapIndexMemNode) Reset() {
	// no-op
}

func (node *SegmentZoneMapIndexMemNode) GetMemorySize() uint64 {
	if node.UseCompress {
		return uint64(node.File.Stat().Size())
	} else {
		return uint64(node.File.Stat().OriginSize())
	}
}

func (node *SegmentZoneMapIndexMemNode) GetMemoryCapacity() uint64 {
	if node.UseCompress {
		return uint64(node.File.Stat().Size())
	} else {
		return uint64(node.File.Stat().OriginSize())
	}
}

