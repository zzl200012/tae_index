package mock

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
)

type Part struct {
	offset uint32
	data []byte
}

func (p *Part) GetOffset() uint32 {
	return p.offset
}

func (p *Part) ReadAt(dst []byte, offset int64) (n int, err error) {
	if int(offset) + len(dst) > len(p.data) {
		panic("offset out of bound")
	}
	copy(dst, p.data[offset:offset + int64(len(dst))])
	return len(dst), nil
}

func (p *Part) Read(dst []byte) (n int, err error) {
	maxSize := len(dst)
	if maxSize > len(p.data) {
		panic("read page out of bound")
	}
	copy(dst, p.data)
	return maxSize, nil
}

func (p *Part) Write(data []byte) (n int, err error) {
	p.data = append(p.data, data...)
	return len(data), nil
}

func (p *Part) SeekCurrentOffset() uint32 {
	return uint32(len(p.data))
}

type Resource struct {
	parts []*Part
	bufferManager iface.IBufferManager
}

func NewSegment() *Resource {
	seg := &Resource{parts: make([]*Part, 0)}
	_ = seg.Allocate() // first part is for indices
	seg.bufferManager = manager.MockBufMgr(uint64(1024 * 100))
	return seg
}

func (pc *Resource) GetBlockId() uint32 {
	return 0
}

func (pc *Resource) GetSegmentId() uint32 {
	return 0
}

func (pc *Resource) GetPrimaryKeyType() types.Type {
	return types.Type{Oid: types.T_int32}
}

//func (pc *Resource) MakeIndexHolder() *access.SegmentIndexHolder {
//	return access.NewSegmentIndexHolder(pc)
//}

func (pc *Resource) Allocate() *Part {
	p := &Part{
		offset: uint32(len(pc.parts)),
		data:   []byte(""),
	}
	pc.parts = append(pc.parts, p)
	return p
}

func (pc *Resource) FetchPart(offset uint32) (*Part, error) {
	if int(offset) >= len(pc.parts) {
		panic("fetch page out of bound")
	}
	return pc.parts[offset], nil
}

func (pc *Resource) FetchIndexWriter() *Part {
	return pc.parts[0]
}

func (pc *Resource) FetchBufferManager() iface.IBufferManager {
	return pc.bufferManager
}
