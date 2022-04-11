package mock

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
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
	id uint32
	parts []*Part
	children []*Resource
	data *vector.Vector
	bufferManager iface.IBufferManager
}

var bufManager iface.IBufferManager
var blkIdAlloc *common.IdAlloctor
var segIdAlloc *common.IdAlloctor

func init() {
	bufManager = manager.MockBufMgr(uint64(1024 * 1024 * 1024))
	segIdAlloc = common.NewIdAlloctor(1)
	blkIdAlloc = common.NewIdAlloctor(1)
}

type Table struct {
	*Resource
}

func NewTable() *Table {
	return &Table{
		Resource: NewResource(),
	}
}

func (tbl *Table) NewSegment() *Segment {
	s := &Segment{
		Resource: NewResource(),
	}
	s.Resource.id = uint32(segIdAlloc.Alloc())
	tbl.children = append(tbl.children, s.Resource)
	return s
}

type Segment struct {
	*Resource
}

func (seg *Segment) NewBlock() *Block {
	b := &Block{
		Resource: NewResource(),
	}
	b.Resource.id = uint32(blkIdAlloc.Alloc())
	seg.children = append(seg.children, b.Resource)
	return b
}

type Block struct {
	*Resource
}

func NewResource() *Resource {
	seg := &Resource{parts: make([]*Part, 0)}
	_ = seg.Allocate() // first part is for indices
	seg.bufferManager = bufManager
	return seg
}

func (pc *Resource) GetBlockId() uint32 {
	return pc.id
}

func (pc *Resource) GetSegmentId() uint32 {
	return pc.id
}

func (pc *Resource) GetPrimaryKeyType() types.Type {
	return types.Type{Oid: types.T_int32}
}

func (pc *Resource) AppendData(data *vector.Vector) {
	if pc.data == nil {
		pc.data = data
	} else {
		vector.Append(pc.data, data.Col)
	}
}

func (pc *Resource) GetData() *vector.Vector {
	return pc.data
}

func (pc *Resource) AddChild(r *Resource) {
	pc.children = append(pc.children, r)
}

func (pc *Resource) GetChildren() []*Resource {
	return pc.children
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
