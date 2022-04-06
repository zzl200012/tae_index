package holder

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"tae/index/access/access_iface"
	"tae/index/common"
	"tae/index/io/io_iface"
	m "tae/mock"
)

type mockAppendableSegmentIndexHolder struct {
	host *m.Segment
}

func (holder *mockAppendableSegmentIndexHolder) GetHost() *m.Segment {
	panic("implement me")
}

func NewMockAppendableSegmentIndexHolder(host *m.Segment) access_iface.IAppendableSegmentIndexHolder {
	return &mockAppendableSegmentIndexHolder{host: host}
}

func (holder *mockAppendableSegmentIndexHolder) Insert(key interface{}, offset uint32) error {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify bool) error {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) Delete(key interface{}) error {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) Search(key interface{}) (uint32, error) {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) ContainsKey(key interface{}) (bool, error) {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) ContainsAnyKeys(keys *vector.Vector) (bool, error) {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) GetSegmentId() uint32 {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) Print() string {
	panic("implement me")
}

func (holder *mockAppendableSegmentIndexHolder) Freeze() (access_iface.INonAppendableSegmentIndexHolder, error) {
	panic("implement me")
}

type mockNonAppendableSegmentIndexHolder struct {
	host          *m.Segment
	zoneMapReader io_iface.ISegmentZoneMapIndexReader
	staticFilterReaders []io_iface.IStaticFilterIndexReader
}

func (holder *mockNonAppendableSegmentIndexHolder) GetHost() *m.Segment {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) GetZoneMapReader() io_iface.ISegmentZoneMapIndexReader {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) SetZoneMapReader(reader io_iface.ISegmentZoneMapIndexReader) {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) GetFilterReaders() []io_iface.IStaticFilterIndexReader {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) SetFilterReaders(readers []io_iface.IStaticFilterIndexReader) {
	panic("implement me")
}

func NewMockNonAppendableSegmentIndexHolder(host *m.Segment) access_iface.INonAppendableSegmentIndexHolder {
	return &mockNonAppendableSegmentIndexHolder{host: host}
}

func (holder *mockNonAppendableSegmentIndexHolder) GetBufferManager() iface.IBufferManager {
	return holder.host.FetchBufferManager()
}

func (holder *mockNonAppendableSegmentIndexHolder) GetIndexAppender() *m.Part {
	return holder.host.FetchIndexWriter()
}

func (holder *mockNonAppendableSegmentIndexHolder) MakeVirtualIndexFile(indexMeta *common.IndexMeta) *common.VirtualIndexFile {
	return common.MakeVirtualIndexFile(holder.host, indexMeta)
}

func (holder *mockNonAppendableSegmentIndexHolder) Search(key interface{}) (uint32, error) {
	var err error
	var blockOffset uint32
	var exist bool
	if err = holder.zoneMapReader.Load(); err != nil {
		return 0, err
	}
	// TODO: handle the error
	defer holder.zoneMapReader.Unload()
	exist, blockOffset, err = holder.zoneMapReader.MayContainsKey(key)
	if err != nil {
		return 0, err
	}
	if !exist {
		return 0, m.ErrKeyNotFound
	}
	return blockOffset, nil
}

func (holder *mockNonAppendableSegmentIndexHolder) ContainsKey(key interface{}) (bool, error) {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) ContainsAnyKeys(keys *vector.Vector) (bool, error) {
	panic("implement me")
}

func (holder *mockNonAppendableSegmentIndexHolder) GetSegmentId() uint32 {
	return 0
}

func (holder *mockNonAppendableSegmentIndexHolder) Print() string {
	return ""
}

