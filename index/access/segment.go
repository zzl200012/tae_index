package access

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"tae/index/common"
	"tae/index/io/io_iface"
	"tae/mock"
)

type NonAppendableSegmentIndexHolder struct {
	host          *mock.Segment
	zoneMapReader io_iface.ISegmentZoneMapIndexReader
	staticFilterReaders []io_iface.IStaticFilterIndexReader
}

func NewNonAppendableSegmentIndexHolder(host *mock.Segment) *NonAppendableSegmentIndexHolder {
	return &NonAppendableSegmentIndexHolder{host: host}
}

func (holder *NonAppendableSegmentIndexHolder) GetSegmentId() uint32 {
	return 0
}

func (holder *NonAppendableSegmentIndexHolder) GetHost() *mock.Segment {
	return holder.host
}

func (holder *NonAppendableSegmentIndexHolder) SetHost(host *mock.Segment) {
	holder.host = host
}

func (holder *NonAppendableSegmentIndexHolder) GetZoneMapReader() io_iface.ISegmentZoneMapIndexReader {
	return holder.zoneMapReader
}

func (holder *NonAppendableSegmentIndexHolder) SetZoneMapReader(reader io_iface.ISegmentZoneMapIndexReader) {
	holder.zoneMapReader = reader
}

func (holder *NonAppendableSegmentIndexHolder) GetFilterReaders() []io_iface.IStaticFilterIndexReader {
	return holder.staticFilterReaders
}

func (holder *NonAppendableSegmentIndexHolder) SetFilterReaders(readers []io_iface.IStaticFilterIndexReader) {
	holder.staticFilterReaders = readers
}

func (holder *NonAppendableSegmentIndexHolder) Search(key interface{}) (uint32, error) {
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
		return 0, mock.ErrKeyNotFound
	}
	return blockOffset, nil
}

func (holder *NonAppendableSegmentIndexHolder) ContainsKey(key interface{}) (bool, error) {
	var err error
	var blockOffset uint32
	var exist bool
	if err = holder.zoneMapReader.Load(); err != nil {
		return false, err
	}
	// TODO: handle the error
	defer holder.zoneMapReader.Unload()
	exist, blockOffset, err = holder.zoneMapReader.MayContainsKey(key)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}
	filter := holder.staticFilterReaders[blockOffset]
	if err = filter.Load(); err != nil {
		return false, err
	}
	// TODO: handle the error
	defer filter.Unload()
	exist, err = filter.MayContainsKey(key)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}
	// TODO: load column block of `blockOffset` and check the existence
	return true, nil
}

func (holder *NonAppendableSegmentIndexHolder) ContainsAnyKeys(keys *vector.Vector) (bool, error) {
	var err error
	var exist bool
	var ans []*roaring.Bitmap
	filterConsultTimes := 0
	if err = holder.zoneMapReader.Load(); err != nil {
		return false, err
	}
	// TODO: handle the error
	defer holder.zoneMapReader.Unload()
	exist, ans, err = holder.zoneMapReader.MayContainsAnyKeys(keys)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}
	if ans == nil {
		panic("unexpected error")
	}
	for i := 0; i < len(ans); i++ {
		idxes := ans[i]
		if idxes == nil {
			continue
		}
		blockFilter := holder.staticFilterReaders[i]
		if err = blockFilter.Load(); err != nil {
			return false, err
		}
		filterConsultTimes++
		further, err := blockFilter.MayContainsAnyKeys(keys, idxes)
		if err != nil {
			blockFilter.Unload() // TODO: handle error
			return false, err
		}
		if err = blockFilter.Unload(); err != nil {
			return false, err
		}
		if further.GetCardinality() != 0 {
			// TODO: load column block of `blockOffset` and check the existence
			//logrus.Info(further.GetCardinality())
			//logrus.Info(filterConsultTimes)
			return true, nil
		}
	}
	return false, nil
}

func (holder *NonAppendableSegmentIndexHolder) GetBufferManager() iface.IBufferManager {
	return holder.host.FetchBufferManager()
}

func (holder *NonAppendableSegmentIndexHolder) GetIndexAppender() *mock.Part {
	return holder.host.FetchIndexWriter()
}

func (holder *NonAppendableSegmentIndexHolder) MakeVirtualIndexFile(indexMeta *common.IndexMeta) *common.VirtualIndexFile {
	return common.MakeVirtualIndexFile(holder.host, indexMeta)
}

func (holder *NonAppendableSegmentIndexHolder) Print() string {
	s := ""
	zm := holder.zoneMapReader.Print()
	s += zm
	s += "\n"
	for _, sf := range holder.staticFilterReaders {
		s += sf.Print()
		s += "\n"
	}
	return s
}