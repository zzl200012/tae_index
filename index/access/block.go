package access

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"tae/index/common"
	"tae/index/io/io_iface"
	"tae/mock"
)

type NonAppendableBlockIndexHolder struct {
	host          *mock.Segment
	zoneMapReader      io_iface.IBlockZoneMapIndexReader
	staticFilterReader io_iface.IStaticFilterIndexReader
}

func NewNonAppendableBlockIndexHolder(host *mock.Segment) *NonAppendableBlockIndexHolder {
	return &NonAppendableBlockIndexHolder{host: host}
}

func (holder *NonAppendableBlockIndexHolder) GetBlockId() uint32 {
	return holder.host.GetBlockId()
}

func (holder *NonAppendableBlockIndexHolder) Print() string {
	zm := holder.zoneMapReader.Print()
	sf := holder.staticFilterReader.Print()
	return zm + "\n" + sf
}

func (holder *NonAppendableBlockIndexHolder) SetHost(host *mock.Segment) {
	holder.host = host
}

func (holder *NonAppendableBlockIndexHolder) GetZoneMapReader() io_iface.IBlockZoneMapIndexReader {
	return holder.zoneMapReader
}

func (holder *NonAppendableBlockIndexHolder) SetZoneMapReader(reader io_iface.IBlockZoneMapIndexReader) {
	holder.zoneMapReader = reader
}

func (holder *NonAppendableBlockIndexHolder) GetFilterReader() io_iface.IStaticFilterIndexReader {
	return holder.staticFilterReader
}

func (holder *NonAppendableBlockIndexHolder) SetFilterReader(reader io_iface.IStaticFilterIndexReader) {
	holder.staticFilterReader = reader
}

func (holder *NonAppendableBlockIndexHolder) GetBufferManager() iface.IBufferManager {
	return holder.host.FetchBufferManager()
}

func (holder *NonAppendableBlockIndexHolder) GetWriter() *mock.Part {
	return holder.host.FetchIndexWriter()
}

func (holder *NonAppendableBlockIndexHolder) MakeVirtualIndexFile(indexMeta *common.IndexMeta) *common.VirtualIndexFile {
	return common.MakeVirtualIndexFile(holder.host, indexMeta)
}

func (holder *NonAppendableBlockIndexHolder) Search(key interface{}) (uint32, error) {
	var err error
	var rowOffset uint32
	var exist bool
	if err = holder.zoneMapReader.Load(); err != nil {
		return 0, err
	}
	// TODO: handle the error
	defer holder.zoneMapReader.Unload()
	exist, err = holder.zoneMapReader.MayContainsKey(key)
	if err != nil {
		return 0, err
	}
	if !exist {
		return 0, mock.ErrKeyNotFound
	}
	// TODO: load exact data and get the `rowOffset`
	return rowOffset, nil
}

func (holder *NonAppendableBlockIndexHolder) ContainsKey(key interface{}) (bool, error) {
	var err error
	var exist bool
	if err = holder.zoneMapReader.Load(); err != nil {
		return false, err
	}
	// TODO: handle the error
	defer holder.zoneMapReader.Unload()
	exist, err = holder.zoneMapReader.MayContainsKey(key)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}
	if err = holder.staticFilterReader.Load(); err != nil {
		return false, nil
	}
	// TODO: handle the error
	defer holder.staticFilterReader.Unload()
	exist, err = holder.staticFilterReader.MayContainsKey(key)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}
	// TODO: load exact data and check the existence
	return true, nil
}

func (holder *NonAppendableBlockIndexHolder) ContainsAnyKeys(keys *vector.Vector) (bool, error) {
	var err error
	var ans *roaring.Bitmap
	var exist bool
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
	if err = holder.staticFilterReader.Load(); err != nil {
		return false, nil
	}
	// TODO: handle the error
	defer holder.staticFilterReader.Unload()
	ans, err = holder.staticFilterReader.MayContainsAnyKeys(keys, ans)
	if err != nil {
		return false, err
	}
	if ans.GetCardinality() == 0 {
		return false, nil
	}
	// TODO: load exact data and check the existence
	return true, nil
}

