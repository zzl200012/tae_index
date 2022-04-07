package access

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"tae/index/access/access_iface"
	"tae/index/basic"
	"tae/index/common"
	"tae/index/io"
	"tae/mock"
)

type AppendableBlockIndexHolder struct {
	host *mock.Resource
	zoneMap *basic.ZoneMap
	artIndex basic.ARTMap
}

func NewAppendableBlockIndexHolder(host *mock.Resource /*, pkType types.Type*/) *AppendableBlockIndexHolder {
	return &AppendableBlockIndexHolder{
		host: host,
		zoneMap:  basic.NewZoneMap(host.GetPrimaryKeyType(), nil),
		artIndex: basic.NewSimpleARTMap(host.GetPrimaryKeyType(), nil),
	}
}

func (holder *AppendableBlockIndexHolder) GetBlockId() uint32 {
	return holder.host.GetBlockId()
}

func (holder *AppendableBlockIndexHolder) GetHost() *mock.Resource {
	return holder.host
}

func (holder *AppendableBlockIndexHolder) Print() string {
	zm := holder.zoneMap.Print()
	art := holder.artIndex.Print()
	return "<A_BLK>\n" + zm + "\n" + art + "\n" + "</A_BLK>"
}

func (holder *AppendableBlockIndexHolder) Insert(key interface{}, offset uint32) (err error) {
	if err = holder.zoneMap.Update(key); err != nil {
		return err
	}
	if err = holder.artIndex.Insert(key, offset); err != nil {
		return err
	}
	return nil
}

func (holder *AppendableBlockIndexHolder) BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify bool) (err error) {
	if err = holder.zoneMap.BatchUpdate(keys); err != nil {
		return err
	}
	if err = holder.artIndex.BatchInsert(keys, start, count, offset, verify); err != nil {
		return err
	}
	return nil
}

func (holder *AppendableBlockIndexHolder) Delete(key interface{}) error {
	if err := holder.artIndex.Delete(key); err != nil {
		return err
	}
	return nil
}

func (holder *AppendableBlockIndexHolder) Search(key interface{}) (rowOffset uint32, err error) {
	var exist bool
	if exist, err = holder.zoneMap.MayContainsKey(key); err != nil {
		return 0, err
	}
	if !exist {
		return 0, mock.ErrKeyNotFound
	}
	if rowOffset, err = holder.artIndex.Search(key); err != nil {
		return 0, err
	}
	return rowOffset, nil
}

func (holder *AppendableBlockIndexHolder) ContainsKey(key interface{}) (exist bool, err error) {
	if exist, err = holder.zoneMap.MayContainsKey(key); err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}
	if exist, err = holder.artIndex.ContainsKey(key); err != nil {
		return false, err
	}
	return exist, nil
}

func (holder *AppendableBlockIndexHolder) ContainsAnyKeys(keys *vector.Vector) (exist bool, err error) {
	var ans *roaring.Bitmap
	exist, ans, err = holder.zoneMap.MayContainsAnyKeys(keys)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}
	if ans.GetCardinality() == 0 {
		panic("unexpected error")
	}
	exist, err = holder.artIndex.ContainsAnyKeys(keys, ans)
	if err != nil {
		return false, err
	}
	return exist, nil
}

func (holder *AppendableBlockIndexHolder) Upgrade() (access_iface.INonAppendableBlockIndexHolder, error) {
	var err error
	var meta *common.IndexMeta
	newHolder := NewNonAppendableBlockIndexHolder(holder.host)
	zoneMapWriter := io.NewBlockZoneMapIndexWriter()
	err = zoneMapWriter.Init(newHolder, common.Plain, uint16(0)) // TODO: fill the args by passed fields
	if err != nil {
		return nil, err
	}
	zoneMapWriter.SetMinMax(holder.zoneMap.GetMin(), holder.zoneMap.GetMax(), holder.host.GetPrimaryKeyType())

	meta, err = zoneMapWriter.Finalize()
	if err != nil {
		return nil, err
	}
	zoneMapReader := io.NewBlockZoneMapIndexReader()
	err = zoneMapReader.Init(newHolder, meta)
	if err != nil {
		return nil, err
	}
	staticFilterWriter := io.NewStaticFilterIndexWriter()
	var columnData *vector.Vector // TODO: fill the data

	columnData = holder.artIndex.Freeze()

	err = staticFilterWriter.Init(newHolder, common.Plain, uint16(0))
	if err != nil {
		return nil, err
	}
	err = staticFilterWriter.SetValues(columnData)
	if err != nil {
		return nil, err
	}
	meta, err = staticFilterWriter.Finalize()
	if err != nil {
		return nil, err
	}
	staticFilterReader := io.NewStaticFilterIndexReader()
	err = staticFilterReader.Init(newHolder, meta)
	if err != nil {
		return nil, err
	}
	newHolder.SetZoneMapReader(zoneMapReader)
	newHolder.SetFilterReader(staticFilterReader)
	//logrus.Info(newHolder.staticFilterReader.Print())
	return newHolder, nil
}


