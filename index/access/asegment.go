package access

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"sync"
	"tae/index/access/access_iface"
	"tae/index/common"
	"tae/index/io"
	"tae/index/io/io_iface"
	"tae/mock"
)

type AppendableSegmentIndexHolder struct {
	host *mock.Resource
	metaLatch *sync.RWMutex
	frozenBlockHolders []access_iface.INonAppendableBlockIndexHolder
	activeBlockHolder *AppendableBlockIndexHolder
	immutable bool
}

func NewAppendableSegmentIndexHolder(host *mock.Resource) *AppendableSegmentIndexHolder {
	return &AppendableSegmentIndexHolder{
		host:               host,
		metaLatch:          new(sync.RWMutex),
		frozenBlockHolders: make([]access_iface.INonAppendableBlockIndexHolder, 0),
	}
}

func (holder *AppendableSegmentIndexHolder) RegisterBlock(block *mock.Resource) error {
	holder.metaLatch.Lock()
	defer holder.metaLatch.Unlock()
	if holder.activeBlockHolder != nil {
		panic("unexpected error")
	}
	holder.activeBlockHolder = NewAppendableBlockIndexHolder(block)
	return nil
}

func (holder *AppendableSegmentIndexHolder) Insert(key interface{}, offset uint32) error {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeBlockHolder == nil {
		panic("unexpected error")
	}
	return holder.activeBlockHolder.Insert(key, offset)
}

func (holder *AppendableSegmentIndexHolder) BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify bool) error {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeBlockHolder == nil {
		panic("unexpected error")
	}
	return holder.activeBlockHolder.BatchInsert(keys, start, count, offset, verify)
}

func (holder *AppendableSegmentIndexHolder) Delete(key interface{}) error {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeBlockHolder == nil {
		panic("unexpected error")
	}
	return holder.activeBlockHolder.Delete(key)
}

func (holder *AppendableSegmentIndexHolder) Search(key interface{}) (uint32, error) {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeBlockHolder != nil {
		if rowOffset, err := holder.activeBlockHolder.Search(key); err != nil {
			if err != mock.ErrKeyNotFound {
				return 0, err
			}
		} else {
			return rowOffset, nil
		}
	}
	for _, frozen := range holder.frozenBlockHolders {
		if rowOffset, err := frozen.Search(key); err != nil {
			if err != mock.ErrKeyNotFound {
				return 0, err
			}
		} else {
			return rowOffset, nil
		}
	}
	return 0, mock.ErrKeyNotFound
}

func (holder *AppendableSegmentIndexHolder) ContainsKey(key interface{}) (exist bool, err error) {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeBlockHolder != nil {
		exist, err = holder.activeBlockHolder.ContainsKey(key)
		if err != nil {
			return false, err
		}
		if exist {
			return true, nil
		}
	}
	for _, frozen := range holder.frozenBlockHolders {
		if exist, err = frozen.ContainsKey(key); err != nil {
			return false, err
		}
		if exist {
			return true, nil
		}
	}
	return false, nil
}

func (holder *AppendableSegmentIndexHolder) ContainsAnyKeys(keys *vector.Vector) (exist bool, err error) {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeBlockHolder != nil {
		exist, err = holder.activeBlockHolder.ContainsAnyKeys(keys)
		if err != nil {
			return false, err
		}
		if exist {
			return true, nil
		}
	}
	for _, frozen := range holder.frozenBlockHolders {
		if exist, err = frozen.ContainsAnyKeys(keys); err != nil {
			return false, err
		}
		if exist {
			return true, nil
		}
	}
	return false, nil
}

func (holder *AppendableSegmentIndexHolder) GetSegmentId() uint32 {
	return holder.host.GetSegmentId()
}

func (holder *AppendableSegmentIndexHolder) GetHost() *mock.Resource {
	return holder.host
}

func (holder *AppendableSegmentIndexHolder) Print() string {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	s := "\n<A_SEG>\n\n"
	if holder.activeBlockHolder != nil {
		s += holder.activeBlockHolder.Print()
		s += "\n\n"
	}
	for _, frozen := range holder.frozenBlockHolders {
		s += frozen.Print()
		s += "\n\n"
	}
	s += "</A_SEG>"
	return s
}

func (holder *AppendableSegmentIndexHolder) Upgrade() (access_iface.INonAppendableSegmentIndexHolder, error) {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeBlockHolder != nil {
		panic("unexpected error")
	}
	upgraded := NewNonAppendableSegmentIndexHolder(holder.host)

	// TODO: fill new index holder with the real data
	//typ := types.Type{Oid: types.T_int32}
	//rowsPerBatch := 10000
	//batchesPerBlock :=  4
	//blockCount := 10
	//counter := 0
	zoneMapReader := io.NewSegmentZoneMapIndexReader()
	staticFilterReaders := make([]io_iface.IStaticFilterIndexReader, 0)
	zoneMapWriter := io.NewSegmentZoneMapIndexWriter()
	zoneMapWriter.Init(upgraded, common.Plain, uint16(0))
	staticFilterWriter := io.NewStaticFilterIndexWriter()
	staticFilterWriter.Init(upgraded, common.Plain, uint16(0))
	metas := make([]*common.IndexMeta, 0)
	blocks := holder.host.GetChildren()
	for _, block := range blocks {
		batch := block.GetData()
		zoneMapWriter.AddValues(batch)
		staticFilterWriter.AddValues(batch)
		zoneMapWriter.FinishBlock()
		staticFilterWriter.Finish()
		meta, _ := staticFilterWriter.Finalize()
		metas = append(metas, meta)
	}
	//for i := 0; i < blockCount; i++ {
	//	for j := 0; j < batchesPerBlock; j++ {
	//		offset := rowsPerBatch * counter
	//		batch := mock.MockVec(typ, rowsPerBatch, offset)
	//		counter++
	//		zoneMapWriter.AddValues(batch)
	//		staticFilterWriter.AddValues(batch)
	//	}
	//	zoneMapWriter.FinishBlock()
	//	staticFilterWriter.Finish()
	//	meta, _ := staticFilterWriter.Finalize()
	//	metas = append(metas, meta)
	//}
	meta, _ := zoneMapWriter.Finalize()
	metas = append(metas, meta)

	blockCount := len(blocks)
	for i := 0; i < blockCount; i++ {
		sfR := io.NewStaticFilterIndexReader()
		sfR.Init(upgraded, metas[i])
		staticFilterReaders = append(staticFilterReaders, sfR)
	}
	//zmR := io.NewSegmentZoneMapIndexReader()
	zoneMapReader.Init(upgraded, metas[blockCount])
	upgraded.SetZoneMapReader(zoneMapReader)
	upgraded.SetFilterReaders(staticFilterReaders)
	//

	return upgraded, nil
}

func (holder *AppendableSegmentIndexHolder) CloseCurrentActiveBlock() error {
	holder.metaLatch.Lock()
	defer holder.metaLatch.Unlock()
	frozen, err := holder.activeBlockHolder.Upgrade()
	if err != nil {
		return err
	}
	holder.activeBlockHolder = nil
	holder.frozenBlockHolders = append(holder.frozenBlockHolders, frozen)
	return nil
}

func (holder *AppendableSegmentIndexHolder) ReadyToUpgrade() bool {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	return holder.immutable
}

func (holder *AppendableSegmentIndexHolder) MarkAsImmutable() error {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeBlockHolder != nil {
		panic("unexpected error")
	}
	if holder.immutable {
		panic("unexpected error")
	}
	holder.immutable = true
	return nil
}
