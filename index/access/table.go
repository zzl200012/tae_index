package access

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"sync"
	"tae/index/access/access_iface"
	"tae/mock"
)

type TableIndexHolder struct {
	host                 *mock.Resource
	metaLatch            *sync.RWMutex
	frozenSegmentHolders []access_iface.INonAppendableSegmentIndexHolder
	closedSegmentHolder []*AppendableSegmentIndexHolder
	activeSegmentHolder *AppendableSegmentIndexHolder
}

func NewTableIndexHolder(host *mock.Resource) *TableIndexHolder {
	return &TableIndexHolder{
		host:                 host,
		metaLatch:            new(sync.RWMutex),
		frozenSegmentHolders: make([]access_iface.INonAppendableSegmentIndexHolder, 0),
	}
}

func (holder *TableIndexHolder) RegisterSegment(host *mock.Resource) error {
	holder.metaLatch.Lock()
	defer holder.metaLatch.Unlock()
	if holder.activeSegmentHolder != nil {
		panic("unexpected error")
	}
	holder.activeSegmentHolder = NewAppendableSegmentIndexHolder(host)
	return nil
}

func (holder *TableIndexHolder) RegisterBlock(host *mock.Resource) error {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeSegmentHolder == nil {
		panic("unexpected error")
	}
	err := holder.activeSegmentHolder.RegisterBlock(host)
	if err != nil {
		return err
	}
	return nil
}

func (holder *TableIndexHolder) CloseCurrentActiveBlock() error {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeSegmentHolder == nil {
		panic("unexpected error")
	}
	err := holder.activeSegmentHolder.CloseCurrentActiveBlock()
	if err != nil {
		return err
	}
	return nil
}

func (holder *TableIndexHolder) CloseCurrentActiveSegment() error {
	holder.metaLatch.Lock()
	defer holder.metaLatch.Unlock()
	if holder.activeSegmentHolder == nil {
		panic("unexpected error")
	}
	if err := holder.activeSegmentHolder.MarkAsImmutable(); err != nil {
		return err
	}
	closed := holder.activeSegmentHolder
	holder.activeSegmentHolder = nil
	holder.closedSegmentHolder = append(holder.closedSegmentHolder, closed)
	return nil
}

func (holder *TableIndexHolder) UpgradeSegment(id uint32) error {
	holder.metaLatch.Lock()
	defer holder.metaLatch.Unlock()
	var target *AppendableSegmentIndexHolder
	for i, closed := range holder.closedSegmentHolder {
		if closed.GetSegmentId() == id {
			if !closed.ReadyToUpgrade() {
				panic("unexpected error")
			}
			holder.closedSegmentHolder = append(holder.closedSegmentHolder[:i], holder.closedSegmentHolder[i+1:]...)
			target = closed
			break
		}
	}
	if target == nil {
		panic("segment not found")
	}
	frozen, err := target.Upgrade()
	if err != nil {
		return err
	}
	holder.frozenSegmentHolders = append(holder.frozenSegmentHolders, frozen)
	return nil
}

func (holder *TableIndexHolder) Insert(key interface{}, offset uint32) error {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	activeSeg := holder.activeSegmentHolder
	if activeSeg == nil {
		panic("unexpected error")
	}
	err := activeSeg.Insert(key, offset)
	if err != nil {
		return err
	}
	return nil
}

func (holder *TableIndexHolder) BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify bool) error {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	activeSeg := holder.activeSegmentHolder
	if activeSeg == nil {
		panic("unexpected error")
	}
	err := activeSeg.BatchInsert(keys, start, count, offset, verify)
	if err != nil {
		return err
	}
	return nil
}

func (holder *TableIndexHolder) Delete(key interface{}) error {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	activeSeg := holder.activeSegmentHolder
	if activeSeg == nil {
		panic("unexpected error")
	}
	err := activeSeg.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

func (holder *TableIndexHolder) Search(key interface{}) (uint32, error) {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeSegmentHolder != nil {
		rowOffset, err := holder.activeSegmentHolder.Search(key)
		if err != nil {
			if err != mock.ErrKeyNotFound {
				return 0, err
			}
		} else {
			return rowOffset, nil
		}
	}
	for _, closedSeg := range holder.closedSegmentHolder {
		rowOffset, err := closedSeg.Search(key)
		if err != nil {
			if err != mock.ErrKeyNotFound {
				return 0, err
			}
		} else {
			return rowOffset, nil
		}
	}
	for _, frozenSeg := range holder.frozenSegmentHolders {
		rowOffset, err := frozenSeg.Search(key)
		if err != nil {
			if err != mock.ErrKeyNotFound {
				return 0, err
			}
		} else {
			return rowOffset, nil
		}
	}
	return 0, mock.ErrKeyNotFound
}

func (holder *TableIndexHolder) ContainsKey(key interface{}) (bool, error) {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeSegmentHolder != nil {
		exist, err := holder.activeSegmentHolder.ContainsKey(key)
		if err != nil {
			return false, err
		}
		if exist {
			return true, nil
		}
	}
	for _, closedSeg := range holder.closedSegmentHolder {
		exist, err := closedSeg.ContainsKey(key)
		if err != nil {
			return false, err
		}
		if exist {
			return true, nil
		}
	}
	for _, frozenSeg := range holder.frozenSegmentHolders {
		exist, err := frozenSeg.ContainsKey(key)
		if err != nil {
			return false, err
		}
		if exist {
			return true, nil
		}
	}
	return false, nil
}

func (holder *TableIndexHolder) ContainsAnyKeys(keys *vector.Vector) (bool, error) {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	if holder.activeSegmentHolder != nil {
		exist, err := holder.activeSegmentHolder.ContainsAnyKeys(keys)
		if err != nil {
			return false, err
		}
		if exist {
			return true, nil
		}
	}
	for _, closedSeg := range holder.closedSegmentHolder {
		exist, err := closedSeg.ContainsAnyKeys(keys)
		if err != nil {
			return false, err
		}
		if exist {
			return true, nil
		}
	}
	for _, frozenSeg := range holder.frozenSegmentHolders {
		exist, err := frozenSeg.ContainsAnyKeys(keys)
		if err != nil {
			return false, err
		}
		if exist {
			return true, nil
		}
	}
	return false, nil
}

func (holder *TableIndexHolder) GetTableId() uint32 {
	return 0
}

func (holder *TableIndexHolder) GetHost() *mock.Resource {
	return holder.host
}

func (holder *TableIndexHolder) PrintShort() string {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	s := ""
	if holder.activeSegmentHolder != nil {
		s += "<ACT_SEG>"
	}
	for _, closed := range holder.closedSegmentHolder {
		s += "<CLD_SEG>"
		s += closed.segmentZoneMap.Print()
	}
	for range holder.frozenSegmentHolders {
		s += "<FRZ_SEG>"
	}
	return s
}

func (holder *TableIndexHolder) Print() string {
	holder.metaLatch.RLock()
	defer holder.metaLatch.RUnlock()
	s := ""
	if holder.activeSegmentHolder != nil {
		s += holder.activeSegmentHolder.Print()
		s += "\n"
	}
	for _, closed := range holder.closedSegmentHolder {
		s += closed.Print()
		s += "\n"
	}
	for _, frozen := range holder.frozenSegmentHolders {
		s += frozen.Print()
		s += "\n"
	}
	return s
}
