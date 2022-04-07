package access

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"sync"
	"tae/index/access/access_iface"
	"tae/mock"
)

type TableIndexHolder struct {
	host *mock.Resource
	metaLock *sync.RWMutex
	frozenSegmentHolders []access_iface.INonAppendableSegmentIndexHolder
	activeSegmentHolder *AppendableSegmentIndexHolder
}

func NewTableIndexHolder(host *mock.Resource) *TableIndexHolder {
	return &TableIndexHolder{
		host:                 host,
		metaLock:             new(sync.RWMutex),
		frozenSegmentHolders: make([]access_iface.INonAppendableSegmentIndexHolder, 0),
	}
}

func (holder *TableIndexHolder) RegisterSegment(host *mock.Resource) error {
	holder.metaLock.Lock()
	defer holder.metaLock.Unlock()
	if holder.activeSegmentHolder != nil {
		panic("unexpected error")
	}
	holder.activeSegmentHolder = NewAppendableSegmentIndexHolder(host)
	return nil
}

func (holder *TableIndexHolder) CloseCurrentActiveSegment() error {
	holder.metaLock.Lock()
	defer holder.metaLock.Unlock()
	if holder.activeSegmentHolder == nil {
		panic("unexpected error")
	}
	if !holder.activeSegmentHolder.ReadyToUpgrade() {
		panic("unexpected error")
	}
	frozen, err := holder.activeSegmentHolder.Upgrade()
	if err != nil {
		return err
	}
	holder.activeSegmentHolder = nil
	holder.frozenSegmentHolders = append(holder.frozenSegmentHolders, frozen)
	return nil
}

func (holder *TableIndexHolder) Insert(key interface{}, offset uint32) error {
	panic("implement me")
}

func (holder *TableIndexHolder) BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify bool) error {
	panic("implement me")
}

func (holder *TableIndexHolder) Delete(key interface{}) error {
	panic("implement me")
}

func (holder *TableIndexHolder) Search(key interface{}) (uint32, error) {
	panic("implement me")
}

func (holder *TableIndexHolder) ContainsKey(key interface{}) (bool, error) {
	panic("implement me")
}

func (holder *TableIndexHolder) ContainsAnyKeys(keys *vector.Vector) (bool, error) {
	panic("implement me")
}

func (holder *TableIndexHolder) GetTableId() uint32 {
	panic("implement me")
}

func (holder *TableIndexHolder) GetHost() *mock.Resource {
	panic("implement me")
}

func (holder *TableIndexHolder) Print() string {
	panic("implement me")
}
