package basic

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	art "github.com/plar/go-adaptive-radix-tree"
	"strconv"
	"sync"
	"tae/index/common"
	"tae/mock"
)

type ARTMap interface {
	Insert(key interface{}, offset uint32) error
	BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify bool) error
	Update(key interface{}, offset uint32) error
	BatchUpdate(keys *vector.Vector, offsets []uint32) error
	Delete(key interface{}) error
	Search(key interface{}) (uint32, error)
	ContainsKey(key interface{}) (bool, error)
	ContainsAnyKeys(keys *vector.Vector, visibility *roaring.Bitmap) (bool, error)
	Print() string
	Freeze() *vector.Vector
}

type simpleARTMap struct {
	mu    *sync.RWMutex
	typ   types.Type
	inner art.Tree
}

func NewSimpleARTMap(typ types.Type, mutex *sync.RWMutex) ARTMap {
	if mutex == nil {
		mutex = new(sync.RWMutex)
	}
	tree := art.New()
	return &simpleARTMap{
		mu:    mutex,
		typ:   typ,
		inner: tree,
	}
}

func (art *simpleARTMap) Insert(key interface{}, offset uint32) error {
	art.mu.Lock()
	defer art.mu.Unlock()
	return art.InsertLocked(key, offset)
}

func (art *simpleARTMap) InsertLocked(key interface{}, offset uint32) error {
	ikey, err := mock.EncodeKey(key, art.typ)
	if err != nil {
		return err
	}
	old, _ := art.inner.Insert(ikey, offset)
	if old != nil {
		art.inner.Insert(ikey, old)
		return mock.ErrKeyDuplicate
	}
	return nil
}

func (art *simpleARTMap) BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify bool) error {
	art.mu.Lock()
	defer art.mu.Unlock()
	return art.BatchInsertLocked(keys, start, count, offset, verify)
}

func (art *simpleARTMap) BatchInsertLocked(keys *vector.Vector, start int, count int, offset uint32, verify bool) error {
	existence := make(map[interface{}]bool)

	processor := func(v interface{}) error {
		encoded, err := mock.EncodeKey(v, art.typ)
		if err != nil {
			return err
		}
		if verify {
			if _, found := existence[string(encoded)]; found {
				return mock.ErrKeyDuplicate
			}
			existence[string(encoded)] = true
		}
		old, _ := art.inner.Insert(encoded, offset)
		if old != nil {
			// TODO: rollback previous insertion if duplication comes up
			return mock.ErrKeyDuplicate
		}
		offset++
		return nil
	}

	exact := vector.New(keys.Typ)
	vector.Window(keys, start, start + count, exact)
	if err := mock.ProcessVector(exact, processor, nil); err != nil {
		return err
	}
	return nil
}

func (art *simpleARTMap) Update(key interface{}, offset uint32) error {
	art.mu.Lock()
	defer art.mu.Unlock()
	return art.UpdateLocked(key, offset)
}

func (art *simpleARTMap) UpdateLocked(key interface{}, offset uint32) error {
	ikey, err := mock.EncodeKey(key, art.typ)
	if err != nil {
		return err
	}
	old, _ := art.inner.Insert(ikey, offset)
	if old == nil {
		art.inner.Delete(ikey)
		return mock.ErrKeyNotFound
	}
	return nil
}

func (art *simpleARTMap) BatchUpdate(keys *vector.Vector, offsets []uint32) error {
	art.mu.Lock()
	defer art.mu.Unlock()
	return art.BatchUpdateLocked(keys, offsets)
}

func (art *simpleARTMap) BatchUpdateLocked(keys *vector.Vector, offsets []uint32) error {
	idx := 0

	processor := func(v interface{}) error {
		encoded, err := mock.EncodeKey(v, art.typ)
		if err != nil {
			return err
		}
		old, _ := art.inner.Insert(encoded, offsets[idx])
		if old == nil {
			art.inner.Delete(encoded)
			return mock.ErrKeyNotFound
		}
		idx++
		return nil
	}

	if err := mock.ProcessVector(keys, processor, nil); err != nil {
		return err
	}
	return nil
}

func (art *simpleARTMap) Delete(key interface{}) error {
	art.mu.Lock()
	defer art.mu.Unlock()
	return art.DeleteLocked(key)
}

func (art *simpleARTMap) DeleteLocked(key interface{}) error {
	ikey, err := mock.EncodeKey(key, art.typ)
	if err != nil {
		return err
	}
	_, found := art.inner.Delete(ikey)
	if !found {
		return mock.ErrKeyNotFound
	}
	return nil
}

func (art *simpleARTMap) Search(key interface{}) (uint32, error) {
	art.mu.RLock()
	defer art.mu.RUnlock()
	return art.SearchLocked(key)
}

func (art *simpleARTMap) SearchLocked(key interface{}) (uint32, error) {
	ikey, err := mock.EncodeKey(key, art.typ)
	if err != nil {
		return 0, err
	}
	offset, found := art.inner.Search(ikey)
	if !found {
		return 0, mock.ErrKeyNotFound
	}
	return offset.(uint32), nil
}

func (art *simpleARTMap) ContainsKey(key interface{}) (bool, error) {
	art.mu.RLock()
	defer art.mu.RUnlock()
	common.ARTIndexConsulted++
	return art.ContainsKeyLocked(key)
}

func (art *simpleARTMap) ContainsKeyLocked(key interface{}) (bool, error) {
	ikey, err := mock.EncodeKey(key, art.typ)
	if err != nil {
		return false, err
	}
	_, exists := art.inner.Search(ikey)
	if exists {
		return true, nil
	}
	return false, nil
}

func (art *simpleARTMap) ContainsAnyKeys(keys *vector.Vector, visibility *roaring.Bitmap) (bool, error) {
	art.mu.RLock()
	defer art.mu.RUnlock()
	return art.ContainsAnyKeysLocked(keys, visibility)
}

func (art *simpleARTMap) ContainsAnyKeysLocked(keys *vector.Vector, visibility *roaring.Bitmap) (bool, error) {
	processor := func(v interface{}) error {
		common.ARTIndexConsulted++
		encoded, err := mock.EncodeKey(v, art.typ)
		if err != nil {
			return err
		}
		if _, found := art.inner.Search(encoded); found {
			return mock.ErrKeyDuplicate
		}
		return nil
	}
	if err := mock.ProcessVector(keys, processor, visibility); err != nil {
		if err == mock.ErrKeyDuplicate {
			return true, nil
		} else {
			return false, err
		}
	}
	return false, nil
}

func (art *simpleARTMap) Print() string {
	art.mu.RLock()
	defer art.mu.RUnlock()
	min, _ := art.inner.Minimum()
	max, _ := art.inner.Maximum() // TODO: seems not accurate here
	return "<ART>\n" + "[" + strconv.Itoa(int(min.(uint32))) + ", " + strconv.Itoa(int(max.(uint32))) + "]" + "(" + strconv.Itoa(art.inner.Size()) + ")"
}

func (art *simpleARTMap) Freeze() *vector.Vector {
	// TODO: support all types
	art.mu.RLock()
	defer art.mu.RUnlock()
	iter := art.inner.Iterator()
	vec := vector.New(art.typ)
	keys := make([]int32, 0)
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			panic(err)
		}
		key := mock.DecodeKey(node.Key(), art.typ).(int32)
		//err = vector.Append(vec, key)
		//if err != nil {
		//	panic(err)
		//}
		keys = append(keys, key)
	}
	err := vector.Append(vec, keys)
	if err != nil {
		panic(err)
	}
	return vec
}
