package mock

import (
	"bytes"
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/spaolacci/murmur3"
	"strconv"
)

func MockVec(typ types.Type, rows int, offset int) *vector.Vector {
	vec := vector.New(typ)
	switch typ.Oid {
	case types.T_int8:
		data := make([]int8, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int8(i+offset))
		}
		vector.Append(vec, data)
	case types.T_int16:
		data := make([]int16, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int16(i+offset))
		}
		vector.Append(vec, data)
	case types.T_int32:
		data := make([]int32, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int32(i+offset))
		}
		vector.Append(vec, data)
	case types.T_int64:
		data := make([]int64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, int64(i+offset))
		}
		vector.Append(vec, data)
	case types.T_uint8:
		data := make([]uint8, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint8(i+offset))
		}
		vector.Append(vec, data)
	case types.T_uint16:
		data := make([]uint16, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint16(i+offset))
		}
		vector.Append(vec, data)
	case types.T_uint32:
		data := make([]uint32, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint32(i+offset))
		}
		vector.Append(vec, data)
	case types.T_uint64:
		data := make([]uint64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, uint64(i+offset))
		}
		vector.Append(vec, data)
	case types.T_float32:
		data := make([]float32, 0)
		for i := 0; i < rows; i++ {
			data = append(data, float32(i+offset))
		}
		vector.Append(vec, data)
	case types.T_float64:
		data := make([]float64, 0)
		for i := 0; i < rows; i++ {
			data = append(data, float64(i+offset))
		}
		vector.Append(vec, data)
	case types.T_date:
		data := make([]types.Date, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Date(i+offset))
		}
		vector.Append(vec, data)
	case types.T_datetime:
		data := make([]types.Datetime, 0)
		for i := 0; i < rows; i++ {
			data = append(data, types.Datetime(i+offset))
		}
		vector.Append(vec, data)
	case types.T_char, types.T_varchar:
		data := make([][]byte, 0)
		for i := 0; i < rows; i++ {
			data = append(data, []byte(strconv.Itoa(i+offset)))
		}
		vector.Append(vec, data)
	default:
		panic("unsupported type")
	}
	return vec
}

func Hash(v interface{}, typ types.Type) (uint64, error) {
	data, err := EncodeKey(v, typ)
	if err != nil {
		return 0, err
	}
	murmur := murmur3.Sum64(data)
	//xx := xxhash.Sum64(data)
	return murmur, nil
}

func EncodeKey(key interface{}, typ types.Type) ([]byte, error) {
	switch typ.Oid {
	case types.T_int8:
		if v, ok := key.(int8); ok {
			return encoding.EncodeInt8(v), nil
		} else {
			return nil, ErrTypeMismatch
		}
	case types.T_int16:
		if v, ok := key.(int16); ok {
			return encoding.EncodeInt16(v), nil
		} else {
			return nil, ErrTypeMismatch
		}
	case types.T_int32:
		if v, ok := key.(int32); ok {
			return encoding.EncodeInt32(v), nil
		} else {
			return nil, ErrTypeMismatch
		}
	case types.T_int64:
		if v, ok := key.(int64); ok {
			return encoding.EncodeInt64(v), nil
		} else {
			return nil, ErrTypeMismatch
		}
	case types.T_uint8:
		if v, ok := key.(uint8); ok {
			return encoding.EncodeUint8(v), nil
		} else {
			return nil, ErrTypeMismatch
		}
	case types.T_uint16:
		if v, ok := key.(uint16); ok {
			return encoding.EncodeUint16(v), nil
		} else {
			return nil, ErrTypeMismatch
		}
	case types.T_uint32:
		if v, ok := key.(uint32); ok {
			return encoding.EncodeUint32(v), nil
		} else {
			return nil, ErrTypeMismatch
		}
	case types.T_uint64:
		if v, ok := key.(uint64); ok {
			return encoding.EncodeUint64(v), nil
		} else {
			return nil, ErrTypeMismatch
		}
	case types.T_float32:
		if v, ok := key.(float32); ok {
			return encoding.EncodeFloat32(v), nil
		} else {
			return nil, ErrTypeMismatch
		}
	case types.T_float64:
		if v, ok := key.(float64); ok {
			return encoding.EncodeFloat64(v), nil
		} else {
			return nil, ErrTypeMismatch
		}
	case types.T_date:
		if v, ok := key.(types.Date); ok {
			return encoding.EncodeDate(v), nil
		} else {
			return nil, ErrTypeMismatch
		}
	case types.T_datetime:
		if v, ok := key.(types.Datetime); ok {
			return encoding.EncodeDatetime(v), nil
		} else {
			return nil, ErrTypeMismatch
		}
	case types.T_char, types.T_varchar:
		if v, ok := key.([]byte); ok {
			return v, nil
		} else {
			return nil, ErrTypeMismatch
		}
	default:
		panic("unsupported type")
	}
}

func ProcessVector(vec *vector.Vector, task func(v interface{}) error, visibility *roaring.Bitmap) error {
	var idxes []uint32
	if visibility != nil {
		idxes = visibility.ToArray()
	}
	switch vec.Typ.Oid {
	case types.T_int8:
		vs := vec.Col.([]int8)
		if visibility == nil {
			for _, v := range vs {
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v); err != nil {
					return err
				}
			}
		}
	case types.T_int16:
		vs := vec.Col.([]int16)
		if visibility == nil {
			for _, v := range vs {
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v); err != nil {
					return err
				}
			}
		}
	case types.T_int32:
		vs := vec.Col.([]int32)
		if visibility == nil {
			for _, v := range vs {
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v); err != nil {
					return err
				}
			}
		}
	case types.T_int64:
		vs := vec.Col.([]int64)
		if visibility == nil {
			for _, v := range vs {
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v); err != nil {
					return err
				}
			}
		}
	case types.T_uint8:
		vs := vec.Col.([]uint8)
		if visibility == nil {
			for _, v := range vs {
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v); err != nil {
					return err
				}
			}
		}
	case types.T_uint16:
		vs := vec.Col.([]uint16)
		if visibility == nil {
			for _, v := range vs {
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v); err != nil {
					return err
				}
			}
		}
	case types.T_uint32:
		vs := vec.Col.([]uint32)
		if visibility == nil {
			for _, v := range vs {
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v); err != nil {
					return err
				}
			}
		}
	case types.T_uint64:
		vs := vec.Col.([]uint64)
		if visibility == nil {
			for _, v := range vs {
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v); err != nil {
					return err
				}
			}
		}
	case types.T_float32:
		vs := vec.Col.([]float32)
		if visibility == nil {
			for _, v := range vs {
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v); err != nil {
					return err
				}
			}
		}
	case types.T_float64:
		vs := vec.Col.([]float64)
		if visibility == nil {
			for _, v := range vs {
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v); err != nil {
					return err
				}
			}
		}
	case types.T_date:
		vs := vec.Col.([]types.Date)
		if visibility == nil {
			for _, v := range vs {
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v); err != nil {
					return err
				}
			}
		}
	case types.T_datetime:
		vs := vec.Col.([]types.Datetime)
		if visibility == nil {
			for _, v := range vs {
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs[idx]
				if err := task(v); err != nil {
					return err
				}
			}
		}
	case types.T_char, types.T_varchar:
		vs := vec.Col.(*types.Bytes)
		if visibility == nil {
			for i := range vs.Offsets {
				v := vs.Get(int64(i))
				if err := task(v); err != nil {
					return err
				}
			}
		} else {
			for _, idx := range idxes {
				v := vs.Get(int64(idx))
				if err := task(v); err != nil {
					return err
				}
			}
		}
	default:
		panic("unsupported type")
	}
	return nil
}

func Compare(a, b interface{}, t types.Type) int {
	switch t.Oid {
	case types.T_int8:
		if a.(int8) > b.(int8) {
			return 1
		} else if a.(int8) < b.(int8) {
			return -1
		} else {
			return 0
		}
	case types.T_int16:
		if a.(int16) > b.(int16) {
			return 1
		} else if a.(int16) < b.(int16) {
			return -1
		} else {
			return 0
		}
	case types.T_int32:
		if a.(int32) > b.(int32) {
			return 1
		} else if a.(int32) < b.(int32) {
			return -1
		} else {
			return 0
		}
	case types.T_int64:
		if a.(int64) > b.(int64) {
			return 1
		} else if a.(int64) < b.(int64) {
			return -1
		} else {
			return 0
		}
	case types.T_uint8:
		if a.(uint8) > b.(uint8) {
			return 1
		} else if a.(uint8) < b.(uint8) {
			return -1
		} else {
			return 0
		}
	case types.T_uint16:
		if a.(uint16) > b.(uint16) {
			return 1
		} else if a.(uint16) < b.(uint16) {
			return -1
		} else {
			return 0
		}
	case types.T_uint32:
		if a.(uint32) > b.(uint32) {
			return 1
		} else if a.(uint32) < b.(uint32) {
			return -1
		} else {
			return 0
		}
	case types.T_uint64:
		if a.(uint64) > b.(uint64) {
			return 1
		} else if a.(uint64) < b.(uint64) {
			return -1
		} else {
			return 0
		}
	case types.T_float32:
		if a.(float32) > b.(float32) {
			return 1
		} else if a.(float32) < b.(float32) {
			return -1
		} else {
			return 0
		}
	case types.T_float64:
		if a.(float64) > b.(float64) {
			return 1
		} else if a.(float64) < b.(float64) {
			return -1
		} else {
			return 0
		}
	case types.T_date:
		if a.(types.Date) > b.(types.Date) {
			return 1
		} else if a.(types.Date) < b.(types.Date) {
			return -1
		} else {
			return 0
		}
	case types.T_datetime:
		if a.(types.Datetime) > b.(types.Datetime) {
			return 1
		} else if a.(types.Datetime) < b.(types.Datetime) {
			return -1
		} else {
			return 0
		}
	case types.T_char, types.T_varchar:
		res := bytes.Compare(a.([]byte), b.([]byte))
		if res > 0 {
			return 1
		} else if res < 0 {
			return -1
		} else {
			return 0
		}
	default:
		panic("unsupported type")
	}
}