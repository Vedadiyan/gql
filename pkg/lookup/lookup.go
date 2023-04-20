package lookup

import (
	"fmt"
	"strings"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/sentinel"
)

const (
	_WILD_CARD = -1
)

type LookupTable = map[any]map[int]bool

func ReadObject(row map[string]any, key string) (any, error) {
	ref := any(row)
	keys := strings.Split(key, ".")
	for _, key := range keys {
		result, isArray, err := readArray(ref, key)
		if err != nil {
			return nil, err
		}
		if isArray {
			ref = result
			continue
		}
		err = setObj(&ref, key)
		if err != nil {
			return nil, err
		}
	}
	return ref, nil
}

func readArray(row any, segment string) (any, bool, error) {
	if !cmn.IsIndex(segment) {
		return nil, false, nil
	}
	index, err := cmn.Index(segment)
	if err != nil {
		return nil, false, err
	}
	if index == _WILD_CARD {
		list, skipped := skipDim(row)
		if skipped {
			return list, true, nil
		}
		_, ok := row.(LookupTable)
		if !ok {
			return nil, false, sentinel.INVALID_TYPE.Extend(fmt.Sprintf("%T", row))
		}
		return row, true, nil
	}
	list, err := readIndexedDim(row, index)
	if err != nil {
		return nil, false, err
	}
	return list, true, nil
}

func skipDim(row any) ([]any, bool) {
	rows, ok := row.([]any)
	if !ok {
		return nil, false
	}
	list := make([]any, 0)
	for _, row := range rows {
		switch t := row.(type) {
		case []any:
			{
				list = append(list, t...)
			}
		default:
			{
				list = append(list, row)
			}
		}
	}
	return list, true
}

func readIndexedDim(row any, index int) (any, error) {
	switch t := row.(type) {
	case LookupTable:
		{
			return fromLookupTable(t, index), nil
		}
	case []any:
		{
			if index > len(t)-1 {
				return make(LookupTable), nil
			}
			return t[index], nil
		}
	default:
		{
			return nil, sentinel.UNSUPPORTED_CASE
		}
	}
}

func fromLookupTable(rl LookupTable, i int) LookupTable {
	output := make(LookupTable)
	for key, value := range rl {
		_, ok := value[i]
		if !ok {
			continue
		}
		output[key] = make(map[int]bool)
		output[key][i] = true
	}
	return output
}

func setObj(obj *any, key string) error {
	switch t := (*obj).(type) {
	case map[string]any:
		{
			v, ok := t[key]
			if ok {
				*obj = v
				return nil
			}
			return sentinel.KEY_NOT_FOUND
		}
	case []any:
		{
			setArray(obj, t, key)
			return nil
		}
	default:
		{
			set(obj, t, key)
			return nil
		}
	}
}

func set(ref *any, row any, key string) {
	list := make(LookupTable)
	switch t := row.(type) {
	case map[any]map[int]bool:
		{
			for rk := range t {
				v, ok := rk.(*map[string]any)
				if !ok {
					continue
				}
				setObjDim(list, v, key)
			}
		}
	}
	*ref = list
}

func setObjDim(list LookupTable, row *map[string]any, key string) {
	counter := 0
	for rk, rv := range *row {
		if rk != key {
			continue
		}
		switch t := rv.(type) {
		case []any:
			{
				for i, v := range t {
					switch t := v.(type) {
					case map[string]any:
						{
							_, ok := list[&t]
							if !ok {
								list[&t] = make(map[int]bool)
							}
							list[&t][i] = true
						}
					default:
						{
							_, ok := list[t]
							if !ok {
								list[t] = make(map[int]bool)
							}
							list[t][i] = true
						}
					}
				}
			}
		case map[string]any:
			{
				_, ok := list[&t]
				if !ok {
					list[&t] = make(map[int]bool)
				}
				list[&t][counter] = true
				counter++
			}
		default:
			{
				list[t] = make(map[int]bool)
				list[t][counter] = true
				counter++
			}
		}
	}
}

func setArray(ref *any, rows []any, key string) {
	array := make(map[any]map[int]bool)
	for i, v := range rows {
		switch itemType := v.(map[string]any)[key].(type) {
		case map[string]any:
			{
				array[&itemType] = make(map[int]bool)
				array[&itemType][i] = true
			}
		case []any:
			{
				_array := make(map[any]map[int]bool)
				for x, v := range itemType {
					switch valueType := v.(type) {
					case []any, map[string]any:
						{
							_, ok := array[&valueType]
							if !ok {
								array[&valueType] = map[int]bool{}
							}
							array[&valueType][i] = true
						}
					default:
						{
							_, ok := _array[valueType]
							if !ok {
								_array[valueType] = map[int]bool{}
							}
							// array[valueType][i] = true
							_array[valueType][x] = true
						}
					}
				}
				array[&_array] = make(map[int]bool)
				array[&_array][i] = true
			}
		default:
			{
				array[itemType] = make(map[int]bool)
				array[itemType][i] = true
			}
		}
	}
	*ref = array
}

func ToResult(obj any, recuring bool) any {
	switch t := obj.(type) {
	case LookupTable:
		{
			array := make([]any, 0, len(t))
			for k, v := range t {
				if arr, ok := k.(*map[any]map[int]bool); ok {
					array = append(array, ToResult(*arr, true))
					continue
				}
				for i, b := range v {
					if recuring {
						if b {
							if i >= len(array) {
								l := i - len(array) + 1
								for x := 0; x < l; x++ {
									array = append(array, nil)
								}
							}
							array[i] = k
						}
						continue
					}
					if b {
						array = append(array, k)
					}
				}
			}
			return array
		}
	case []any:
		{
			return t
		}
	default:
		{
			return obj
		}
	}
}
