package sql

import (
	"fmt"
	"strings"
)

func jumpDimension(row any) ([]any, bool) {
	rows, ok := row.([]any)
	if !ok {
		return nil, false
	}
	list := make([]any, 0)
	for _, item := range rows {
		switch itemType := item.(type) {
		case []any:
			{
				list = append(list, itemType...)
			}
		default:
			{
				list = append(list, item)
			}
		}
	}
	return list, true
}

func readIndexOfArray(row any, index int) (any, error) {
	switch rowType := row.(type) {
	case map[any]map[int]bool:
		{
			list := make(map[any]map[int]bool)
			for key, value := range rowType {
				_, ok := value[int(index)]
				if !ok {
					continue
				}
				list[key] = make(map[int]bool)
				list[key][int(index)] = true
			}
			return list, nil
		}
	case []any:
		{
			if int(index) > len(rowType)-1 {
				return make(map[any]map[int]bool), nil
			}
			return rowType[index], nil
		}
	default:
		{
			return nil, UNSUPPORTED_CASE
		}
	}
}

func readArray(row any, segment string) (any, bool, error) {
	if isIndex(segment) {
		index, err := index(segment)
		if err != nil {
			return nil, false, err
		}
		if index == -1 {
			list, ok := jumpDimension(row)
			if ok {
				return list, true, nil
			}
			_, ok = row.(map[any]map[int]bool)
			if !ok {
				return nil, false, INVALID_TYPE.Extend(fmt.Sprintf("%T", row))
			}
			return row, true, nil
		}
		list, err := readIndexOfArray(row, index)
		if err != nil {
			return nil, false, err
		}
		return list, true, nil
	}
	return nil, false, nil
}

func readObject(row map[string]any, key string) (any, error) {
	ref := any(row)
	segments := strings.Split(key, ".")
	for _, segment := range segments {
		result, ok, err := readArray(ref, segment)
		if err != nil {
			return nil, err
		}
		if ok {
			ref = result
			continue
		}
		err = referenceObject(&ref, segment)
		if err != nil {
			return nil, err
		}
	}
	return ref, nil
}

func referenceObject(ref *any, key string) error {
	switch refType := (*ref).(type) {
	case map[string]any:
		{
			result, ok := refType[key]
			if ok {
				*ref = result
				return nil
			}
			return KEY_NOT_FOUND
		}
	case []any:
		{
			referenceArray(ref, refType, key)
			return nil
		}
	default:
		{
			referenceDimension(ref, refType, key)
			return nil
		}
	}
}

func referenceDimension(ref *any, row any, key string) {
	list := make(map[any]map[int]bool)
	switch rowType := row.(type) {
	case map[any]map[int]bool:
		{
			for rowKey := range rowType {
				switch rowKeyType := rowKey.(type) {
				case *map[string]any:
					{
						referenceObjectDimension(list, rowKeyType, key)
					}
					// These case may not be needed
					// case []any:
					// 	{
					// 		panic("unknown case")
					// 	}
					// case *[]any:
					// 	{
					// 		panic("unknown case")
					// 	}
					// default:
					// 	{
					// 		if _, ok := array[k]; !ok {
					// 			array[k] = make(map[int]bool)
					// 		}
					// 		array[k][0] = true
					// 	}
				}
			}
		}
	}

	*ref = list
}

func referenceObjectDimension(list map[any]map[int]bool, row *map[string]any, key string) {
	for rowKey, rowValue := range *row {
		if rowKey == key {
			switch rowValueType := rowValue.(type) {
			case []any:
				{
					for index, item := range rowValueType {
						switch itemType := item.(type) {
						case map[string]any:
							{
								_, ok := list[&itemType]
								if !ok {
									list[&itemType] = make(map[int]bool)
								}
								list[&itemType][index] = true
							}
						default:
							{
								_, ok := list[itemType]
								if !ok {
									list[itemType] = make(map[int]bool)
								}
								list[itemType][index] = true
							}
						}
					}
				}
			case map[string]any:
				{
					_, ok := list[&rowValueType]
					if !ok {
						list[&rowValueType] = make(map[int]bool)
					}
					list[&rowValueType][0] = true
				}
			default:
				{
					list[rowValueType] = make(map[int]bool)
					list[rowValueType][0] = true
				}
			}
		}
	}
}

func referenceArray(ref *any, rows []any, key string) {
	array := make(map[any]map[int]bool)
	for index, item := range rows {
		switch itemType := item.(map[string]any)[key].(type) {
		case map[string]any:
			{
				array[&itemType] = make(map[int]bool)
				array[&itemType][index] = true
			}
		case []any:
			{
				for index, value := range itemType {
					_, ok := array[value]
					if !ok {
						array[value] = map[int]bool{}
					}
					array[value][index] = true
				}

			}
		default:
			{
				array[itemType] = make(map[int]bool)
				array[itemType][0] = true
			}
		}
	}
	*ref = array
}

func toResult(ref any) any {
	switch refType := ref.(type) {
	case map[any]map[int]bool:
		{
			array := make([]any, 0, len(refType))
			for k := range refType {
				array = append(array, k)
			}
			return array
		}
	case []any:
		{
			return refType
		}
	default:
		{
			// array := make([]any, 0)
			// array = append(array, ref)
			return ref
		}
	}
}
