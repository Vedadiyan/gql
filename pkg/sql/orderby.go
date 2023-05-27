package sql

import (
	"fmt"
	"sort"

	"github.com/vedadiyan/gql/pkg/lookup"
	"github.com/vedadiyan/gql/pkg/sentinel"
)

func Sort(slice []any, key string, desc bool) {
	sort.Slice(slice, func(i, j int) bool {
		_1 := slice[i].(map[string]any)[key]
		if _1 == nil {
			return desc
		}
		_2 := slice[j].(map[string]any)[key]
		if _2 == nil {
			return !desc
		}
		if fmt.Sprintf("%T", _1) != fmt.Sprintf("%T", _2) {
			return false
		}
		switch _1.(type) {
		case float64:
			{
				return compare[float64](_1, _2) != desc
			}
		case float32:
			{
				return compare[float32](_1, _2) != desc
			}
		case int64:
			{
				return compare[int64](_1, _2) != desc
			}
		case int:
			{
				return compare[int](_1, _2) != desc
			}
		case int32:
			{
				return compare[int32](_1, _2) != desc
			}
		case int16:
			{
				return compare[int16](_1, _2) != desc
			}
		case int8:
			{
				return compare[int8](_1, _2) != desc
			}
		case string:
			{
				return compare[string](_1, _2) != desc
			}
		case bool:
			{
				return compare[string](fmt.Sprintf("%v", _1), fmt.Sprintf("%v", _2)) != desc
			}

		}
		return false
	})
}

func compare[T float64 | float32 | int | int16 | int32 | int64 | int8 | string](first any, second any) bool {
	_1 := first.(T)
	_2 := second.(T)
	return _1 > _2
}

type KeyValue struct {
	Key   string
	Value bool
}

func orderBy(order []KeyValue, list []any) (err error) {
	if len(order) == 0 {
		return nil
	}
	Sort(list, order[0].Key, order[0].Value)
	for idx := 1; idx < len(order); idx++ {
		prev := "#"
		bucket := make([]any, 0)
		var firstIndex = 0
		for index, i := range list {
			var _key string
			arr := order[:idx]
			for _, key := range arr {
				value := i.(map[string]any)[key.Key]
				_key = _key + fmt.Sprintf("%v-", value)
			}
			if prev != "#" && _key != prev {
				prev = "#"
				Sort(bucket, order[idx].Key, order[idx].Value)
				j := 0
				for x := firstIndex; x < index; x++ {
					list[x] = bucket[j]
					j++
				}
				firstIndex = index
				bucket = make([]any, 0)
			}
			prev = _key
			bucket = append(bucket, i)
		}
		Sort(bucket, order[idx].Key, order[idx].Value)
		j := 0
		for x := firstIndex; x < len(list); x++ {
			list[x] = bucket[j]
			j++
		}
	}
	return nil
}

// func orderBy(order map[string]bool, list []any) (err error) {
// 	if len(order) > 0 {
// 		sort.Slice(list, func(i, j int) bool {
// 			defer func() {
// 				if r := recover(); r != nil {
// 					err = r.(error)
// 				}
// 			}()
// 			for key, desc := range order {
// 				first, err := rowdesc(list, i, key)
// 				if err != nil {
// 					panic(err)
// 				}
// 				if first == nil {
// 					return false
// 				}
// 				second, err := rowdesc(list, j, key)
// 				if err != nil {
// 					panic(err)
// 				}
// 				if second == nil {
// 					return true
// 				}
// 				if fmt.Sprintf("%T", first) != fmt.Sprintf("%T", second) {
// 					panic("type mismatch")
// 				}
// 				switch t := first.(type) {
// 				case string:
// 					{
// 						t2 := second.(string)
// 						if t != t2 {
// 							if desc {
// 								return t < t2
// 							}
// 							return t > t2
// 						}
// 					}
// 				case float64:
// 					{
// 						t2 := second.(float64)
// 						if t != t2 {
// 							if desc {
// 								return t < t2
// 							}
// 							return t > t2
// 						}
// 						c := 10
// 						_ = c
// 					}
// 				case int64:
// 					{
// 						t2 := second.(int64)
// 						if t != t2 {
// 							if desc {
// 								return t < t2
// 							}
// 							return t > t2
// 						}
// 					}
// 				case bool:
// 					{
// 						t2 := second.(bool)
// 						if t != t2 {
// 							firstVal := 0
// 							if t {
// 								firstVal = 1
// 							}
// 							secondVal := 0
// 							if t2 {
// 								secondVal = 1
// 							}
// 							if desc {
// 								return firstVal < secondVal
// 							}
// 							return firstVal > secondVal
// 						}
// 					}
// 				default:
// 					{
// 						panic(sentinel.UNSUPPORTED_CASE)
// 					}
// 				}
// 			}
// 			panic(sentinel.UNSUPPORTED_CASE)
// 		})
// 	}
// 	return nil
// }

func rowdesc(list []any, index int, key string) (any, error) {
	switch rowType := list[index].(type) {
	case map[string]any:
		{
			return lookup.ReadObject(rowType, key)
		}
	case []any:
		{
			return nil, sentinel.UNSUPPORTED_CASE
		}
	default:
		{
			return rowType, nil
		}
	}
}
