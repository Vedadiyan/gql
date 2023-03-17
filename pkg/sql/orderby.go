package sql

import (
	"fmt"
	"sort"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/sentinel"
)

func orderBy(order map[string]bool, list []any) (err error) {
	if len(order) > 0 {
		sort.Slice(list, func(i, j int) bool {
			defer func() {
				if r := recover(); r != nil {
					err = r.(error)
				}
			}()
			for key, value := range order {
				first, err := rowValue(list, i, key)
				if err != nil {
					panic(err)
				}
				second, err := rowValue(list, j, key)
				if err != nil {
					panic(err)
				}
				if fmt.Sprintf("%T", first) != fmt.Sprintf("%T", second) {
					panic("type mismatch")
				}
				switch t := first.(type) {
				case string:
					{
						if t != second {
							second := second.(string)
							if value {
								return t < second
							}
							return t > second
						}
					}
				case float64:
					{
						if t != second {
							second := second.(float64)
							if value {
								return t < second
							}
							return t > second
						}
					}
				case bool:
					{
						if t != second {
							second := second.(bool)
							firstVal := 0
							if t {
								firstVal = 1
							}
							secondVal := 0
							if second {
								secondVal = 1
							}
							if value {
								return firstVal < secondVal
							}
							return firstVal > secondVal
						}
					}
				default:
					{
						panic(sentinel.UNSUPPORTED_CASE)
					}
				}
			}
			panic(sentinel.UNSUPPORTED_CASE)
		})
	}
	return nil
}

func rowValue(list []any, index int, key string) (any, error) {
	switch rowType := list[index].(type) {
	case map[string]any:
		{
			return cmn.Select(rowType, key)
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
