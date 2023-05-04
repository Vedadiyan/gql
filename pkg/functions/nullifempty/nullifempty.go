package nullifempty

import (
	"strings"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/lookup"
)

func NullIfEmpty(jo *[]any, row any, args []any) any {
	obj, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	list, ok := obj.([]any)
	if !ok {
		return obj
	}
	if len(list) != 0 {
		return list
	}
	return nil
}

func readArgs(args []any, row any, jo *[]any) (any, error) {
	var obj any
	readObj := func(arg any) error {
		switch argType := arg.(type) {
		case string:
			{
				if strings.HasPrefix(argType, "$.") {
					result, err := lookup.ReadObject(map[string]any{"$": *jo}, argType)
					if err != nil {
						return err
					}
					obj = result
					return nil
				}
				result, err := lookup.ReadObject(row.(map[string]any), argType)
				if err != nil {
					return err
				}
				obj = result
				return nil

			}
		default:
			{
				obj = arg
				return nil
			}
		}
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.ANY}, []functions.Reader{readObj})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func init() {
	cmn.RegisterFunction("nullifempty", NullIfEmpty)
}