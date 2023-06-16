package unwind

import (
	"strings"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

func Unwind(jo *[]any, row any, args []any) any {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	list, ok := fnArgs.([]any)
	if !ok {
		return nil
	}
	output := make([]any, 0)
	for _, item := range list {
		innerList, ok := item.([]any)
		if !ok {
			output = append(output, item)
		}
		output = append(output, innerList...)
	}
	return output
}

func readArgs(args []any, row any, jo *[]any) (any, error) {
	var fnArg any
	fnArgReader := func(arg any) error {
		switch argType := arg.(type) {
		case string:
			{
				if strings.HasPrefix(argType, "$.") {
					result, err := cmn.Select(map[string]any{"$": *jo}, argType)
					if err != nil {
						return err
					}
					fnArg = result
					return nil
				}
				result, err := cmn.Select(row.(map[string]any), argType)
				if err != nil {
					return err
				}
				fnArg = result
				return nil
			}
		default:
			{
				fnArg = arg
				return nil
			}
		}
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.ANY}, []functions.Reader{fnArgReader})
	if err != nil {
		return nil, err
	}
	return fnArg, nil
}

func init() {
	cmn.RegisterFunction("unwind", Unwind)
}
