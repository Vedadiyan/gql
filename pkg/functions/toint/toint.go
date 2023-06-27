package toint

import (
	"fmt"
	"strconv"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

func ToInt(jo *[]any, row any, args []any) (any, error) {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	if fnArgs == nil {
		return nil, nil
	}
	switch t := fnArgs.(type) {
	case float64:
		{
			return int(t), nil
		}
	case int32, int:
		{
			return t, nil
		}
	case string:
		{
			value, err := strconv.ParseInt(t, 10, 64)
			if err != nil {
				return nil, err
			}
			return value, nil
		}
	default:
		{
			return nil, fmt.Errorf("unsupported type")
		}
	}
}

func readArgs(args []any, row any, _ *[]any) (any, error) {
	var fnArg any
	err := functions.CheckSingnature(
		args,
		[]functions.ArgTypes{
			functions.ANY,
		},
		[]functions.Reader{
			func(arg any) error {
				fnArg = arg
				return nil
			},
		},
	)
	if err != nil {
		return nil, err
	}
	return fnArg, nil
}

func init() {
	cmn.RegisterFunction("toint", ToInt)
}
