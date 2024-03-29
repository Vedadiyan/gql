package tostring

import (
	"fmt"
	"math"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

func ToString(jo *[]any, row any, args []any) (any, error) {
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
			remainder := math.Mod(t, 1)
			if remainder == 0 {
				return fmt.Sprintf("%d", int64(t)), nil
			}
			return fmt.Sprintf("%f", t), nil
		}
	case int, int16, int32, int64, int8:
		{
			return fmt.Sprintf("%d", t), nil
		}
	default:
		{
			return fmt.Sprintf("%v", t), nil
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
	cmn.RegisterFunction("tostring", ToString)
}
