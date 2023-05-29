package tostring

import (
	"fmt"
	"math"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

func ToString(jo *[]any, row any, args []any) any {
	obj, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	if obj == nil {
		return nil
	}
	switch t := obj.(type) {
	case float64:
		{
			remainder := math.Mod(t, 1)
			if remainder == 0 {
				return fmt.Sprintf("%d", int64(t))
			}
			return fmt.Sprintf("%f", t)
		}
	case int, int16, int32, int64, int8:
		{
			return fmt.Sprintf("%d", t)
		}
	default:
		{
			return fmt.Sprintf("%v", t)
		}
	}
}

func readArgs(args []any, row any, jo *[]any) (any, error) {
	var obj any
	readObj := func(arg any) error {
		obj = arg
		return nil
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.ANY}, []functions.Reader{readObj})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func init() {
	cmn.RegisterFunction("tostring", ToString)
}
