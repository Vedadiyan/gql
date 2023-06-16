package todouble

import (
	"strconv"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

func ToDouble(jo *[]any, row any, args []any) any {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	if fnArgs == nil {
		return nil
	}
	value, err := strconv.ParseFloat(fnArgs.(string), 64)
	if err != nil {
		return err
	}
	return value
}

func readArgs(args []any, row any, jo *[]any) (any, error) {
	var fnArg any
	fnArgReader := func(arg any) error {
		fnArg = arg
		return nil
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.ANY}, []functions.Reader{fnArgReader})
	if err != nil {
		return nil, err
	}
	return fnArg, nil
}

func init() {
	cmn.RegisterFunction("todouble", ToDouble)
}
