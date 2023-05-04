package todouble

import (
	"strconv"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

func ToDouble(jo *[]any, row any, args []any) any {
	obj, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	value, err := strconv.ParseFloat(obj.(string), 64)
	if err != nil {
		return err
	}
	return value
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
	cmn.RegisterFunction("todouble", ToDouble)
}
