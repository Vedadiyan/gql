package toint

import (
	"strconv"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

func ToInt(jo *[]any, row any, args []any) any {
	obj, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	if obj == nil {
		return nil
	}
	value, err := strconv.ParseInt(obj.(string), 10, 64)
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
	cmn.RegisterFunction("toint", ToInt)
}
