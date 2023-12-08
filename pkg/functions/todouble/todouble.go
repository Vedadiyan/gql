package todouble

import (
	"fmt"
	"strconv"

	"github.com/vedadiyan/gql/pkg/common"
	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/sentinel"
)

func ToDouble(jo *[]any, row any, args []any) (any, error) {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	if fnArgs == nil {
		return nil, nil
	}
	switch t := fnArgs.(type) {
	case string:
		{
			value, err := strconv.ParseFloat(t, 64)
			if err != nil {
				return nil, err
			}
			return value, nil
		}
	case common.StringValue:
		{
			value, err := strconv.ParseFloat(string(t), 64)
			if err != nil {
				return nil, err
			}
			return value, nil
		}
	}
	return nil, sentinel.UNSUPPORTED_CASE.Extend(fmt.Sprintf("%T is not supported", fnArgs))
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
	cmn.RegisterFunction("todouble", ToDouble)
}
