package nullifempty

import (
	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/functions/common"
)

func NullIfEmpty(jo *[]any, row any, args []any) (any, error) {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	list, ok := fnArgs.([]any)
	if !ok {
		return common.BoxValue(fnArgs), nil
	}
	if len(list) != 0 {
		return list, nil
	}
	return nil, nil
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
				value, err := common.Select(arg, row)
				if err != nil {
					return err
				}
				fnArg = value
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
	cmn.RegisterFunction("nullifempty", NullIfEmpty)
}
