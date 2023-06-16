package toarray

import (
	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

func ToArray(jo *[]any, row any, args []any) any {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	if fnArgs == nil {
		return []any{}
	}
	return []any{fnArgs}
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
	cmn.RegisterFunction("toarray", ToArray)
}
