package first

import (
	"fmt"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/functions/common"
	"github.com/vedadiyan/gql/pkg/sentinel"
)

func first(jo *[]any, row any, args []any) any {
	list, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	if len(list) > 0 {
		return list[0]
	}
	return nil
}

func readArgs(args []any, row any, jo *[]any) ([]any, error) {
	var fnArg []any
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
				if out, ok := value.([]any); ok {
					fnArg = out
					return nil
				}
				return sentinel.
					EXPECTATION_FAILED.
					Extend(fmt.Sprintf("expected `[]any` but recieved `%T`", value))
			},
		},
	)
	if err != nil {
		return nil, err
	}
	return fnArg, nil
}

func init() {
	cmn.RegisterFunction("first", first)
}
