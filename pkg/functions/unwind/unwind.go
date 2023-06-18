package unwind

import (
	"fmt"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/functions/common"
	"github.com/vedadiyan/gql/pkg/sentinel"
)

func Unwind(jo *[]any, row any, args []any) (any, error) {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	list, ok := fnArgs.([]any)
	if !ok {
		return nil, nil
	}
	output := make([]any, 0)
	for _, item := range list {
		innerList, ok := item.([]any)
		if !ok {
			output = append(output, item)
		}
		output = append(output, innerList...)
	}
	return output, nil
}

func readArgs(args []any, row any, _ *[]any) (any, error) {
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
	cmn.RegisterFunction("unwind", Unwind)
}
