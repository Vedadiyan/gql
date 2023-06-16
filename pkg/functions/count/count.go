package count

import (
	"fmt"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/functions/common"
	"github.com/vedadiyan/gql/pkg/sentinel"
)

func Count(jo *[]any, row any, args []any) any {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	list, ok := fnArgs.([]any)
	if !ok {
		return nil
	}
	return len(list)
}

func readArgs(args []any, row any, jo *[]any) (any, error) {
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
				switch t := value.(type) {
				case map[string]any, []any:
					{
						fnArg = t
						return nil
					}
				default:
					{
						return sentinel.
							EXPECTATION_FAILED.
							Extend(fmt.Sprintf("expected either `map[string]any` or `[]any` but recieved `%T`", t))
					}
				}
			},
		},
	)
	if err != nil {
		return nil, err
	}
	return fnArg, nil
}

func init() {
	cmn.RegisterFunction("count", Count)
}
