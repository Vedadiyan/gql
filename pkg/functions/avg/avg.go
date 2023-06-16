package avg

import (
	"fmt"
	"strconv"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/functions/common"
	"github.com/vedadiyan/gql/pkg/sentinel"
)

func Avg(jo *[]any, row any, args []any) (any, error) {
	list, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	total := float64(0)
	for _, item := range list {
		value, err := strconv.ParseFloat(fmt.Sprintf("%v", item), 64)
		if err != nil {
			return nil, err
		}
		total += value
	}
	avg := total / float64(len(list))
	return avg, nil
}

func readArgs(args []any, row any, _ *[]any) ([]any, error) {
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
					Extend(fmt.Sprintf("expected `[]any` but received `%T`", value))
			},
		},
	)
	if err != nil {
		return nil, err
	}
	return fnArg, nil
}

func init() {
	cmn.RegisterFunction("avg", Avg)
}
