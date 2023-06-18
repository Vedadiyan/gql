package min

import (
	"fmt"
	"math"
	"strconv"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/functions/common"
)

func Min(jo *[]any, row any, args []any) (any, error) {
	list, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	min := math.MaxFloat64
	for _, item := range list {
		value, err := strconv.ParseFloat(fmt.Sprintf("%v", item), 64)
		if err != nil {
			return nil, err
		}
		if value < min {
			min = value
		}
	}
	return min, nil
}

func readArgs(args []any, _ any, jo *[]any) ([]any, error) {
	var fnArg []any
	err := functions.CheckSingnature(
		args,
		[]functions.ArgTypes{
			functions.ANY,
		},
		[]functions.Reader{
			func(arg any) error {
				out := make([]any, 0)
				for _, row := range *jo {
					value, err := common.Select(arg, row)
					if err != nil {
						return err
					}
					out = append(out, value)
				}
				fnArg = out
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
	cmn.RegisterFunction("min", Min)
}
