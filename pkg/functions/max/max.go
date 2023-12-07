package max

import (
	"fmt"
	"math"
	"strconv"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/functions/common"
)

func Max(jo *[]any, row any, args []any) (any, error) {
	list, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	max := math.MaxFloat64 * -1
	for _, item := range functions.Expand(list) {
		value, err := strconv.ParseFloat(fmt.Sprintf("%v", item), 64)
		if err != nil {
			return nil, err
		}
		if value > max {
			max = value
		}
	}
	return max, nil
}

func readArgs(args []any, row any, jo *[]any) ([]any, error) {
	var fnArg []any
	isReservedMax := false
	if len(args) == 2 {
		isReservedMax = true
		args = args[:1]
	}
	err := functions.CheckSingnature(
		args,
		[]functions.ArgTypes{
			functions.ANY,
		},
		[]functions.Reader{
			func(arg any) error {
				out := make([]any, 0)
				if isReservedMax {
					for _, row := range *jo {
						value, err := common.Select(arg, row)
						if err != nil {
							return err
						}
						out = append(out, value)
					}
					fnArg = out
					return nil
				}
				value, err := common.Select(arg, row)
				if err != nil {
					return err
				}
				out = append(out, value)
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
	cmn.RegisterFunction("max", Max)
}
