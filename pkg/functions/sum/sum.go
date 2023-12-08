package sum

import (
	"fmt"
	"strconv"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/functions/common"
)

func Sum(jo *[]any, row any, args []any) (any, error) {
	list, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	total := float64(0)
	for _, item := range functions.Expand(list) {
		value, err := strconv.ParseFloat(fmt.Sprintf("%v", item), 64)
		if err != nil {
			return nil, err
		}
		total += value
	}
	return total, nil
}

func readArgs(args []any, row any, jo *[]any) ([]any, error) {
	var fnArg []any
	isReservedSum := false
	if len(args) == 2 {
		isReservedSum = true
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
				if isReservedSum {
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
	cmn.RegisterFunction("sum", Sum)
	cmn.RegisterFunction("$sum", Sum)
}
