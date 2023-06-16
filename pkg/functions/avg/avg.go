package avg

import (
	"fmt"
	"strconv"
	"strings"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

func Avg(jo *[]any, row any, args []any) any {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	list, ok := fnArgs.([]any)
	if !ok {
		return nil
	}
	total := float64(0)
	for _, item := range list {
		value, err := strconv.ParseFloat(fmt.Sprintf("%v", item), 64)
		if err != nil {
			return err
		}
		total += value
	}
	avg := total / float64(len(list))
	return avg
}

func readArgs(args []any, row any, jo *[]any) (any, error) {
	var fnArg any
	fnArgReader := func(arg any) error {
		switch argType := arg.(type) {
		case string:
			{
				if strings.HasPrefix(argType, "$.") {
					result, err := cmn.Select(map[string]any{"$": *jo}, argType)
					if err != nil {
						return err
					}
					fnArg = result
					return nil
				}
				result, err := cmn.Select(row.(map[string]any), argType)
				if err != nil {
					return err
				}
				fnArg = result
				return nil

			}
		default:
			{
				fnArg = arg
				return nil
			}
		}
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.ANY}, []functions.Reader{fnArgReader})
	if err != nil {
		return nil, err
	}
	return fnArg, nil
}

func init() {
	cmn.RegisterFunction("avg", Avg)
}
