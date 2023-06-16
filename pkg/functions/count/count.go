package count

import (
	"fmt"
	"strings"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/lookup"
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
	fnArgReader := func(arg any) error {
		switch argType := arg.(type) {
		case string:
			{
				if strings.HasPrefix(argType, "$.") {
					result, err := lookup.ReadObject(map[string]any{"$": *jo}, argType)
					if err != nil {
						return err
					}
					fnArg = result
					return nil
				}
				switch t := row.(type) {
				case map[string]any:
					{
						result, err := lookup.ReadObject(t, argType)
						if err != nil {
							return err
						}
						fnArg = result
					}
				case []any:
					{
						result, err := lookup.ReadObject(map[string]any{"$": t}, fmt.Sprintf("$.%s", argType))
						if err != nil {
							return err
						}
						fnArg = result
					}
				}

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
	cmn.RegisterFunction("count", Count)
}
