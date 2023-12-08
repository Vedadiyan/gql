package selectkey

import (
	"fmt"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/functions/common"
	"github.com/vedadiyan/gql/pkg/lookup"
)

func SelectKey(jo *[]any, row any, args []any) (any, error) {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	mapper := fnArgs.(map[string]any)
	value, err := lookup.ReadObject(map[string]any{"$": mapper["bucket"]}, fmt.Sprintf("$.%s", mapper["selector"]))
	if err != nil {
		return nil, err
	}
	if arr, ok := value.([]any); ok {
		if len(arr) == 0 {
			return nil, nil
		}
		return arr, nil
	}
	return common.BoxValue(value), nil
}

func readArgs(args []any, row any, _ *[]any) (any, error) {
	mapper := make(map[string]any)
	bucket := func(args any) error {
		mapper["bucket"] = args
		return nil
	}
	selector := func(args any) error {
		mapper["selector"] = string(args.(cmn.StringValue))
		return nil
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.ANY, functions.STRINGVALUE}, []functions.Reader{bucket, selector})
	if err != nil {
		return nil, err
	}
	return mapper, nil
}

func init() {
	cmn.RegisterFunction("selectkey", SelectKey)
}
