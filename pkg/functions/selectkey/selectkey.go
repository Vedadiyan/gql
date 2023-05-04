package selectkey

import (
	"fmt"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/lookup"
)

func SelectKey(jo *[]any, row any, args []any) any {
	obj, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	mapper := obj.(map[string]any)
	value, err := lookup.ReadObject(map[string]any{"$": mapper["bucket"]}, fmt.Sprintf("$.%s", mapper["selector"]))
	if err != nil {
		return err
	}
	if arr, ok := value.([]any); ok {
		return arr[0]
	}
	return value
}

func readArgs(args []any, row any, jo *[]any) (any, error) {
	mapper := make(map[string]any)
	bucket := func(args any) error {
		mapper["bucket"] = args
		return nil
	}
	selector := func(args any) error {
		mapper["selector"] = args.(string)
		return nil
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.ANY, functions.STRING}, []functions.Reader{bucket, selector})
	if err != nil {
		return nil, err
	}
	return mapper, nil
}

func init() {
	cmn.RegisterFunction("selectkey", SelectKey)
}