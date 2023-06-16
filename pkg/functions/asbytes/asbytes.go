package asbytes

import (
	"encoding/json"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

func AsBytes(jo *[]any, row any, args []any) any {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	if fnArgs == nil {
		return nil
	}
	json, err := json.Marshal(fnArgs)
	if err != nil {
		return err
	}
	return json
}

func readArgs(args []any, row any, jo *[]any) (any, error) {
	var fnArg any
	fnArgReader := func(arg any) error {
		fnArg = arg
		return nil
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.ANY}, []functions.Reader{fnArgReader})
	if err != nil {
		return nil, err
	}
	return fnArg, nil
}

func init() {
	cmn.RegisterFunction("asbytes", AsBytes)
}
