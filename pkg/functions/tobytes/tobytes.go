package asbytes

import (
	"encoding/json"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

func ToBytes(jo *[]any, row any, args []any) (any, error) {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	if fnArgs == nil {
		return nil, nil
	}
	json, err := json.Marshal(fnArgs)
	if err != nil {
		return nil, err
	}
	return json, nil
}

func readArgs(args []any, row any, _ *[]any) (any, error) {
	var fnArg any
	err := functions.CheckSingnature(
		args,
		[]functions.ArgTypes{
			functions.ANY,
		},
		[]functions.Reader{
			func(arg any) error {
				fnArg = arg
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
	cmn.RegisterFunction("tobytes", ToBytes)
	cmn.RegisterFunction("asbytes", ToBytes)
}
