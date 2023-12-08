package hash

import (
	"fmt"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/functions/common"
)

func Hash(jo *[]any, row any, args []any) (any, error) {
	data, hashFunction, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	_ = data
	_ = hashFunction
	return "ok", nil
}

func readArgs(args []any, row any, jo *[]any) ([]byte, string, error) {
	var fnArg []byte
	var hashFunction string
	err := functions.CheckSingnature(
		args,
		[]functions.ArgTypes{
			functions.ANY,
			functions.STRINGVALUE,
		},
		[]functions.Reader{
			func(arg any) error {
				val, err := common.Select(arg, row)
				if err != nil {
					return err
				}
				fmt.Println(val)
				return nil
			},
			func(arg any) error {
				hashFunction = string(arg.(cmn.StringValue))
				return nil
			},
		},
	)
	if err != nil {
		return nil, "", err
	}
	return fnArg, hashFunction, nil
}

func init() {
	cmn.RegisterFunction("hash", Hash)
}
