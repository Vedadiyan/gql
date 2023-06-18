package once

import cmn "github.com/vedadiyan/gql/pkg/common"

func Once(jo *[]any, row any, args []any) (any, error) {
	return args[0], nil
}

func init() {
	cmn.RegisterFunction("once", Once)
}
