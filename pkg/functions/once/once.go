package once

import cmn "github.com/vedadiyan/gql/pkg/common"

func Once(jo *[]any, row any, args []any) any {
	return args[0]
}

func init() {
	cmn.RegisterFunction("once", Once)
}
