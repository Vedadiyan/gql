package count

import (
	cmn "github.com/vedadiyan/gql/pkg/common"
)

func Count(jo *[]any, _ any, args []any) (any, error) {
	return len(*jo), nil
}

func init() {
	cmn.RegisterFunction("count", Count)
}
