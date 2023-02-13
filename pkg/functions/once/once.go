package once

import "github.com/vedadiyan/gql/pkg/sql"

func Once(jo *[]any, row any, args []any) any {
	return args[0]
}

func init() {
	sql.RegisterFunction("once", Once)
}
