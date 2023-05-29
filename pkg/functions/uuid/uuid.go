package uuid

import (
	"github.com/google/uuid"
	cmn "github.com/vedadiyan/gql/pkg/common"
)

func UUID(jo *[]any, row any, args []any) any {
	uuid := uuid.New()
	return uuid.String()
}
func init() {
	cmn.RegisterFunction("uuid", UUID)
}
