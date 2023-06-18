package uuid

import (
	"github.com/google/uuid"
	cmn "github.com/vedadiyan/gql/pkg/common"
)

func UUID(jo *[]any, row any, args []any) (any, error) {
	uuid := uuid.New()
	return uuid.String(), nil
}
func init() {
	cmn.RegisterFunction("uuid", UUID)
}
