package concat

import (
	"bytes"
	"fmt"

	cmn "github.com/vedadiyan/gql/pkg/common"
)

func Concat(jo *[]any, row any, args []any) any {
	buffer := bytes.NewBufferString("")
	for _, value := range args {
		buffer.WriteString(fmt.Sprintf("%v", value))
	}
	return buffer.String()
}

func init() {
	cmn.RegisterFunction("concat", Concat)
}
