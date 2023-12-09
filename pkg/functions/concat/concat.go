package concat

import (
	"bytes"
	"fmt"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions/common"
)

func Concat(jo *[]any, row any, args []any) (any, error) {
	buffer := bytes.NewBufferString("")
	for _, value := range args {
		val, err := common.Select(value, row)
		fmt.Println("DEBUG:", value, fmt.Sprintf("%T", value))
		if err != nil {
			return nil, err
		}
		buffer.WriteString(fmt.Sprintf("%v", val))
	}
	return common.ToStringValue(buffer.String()), nil
}

func init() {
	cmn.RegisterFunction("concat", Concat)
}
