package concat

import (
	"bytes"
	"fmt"
	"time"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions/common"
	"github.com/vedadiyan/gql/pkg/monitor"
)

func Concat(jo *[]any, row any, args []any) (any, error) {
	monitor.SubmitToWorkerQueue(*jo, func() error {
		<-time.After(time.Second * 10)
		return nil
	})
	buffer := bytes.NewBufferString("")
	for _, value := range args {
		val, err := common.Select(value, row)
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
