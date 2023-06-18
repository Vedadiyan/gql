package common

import (
	"fmt"
	"strings"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/sentinel"
)

func Select(arg any, row any) (any, error) {
	switch argType := arg.(type) {
	case string:
		{
			if strings.HasPrefix(argType, "$.") {
				rows, err := cmn.Select(map[string]any{"$": row.(map[string]any)["$"].(map[string]any)}, argType)
				if err != nil {
					return nil, err
				}
				if len(rows) != 1 {
					return nil, sentinel.EXPECTATION_FAILED.Extend(fmt.Sprintf("unexpected length of array `%d`", len(rows)))
				}
				return rows, nil
			}
			result, err := cmn.Select(row.(map[string]any), argType)
			if err != nil {
				return nil, err
			}
			return result, nil
		}
	default:
		{
			return arg, nil
		}
	}
}
