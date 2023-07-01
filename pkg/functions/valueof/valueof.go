package valueof

import (
	"fmt"
	"strings"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

type funcArgs struct {
	key        []string
	bucket     map[string]any
	resultType string
}

func valueOf(jo *[]any, row any, args []any) (any, error) {
	obj, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0)
	for _, item := range obj.key {
		if value, ok := obj.bucket[item]; ok {
			out = append(out, value)
		}
	}
	switch strings.ToLower(obj.resultType) {
	case "array":
		{
			return out, nil
		}
	default:
		{
			if len(out) > 0 {
				return out[0], nil
			}
			return nil, nil
		}
	}
}

func readArgs(args []any, row any, jo *[]any) (*funcArgs, error) {
	var obj funcArgs
	readKey := func(arg any) error {
		switch argType := arg.(type) {
		case string:
			{
				rows, err := cmn.Select(row.(map[string]any), argType)
				if err != nil {
					return err
				}
				if len(rows) == 0 {
					return fmt.Errorf("unexpected array length")
				}
				out := make([]string, 0)
				for _, item := range rows {
					if item == nil {
						continue
					}
					if value, ok := item.(string); ok {
						out = append(out, value)
						continue
					}
					return fmt.Errorf("expected string but recieved `%T`", rows)
				}
				obj.key = out
				return nil
			}
		default:
			{
				return fmt.Errorf("invalid selector")
			}
		}
	}
	readBucket := func(arg any) error {
		switch argType := arg.(type) {
		case string:
			{
				if strings.HasPrefix(argType, "$.") {
					rows, err := cmn.Select(map[string]any{"$": row.(map[string]any)["$"].(map[string]any)}, argType)
					if err != nil {
						return err
					}
					if len(rows) != 1 {
						return fmt.Errorf("unexpected array length")
					}
					if rows[0] == nil {
						return nil
					}
					if bucket, ok := rows[0].(map[string]any); ok {
						obj.bucket = bucket
						return nil
					}
					return fmt.Errorf("expected map[string]any but recieved `%T`", row)
				}
				// result, err := cmn.Select(row.(map[string]any), argType)
				// if err != nil {
				// 	return err
				// }
				// if bucket, ok := result.(map[string]any); ok {
				// 	obj.bucket = bucket
				// 	return nil
				// }
				return fmt.Errorf("expected map[string]any but recieved `%T`", row)
			}
		default:
			{
				return fmt.Errorf("invalid selector")
			}
		}
	}
	readType := func(arg any) error {
		obj.resultType = arg.(string)
		return nil
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.ANY, functions.ANY, functions.STRING}, []functions.Reader{readKey, readBucket, readType})
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func init() {
	cmn.RegisterFunction("valueof", valueOf)
}
