package valueof

import (
	"fmt"
	"strings"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

type funcArgs struct {
	key    string
	bucket map[string]any
}

func valueOf(jo *[]any, row any, args []any) any {
	obj, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	if value, ok := obj.bucket[obj.key]; ok {
		return value
	}
	return nil
}

func readArgs(args []any, row any, jo *[]any) (*funcArgs, error) {
	var obj funcArgs
	readKey := func(arg any) error {
		switch argType := arg.(type) {
		case string:
			{
				result, err := cmn.Select(row.(map[string]any), argType)
				if err != nil {
					return err
				}
				rows, ok := result.([]any)
				if !ok {
					return fmt.Errorf("unexpected result")
				}
				if len(rows) != 1 {
					return fmt.Errorf("unexpected array length")
				}
				if value, ok := rows[0].(string); ok {
					obj.key = value
					return nil
				}
				return fmt.Errorf("expected string but recieved `%T`", result)
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
					result, err := cmn.Select(map[string]any{"$": row.(map[string]any)["$"].(map[string]any)}, argType)
					if err != nil {
						return err
					}
					rows, ok := result.([]any)
					if !ok {
						return fmt.Errorf("unexpected result")
					}
					if len(rows) != 1 {
						return fmt.Errorf("unexpected array length")
					}
					if bucket, ok := rows[0].(map[string]any); ok {
						obj.bucket = bucket
						return nil
					}
					return fmt.Errorf("expected map[string]any but recieved `%T`", result)
				}
				result, err := cmn.Select(row.(map[string]any), argType)
				if err != nil {
					return err
				}
				if bucket, ok := result.(map[string]any); ok {
					obj.bucket = bucket
					return nil
				}
				return fmt.Errorf("expected map[string]any but recieved `%T`", result)
			}
		default:
			{
				return fmt.Errorf("invalid selector")
			}
		}
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.ANY, functions.ANY}, []functions.Reader{readKey, readBucket})
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func init() {
	cmn.RegisterFunction("valueof", valueOf)
}
