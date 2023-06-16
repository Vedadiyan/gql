package tomap

import (
	"fmt"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
)

type Args struct {
	bucket []any
	key    string
	value  string
}

func ToMap(jo *[]any, row any, args []any) any {
	fnArgs, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	mapper := make(map[string]any)
	for _, item := range fnArgs.bucket {
		_item, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("expected map but recieved %T", item)
		}
		key, ok := _item[fnArgs.key].(string)
		if !ok {
			return fmt.Errorf("expected string but recieved %T", _item[fnArgs.key])
		}
		if _, ok := mapper[key]; ok {
			return fmt.Errorf("duplicate key `%s` in map", key)
		}
		mapper[key] = _item[fnArgs.value]
	}
	return mapper
}

func readArgs(args []any, row any, jo *[]any) (*Args, error) {
	fnArgs := Args{}
	readBucket := func(arg any) error {
		if arr, ok := arg.([]any); ok {
			fnArgs.bucket = arr
			return nil
		}
		return fmt.Errorf("expected an array of objects but recieved %T", arg)
	}
	readKey := func(arg any) error {
		if value, ok := arg.(string); ok {
			fnArgs.key = value
			return nil
		}
		return fmt.Errorf("expected string but recieved %T", arg)
	}
	readValue := func(arg any) error {
		if value, ok := arg.(string); ok {
			fnArgs.value = value
			return nil
		}
		return fmt.Errorf("expected string but recieved %T", arg)
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.ANY, functions.STRING, functions.STRING}, []functions.Reader{readBucket, readKey, readValue})
	if err != nil {
		return nil, err
	}
	return &fnArgs, nil
}

func init() {
	cmn.RegisterFunction("tomap", ToMap)
}
