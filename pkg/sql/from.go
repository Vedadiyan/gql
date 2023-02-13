package sql

import (
	"fmt"
	"strconv"
	"strings"
)

func From(jo map[string]any, key string) ([]any, error) {
	ref := any(jo)
	sgmnts := strings.Split(key, ".")
	for i := 0; i < len(sgmnts); i++ {
		item := sgmnts[i]
		if strings.HasPrefix(item, "{") && strings.HasSuffix(item, "}") {
			str := strings.TrimPrefix(item, "{")
			str = strings.TrimSuffix(str, "}")
			if str == "?" {
				if i < len(sgmnts)-1 {
					return nil, fmt.Errorf("invalid selector")
				}
				array, ok := ref.([]any)
				if !ok {
					return []any{ref}, nil
				}
				return array, nil
			}
			index, err := strconv.ParseInt(str, 10, 32)
			if err != nil {
				return nil, err
			}
			ref = ref.([]any)[index]
			continue
		}
		ref = ref.(map[string]any)[item]
	}
	array, ok := ref.([]any)
	if !ok {
		return []any{ref}, nil
	}
	return array, nil
}
