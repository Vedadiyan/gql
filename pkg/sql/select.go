package sql

import (
	"strconv"
	"strings"
)

func Select(doc Document, key string) (any, error) {
	ref := any(doc)
	sgmnts := strings.Split(key, ".")
	for i := 0; i < len(sgmnts); i++ {
		item := sgmnts[i]
		if strings.HasPrefix(item, "{") && strings.HasSuffix(item, "}") {
			str := strings.TrimPrefix(item, "{")
			str = strings.TrimSuffix(str, "}")
			if str == "?" {
				if i < len(sgmnts)-1 {
					continue
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
			ref = indexer(ref, int(index))
			continue
		}
		ref = load(ref, item)
	}
	array, ok := ref.([]any)
	if !ok {
		return ref, nil
	}
	return array, nil
}

func indexer(ref any, index int) any {
	_ref, ok := (ref).([]any)
	if !ok {
		return nil
	}
	isArray := false
	list := make([]any, 0)
	for i, item := range _ref {
		switch itemType := item.(type) {
		case []any:
			{
				isArray = true
				x := indexer(itemType, index)
				if x != nil {
					list = append(list, x)
				}
			}
		default:
			{
				if i == index {
					return []any{item}
				}
			}
		}
	}
	if isArray {
		if len(list) > 0 {
			return list
		}
	}
	return nil
}

func load(ref any, key string) any {
	switch refType := ref.(type) {
	case map[string]any:
		{
			return ref.(map[string]any)[key]
		}
	case []any:
		{
			_ref := make([]any, 0)
			for _, i := range refType {
				_ref = append(_ref, load(i, key))
			}
			return _ref
		}
	default:
		{
			return refType
		}
	}
}
