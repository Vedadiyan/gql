package common

import (
	"strconv"
	"strings"
)

func Select(doc Document, key string) ([]any, error) {
	ref := any(doc)
	keys := strings.Split(key, ".")
	for i := 0; i < len(keys); i++ {
		item := keys[i]
		if !IsIndex(item) {
			ref = load(ref, item)
			continue
		}
		str := GetIndex(item)
		if !IsWildCard(str) {
			index, err := strconv.Atoi(str)
			if err != nil {
				return nil, err
			}
			ref = referenceIndex(ref, int(index))
			continue
		}
		if IsEOA(i, len(keys)) {
			continue
		}
		return ToArray(ref), nil
	}
	return ToArray(ref), nil
}

func referenceIndex(obj any, index int) any {
	ref, ok := (obj).([]any)
	if !ok {
		return nil
	}
	isArray := false
	list := make([]any, 0)
	for i, item := range ref {
		switch itemType := item.(type) {
		case []any:
			{
				isArray = true
				x := referenceIndex(itemType, index)
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
