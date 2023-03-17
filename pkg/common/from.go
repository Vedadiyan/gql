package common

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/vedadiyan/gql/pkg/sentinel"
	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

func From(doc Document, key string) ([]any, error) {
	ref := any(doc)
	sgmnts := strings.Split(key, ".")
	for i := 0; i < len(sgmnts); i++ {
		item := sgmnts[i]
		if !IsIndex(item) {
			ref = ref.(map[string]any)[item]
			continue
		}
		str := GetIndex(item)
		if IsWildCard(str) {
			index, err := strconv.ParseInt(str, 10, 32)
			if err != nil {
				return nil, err
			}
			ref = ref.([]any)[index]
			continue
		}
		if IsEOA(i, len(sgmnts)) {
			return nil, fmt.Errorf("invalid selector")
		}
		array, ok := ref.([]any)
		if !ok {
			return []any{ref}, nil
		}
		return array, nil
	}
	array, ok := ref.([]any)
	if !ok {
		return []any{ref}, nil
	}
	return array, nil
}

func ReadFrom(expr *sqlparser.AliasedTableExpr, from any) ([]any, error) {
	rows, ok := from.([]any)
	if !ok {
		return nil, sentinel.INVALID_CAST
	}
	name := expr.As.String()
	if name != "" {
		list := make([]any, len(rows))
		for index, item := range rows {
			list[index] = map[string]any{
				name: item,
			}
		}
		return list, nil
	}
	return rows, nil
}
