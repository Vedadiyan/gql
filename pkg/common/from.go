package common

import (
	"github.com/vedadiyan/gql/pkg/lookup"
	"github.com/vedadiyan/gql/pkg/sentinel"
	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

func From(doc Document, key string) ([]any, error) {
	result, err := lookup.ReadObject(doc, key)
	if err != nil {
		return nil, err
	}
	switch t := result.(type) {
	case []any:
		{
			return t, nil
		}
	default:
		{
			return []any{t}, nil
		}
	}
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
