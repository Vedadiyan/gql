package sql

import (
	"fmt"

	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

func joinExec(fl IndexedLookup, l Left, r Right) []any {
	result := make([]any, 0)
	for index, value := range fl {
		if len(value) > 0 {
			for _, v := range value {
				out := make(map[string]any)
				for key, value := range r[index].(map[string]any) {
					out[key] = value
				}
				for key, value := range l[v].(map[string]any) {
					out[key] = value
				}
				result = append(result, out)
			}
			continue
		}
		out := make(map[string]any)
		for key, value := range r[index].(map[string]any) {
			out[key] = value
		}
		for key := range l[0].(map[string]any) {
			out[key] = nil
		}
		result = append(result, out)
	}
	return result
}

func whereExec(scope *[]any, row any, expr *sqlparser.Where) (bool, error) {
	if expr != nil {
		result, err := unwrap[bool](ExprReader(scope, row, expr.Expr))
		if err != nil {
			return false, err
		}
		return result, nil
	}
	return true, nil
}

func selectExec(from *[]any, row any, id int64, key *string, exprs sqlparser.SelectExprs, groupBy GroupBy) (map[string]any, error) {
	output := make(map[string]any, 0)
	for index, expr := range exprs {
		switch exprType := expr.(type) {
		case *sqlparser.StarExpr:
			{
				for key, value := range starExpr(row, key, index) {
					output[key] = value
				}
			}
		case *sqlparser.AliasedExpr:
			{
				name, err := aliasedExpr(exprType)
				if err != nil {
					return nil, err
				}
				id := fmt.Sprintf("%d_%d", id, index)
				result := ExprReader(from, row, exprType.Expr, id, groupBy)
				output[name] = result
			}
		default:
			{
				return nil, UNSUPPORTED_CASE
			}
		}
	}
	return output, nil
}
