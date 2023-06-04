package sql

import (
	"fmt"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/sentinel"
	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

// TODO
// function needs revision
func joinExec(il IndexedLookup, l Left, r Right) []any {
	result := make([]any, 0)
	for index, indexes := range il {
		for _, referencedIndex := range indexes {
			out := make(map[string]any)
			for key, value := range l[index].(map[string]any) {
				out[key] = value
			}
			for key, value := range r[referencedIndex].(map[string]any) {
				out[key] = value
			}
			result = append(result, out)
		}
		if len(indexes) == 0 {
			out := make(map[string]any)
			for key, value := range l[index].(map[string]any) {
				out[key] = value
			}
			if len(r) > 0 {
				for key := range r[0].(map[string]any) {
					out[key] = nil
				}
			}
			result = append(result, out)
		}
	}
	return result
}

func whereExec(scope *[]any, row any, expr *sqlparser.Where) (bool, error) {
	if expr != nil {
		result, err := cmn.UnWrap[bool](ExprReader(scope, row, expr.Expr))
		if err != nil {
			return false, err
		}
		return result, nil
	}
	return true, nil
}

func selectExec(b cmn.Bucket, row any, id int64, exprs sqlparser.SelectExprs) (any, error) {
	output := make(map[string]any, 0)
	for index, expr := range exprs {
		switch exprType := expr.(type) {
		case *sqlparser.StarExpr:
			{
				for key, value := range starExpr(row, index) {
					output[key] = value
				}
			}
		case *sqlparser.AliasedExpr:
			{
				if expr, ok := exprType.Expr.(*sqlparser.FuncExpr); ok {
					res, err := funcExpr(b, row, expr)
					if err != nil {
						return nil, err
					}
					if len(exprs) == 1 && len(exprType.As.String()) == 0 {
						return res, nil
					}
					output[exprType.As.String()] = res
					continue
				}
				if colName, ok := exprType.Expr.(*sqlparser.ColName); ok {
					if colName.Name.String() == "$GROUPBY" {
						output["$GROUPBY"] = exprType.As.String()
						continue
					}
				}
				name, err := aliasedExpr(exprType)
				if err != nil {
					return nil, err
				}
				id := fmt.Sprintf("%d_%d", id, index)
				result, err := cmn.UnWrap[any](ExprReader(b, row, exprType.Expr, id))
				if err != nil {
					return nil, err
				}
				output[name] = result
			}
		default:
			{
				return nil, sentinel.UNSUPPORTED_CASE
			}
		}
	}
	return output, nil
}
