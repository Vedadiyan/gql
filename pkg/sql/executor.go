package sql

import (
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

func selectExec(b cmn.Bucket, row any, exprs sqlparser.SelectExprs, cache map[string]any) (any, error) {
	output := make(map[string]any, 0)
	for index, expr := range exprs {
		switch exprType := expr.(type) {
		case *sqlparser.StarExpr:
			{
				// MUST NOT CALL strExpr
				// MUST CALL ReadExpr
				for key, value := range starExpr(row, index) {
					output[key] = value
				}
			}
		case *sqlparser.AliasedExpr:
			{

				// This feature may no longer be needed
				if colName, ok := exprType.Expr.(*sqlparser.ColName); ok {
					if colName.Name.String() == "$GROUPBY" {
						output["$GROUPBY"] = exprType.As.String()
						continue
					}
				}

				// MUST NOT CALL aliasedExpr
				// MUST CALL ReadExpr
				result, err := aliasedExpr(b, row, index, cache, exprType)
				if err != nil {
					return nil, err
				}
				if _, ok := result.(*FunctionAliased); ok && len(result.Name()) == 0 && len(exprs) == 1 {
					return result.Result(), nil
				}
				output[result.Name()] = result.Result()
			}
		default:
			{
				return nil, sentinel.UNSUPPORTED_CASE
			}
		}
	}
	return output, nil
}

func (c *Context) fromExec() ([]any, error) {
	collect := make([]any, 0)
	count := 0
	if c.from == nil {
		c.from = make([]any, 1)
	}
	for index, row := range c.from {
		if index < c.offset {
			continue
		}
		if count == c.limit {
			break
		}
		count++
		cond, err := whereExec(&c.from, row, c.whereCond)
		if err != nil {
			return nil, err
		}
		if cond {
			collect = append(collect, row)
		}
	}
	return collect, nil
}
