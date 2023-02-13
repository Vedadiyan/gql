package sql

import (
	"fmt"
	"time"

	"github.com/xwb1989/sqlparser"
)

type Context struct {
	document   map[string]any
	whereCond  *sqlparser.Where
	selectStmt sqlparser.SelectExprs
	from       []any
	offset     int
	limit      int
}

func New(document map[string]any) *Context {
	ctx := Context{
		document: document,
		offset:   -1,
		limit:    -1,
	}
	return &ctx
}

func (c *Context) Prepare(query string) error {
	sqlStatement, err := sqlparser.Parse(query)
	if err != nil {
		return err
	}
	return c.prepare(sqlStatement)
}
func (c *Context) Exec() (any, error) {
	id := time.Now().UnixNano()
	collect := make([]any, 0)
	for _, row := range c.from {
		cond, err := execWhere(&c.from, row, c.whereCond)
		if err != nil {
			return nil, err
		}
		if cond {
			collect = append(collect, row)
		}
	}
	for index, row := range collect {
		result, err := execSelect(&c.from, row, id, c.selectStmt)
		if err != nil {
			return nil, err
		}
		collect[index] = result
	}
	for index := range c.selectStmt {
		id := fmt.Sprintf("%d_%d", id, index)
		_cache.Delete(id)
	}
	return collect, nil
}

func (c *Context) setFrom(expr sqlparser.TableExpr) error {
	switch fromExprType := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		{
			result, err := readAliasedTableExpr(c.document, fromExprType)
			if err != nil {
				return err
			}
			c.from = result
			return nil
		}
	default:
		{
			return UNSUPPORTED_CASE.Extend("invalid from")
		}
	}
}

func (c *Context) prepare(statement sqlparser.Statement) error {
	slct, ok := statement.(*sqlparser.Select)
	if !ok {
		return fmt.Errorf("invalid statement")
	}
	c.setFrom(slct.From[0])
	c.selectStmt = slct.SelectExprs
	c.whereCond = slct.Where
	return nil
}

func readFrom(expr *sqlparser.AliasedTableExpr, from any) ([]any, error) {
	rows, ok := from.([]any)
	if !ok {
		return nil, INVALID_CAST
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

func readAliasedTableExpr(document map[string]any, expr *sqlparser.AliasedTableExpr) ([]any, error) {
	switch exprType := expr.Expr.(type) {
	case sqlparser.TableName:
		{
			objName := exprType.Name
			from, err := From(document, objName.String())
			if err != nil {
				return nil, err
			}

			return readFrom(expr, from)
		}
	case *sqlparser.Subquery:
		{
			innerCtx := New(document)
			innerCtx.prepare(exprType.Select)
			from, err := innerCtx.Exec()
			if err != nil {
				return nil, err
			}
			return readFrom(expr, from)
		}
	default:
		{
			return nil, UNSUPPORTED_CASE.Extend("invalid from")
		}
	}
}

func execWhere(scope *[]any, row any, expr *sqlparser.Where) (bool, error) {
	if expr != nil {
		result, err := unwrap[bool](ExprReader(scope, row, expr.Expr))
		if err != nil {
			return false, err
		}
		return result, nil
	}
	return true, nil
}

func readStarExpr(row any, index int) map[string]any {
	switch rowType := row.(type) {
	case map[string]any:
		{
			return rowType
		}
	default:
		{
			return map[string]any{
				fmt.Sprintf("col_%d", index): row,
			}
		}
	}
}

func readAliasedExpr(expr *sqlparser.AliasedExpr) (string, error) {
	if expr.As.String() != "" {
		return expr.As.String(), nil
	}
	return unwrap[string](ExprReader(nil, nil, expr.Expr, true))
}

func execSelect(from *[]any, row any, id int64, exprs sqlparser.SelectExprs) (map[string]any, error) {
	output := make(map[string]any, 0)
LOOP:
	for index, expr := range exprs {
		switch exprType := expr.(type) {
		case *sqlparser.StarExpr:
			{
				output = readStarExpr(row, index)
				break LOOP
			}
		case *sqlparser.AliasedExpr:
			{
				name, err := readAliasedExpr(exprType)
				if err != nil {
					return nil, err
				}
				id := fmt.Sprintf("%d_%d", id, index)
				result := ExprReader(from, row, exprType.Expr, id)
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
