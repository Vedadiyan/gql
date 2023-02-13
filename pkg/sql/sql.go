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

func readAliasedTableExpr(document map[string]any, expr *sqlparser.AliasedTableExpr) ([]any, error) {
	switch exprType := expr.Expr.(type) {
	case sqlparser.TableName:
		{
			objName := exprType.Name
			from, err := From(document, objName.String())
			if err != nil {
				return nil, err
			}
			name := expr.As.String()
			if name != "" {
				list := make([]any, len(from))
				for index, item := range from {
					list[index] = map[string]any{
						name: item,
					}
				}
				return list, nil
			} else {
				return from, nil
			}
		}
	case *sqlparser.Subquery:
		{
			_sql := New(document)
			_sql.prepare(exprType.Select)
			from, err := _sql.Exec()
			if err != nil {
				return nil, err
			}
			name := expr.As.String()
			if name != "" {
				list := make([]any, len(from.([]any)))
				for index, item := range from.([]any) {
					list[index] = map[string]any{
						name: item,
					}
				}
				return list, nil
			} else {
				return from.([]any), nil
			}
		}
	default:
		{
			return nil, UNSUPPORTED_CASE.Extend("invalid from")
		}
	}
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

func (c *Context) Exec() (any, error) {
	collect := make([]any, 0)
	id := time.Now().UnixNano()
	for _, row := range c.from {
		cond, err := execWhere(&c.from, row, c.whereCond)
		if err != nil {
			return nil, err
		}
		if cond {
			output := make(map[string]any, 0)
			for index, i := range c.selectStmt {
				switch iType := i.(type) {
				case *sqlparser.StarExpr:
					{
						switch rowType := row.(type) {
						case map[string]any:
							{
								output = rowType
							}
						default:
							{
								output = map[string]any{
									fmt.Sprintf("col_%d", index): row,
								}
							}
						}
					}
				case *sqlparser.AliasedExpr:
					{
						var name string
						if iType.As.String() != "" {
							name = iType.As.String()
						} else {
							_name, err := unwrap[string](ExprReader(nil, nil, iType.Expr, true))
							if err != nil {
								return nil, err
							}
							name = _name
						}
						id := fmt.Sprintf("%d_%d", id, index)
						result := ExprReader(&c.from, row, iType.Expr, id)
						output[name] = result
					}
				default:
					{
						c := 10
						_ = c
					}
				}
			}
			collect = append(collect, output)
		}
	}
	for index := range c.selectStmt {
		id := fmt.Sprintf("%d_%d", id, index)
		_cache.Delete(id)
	}
	return collect, nil
}
