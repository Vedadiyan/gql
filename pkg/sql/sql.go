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

func (c *Context) prepare(statement sqlparser.Statement) error {
	slct, ok := statement.(*sqlparser.Select)
	if !ok {
		return fmt.Errorf("invalid statement")
	}
	fromExpr := slct.From[0]
	switch fromExprType := fromExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		{
			switch exprType := fromExprType.Expr.(type) {
			case sqlparser.TableName:
				{
					objName := exprType.Name
					from, err := From(c.document, objName.String())
					if err != nil {
						return err
					}
					name := fromExprType.As.String()
					if name != "" {
						list := make([]any, len(from))
						for index, item := range from {
							list[index] = map[string]any{
								name: item,
							}
						}
						c.from = list
					} else {
						c.from = from
					}
				}
			case *sqlparser.Subquery:
				{
					_sql := New(c.document)
					_sql.prepare(exprType.Select)
					from, err := _sql.Exec()
					if err != nil {
						return err
					}
					name := fromExprType.As.String()
					if name != "" {
						list := make([]any, len(from.([]any)))
						for index, item := range from.([]any) {
							list[index] = map[string]any{
								name: item,
							}
						}
						c.from = list
					} else {
						c.from = from.([]any)
					}
				}
			default:
				{
					return UNSUPPORTED_CASE.Extend("invalid from")
				}
			}
		}
	default:
		{
			return UNSUPPORTED_CASE.Extend("invalid from")
		}
	}

	c.selectStmt = slct.SelectExprs
	c.whereCond = slct.Where
	return nil
}

func (c *Context) Exec() (any, error) {
	collect := make([]any, 0)
	id := time.Now().UnixNano()
	for _, row := range c.from {
		final := true
		if c.whereCond != nil {
			result, err := unwrap[bool](ExprReader(&c.from, row, c.whereCond.Expr))
			if err != nil {
				return nil, err
			}
			final = result
		}
		if final {
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
