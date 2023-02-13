package sql

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/xwb1989/sqlparser"
)

type GroupBy map[string]bool

type Context struct {
	document   map[string]any
	whereCond  *sqlparser.Where
	selectStmt sqlparser.SelectExprs
	from       []any
	offset     int
	limit      int
	groupBy    map[string]bool
}

func New(document map[string]any) *Context {
	ctx := Context{
		document: document,
		offset:   -1,
		limit:    -1,
		groupBy:  make(map[string]bool),
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
	count := 0
	for index, row := range c.from {
		if index < c.offset {
			continue
		}
		if count == c.limit {
			break
		}
		count++
		cond, err := execWhere(&c.from, row, c.whereCond)
		if err != nil {
			return nil, err
		}
		if cond {
			collect = append(collect, row)
		}
	}
	if len(c.groupBy) > 0 {
		groupped := make(map[string][]any)
		for _, row := range collect {
			var buffer bytes.Buffer
			keys := make([]string, 0)
			for groupBy := range c.groupBy {
				value, err := Select(row.(map[string]any), groupBy)
				if err != nil {
					return nil, err
				}
				switch value.(type) {
				case map[string]any, []any:
					{
						return nil, UNSUPPORTED_CASE.Extend("only value types can be used in group by")
					}
				default:
					{
						keys = append(keys, fmt.Sprintf("%v", value))
					}
				}
			}
			sort.Strings(keys)
			for _, key := range keys {
				buffer.WriteString(key)
				buffer.WriteString(".#.")
			}
			key := string(buffer.Bytes()[:buffer.Len()-3])
			_, ok := groupped[key]
			if !ok {
				groupped[key] = make([]any, 0)
			}
			groupped[key] = append(groupped[key], row)
		}
		collect = make([]any, 0)
		for key, group := range groupped {
			result, err := execSelect(&c.from, group, id, &key, c.selectStmt, GroupBy(c.groupBy))
			if err != nil {
				return nil, err
			}
			collect = append(collect, result)
		}
	} else {
		for index, row := range collect {
			result, err := execSelect(&c.from, row, id, nil, c.selectStmt, nil)
			if err != nil {
				return nil, err
			}
			collect[index] = result
		}
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

func (c *Context) setLimit(expr *sqlparser.Limit) error {
	if expr == nil {
		return nil
	}
	if expr.Offset != nil {
		offset, err := unwrap[float64](ExprReader(nil, nil, expr.Offset))
		if err != nil {
			return err
		}
		c.offset = int(offset)
	}
	limit, err := unwrap[float64](ExprReader(nil, nil, expr.Rowcount))
	if err != nil {
		return err
	}
	c.limit = int(limit)
	return nil
}

func (c *Context) setGroupBy(expr sqlparser.GroupBy) error {
	for _, groupBy := range expr {
		result, err := unwrap[string](ExprReader(nil, nil, groupBy, true))
		if err != nil {
			return err
		}
		c.groupBy[result] = true
	}
	return nil
}

func (c *Context) prepare(statement sqlparser.Statement) error {
	slct, ok := statement.(*sqlparser.Select)
	if !ok {
		return fmt.Errorf("invalid statement")
	}
	err := c.setFrom(slct.From[0])
	if err != nil {
		return err
	}
	err = c.setLimit(slct.Limit)
	if err != nil {
		return err
	}
	err = c.setGroupBy(slct.GroupBy)
	if err != nil {
		return err
	}
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

func readStarExpr(row any, key *string, index int) map[string]any {
	switch rowType := row.(type) {
	case map[string]any:
		{
			return rowType
		}
	default:
		{
			if key == nil {
				return map[string]any{
					fmt.Sprintf("col_%d", index): row,
				}
			}
			return map[string]any{
				*key: row,
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

func execSelect(from *[]any, row any, id int64, key *string, exprs sqlparser.SelectExprs, groupBy GroupBy) (map[string]any, error) {
	output := make(map[string]any, 0)
LOOP:
	for index, expr := range exprs {
		switch exprType := expr.(type) {
		case *sqlparser.StarExpr:
			{
				output = readStarExpr(row, key, index)
				break LOOP
			}
		case *sqlparser.AliasedExpr:
			{
				name, err := readAliasedExpr(exprType)
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
