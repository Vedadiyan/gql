package sql

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

type GroupBy map[string]bool
type Document map[string]any

type Context struct {
	doc        map[string]any
	whereCond  *sqlparser.Where
	selectStmt sqlparser.SelectExprs
	from       []any
	offset     int
	limit      int
	groupBy    map[string]bool
	orderBy    map[string]bool
}

func New(doc Document) *Context {
	ctx := Context{
		doc:     doc,
		offset:  -1,
		limit:   -1,
		groupBy: make(map[string]bool),
		orderBy: make(map[string]bool),
	}
	return &ctx
}
func (c *Context) setSelect(slct *sqlparser.Select) error {
	if slct.With != nil {
		data, err := cteExpr(c.doc, slct.With)
		if err != nil {
			return err
		}
		c.doc = data
	}
	if len(slct.From) > 1 {
		return fmt.Errorf("multiple tables are not supported")
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
	for _, order := range slct.OrderBy {
		name, err := unwrap[string](ExprReader(nil, nil, order.Expr, true))
		if err != nil {
			return err
		}
		switch order.Direction {
		case sqlparser.AscOrder:
			{
				c.orderBy[name] = true

			}
		default:
			{
				c.orderBy[name] = false
			}
		}
	}
	return nil
}
func (c *Context) setFrom(expr sqlparser.TableExpr) error {
	result, err := tableExpr(c.doc, expr)
	if err != nil {
		return err
	}
	c.from = result
	return nil
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
	switch statementType := statement.(type) {
	case *sqlparser.Select:
		{
			return c.setSelect(statementType)
		}
	case *sqlparser.Union:
		{
			left := New(c.doc)
			err := left.prepare(statementType.Left)
			if err != nil {
				return err
			}
			leftRs, err := left.Exec()
			if err != nil {
				return err
			}
			right := New(c.doc)
			err = right.prepare(statementType.Right)
			if err != nil {
				return err
			}
			rightRs, err := right.Exec()
			if err != nil {
				return err
			}
			leftList := leftRs.([]any)
			rightList := rightRs.([]any)
			leftList = append(leftList, rightList...)
			c.from = leftList
			c.selectStmt = sqlparser.SelectExprs{
				&sqlparser.StarExpr{},
			}
			err = c.setLimit(statementType.Limit)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
func (c *Context) Prepare(query string) error {
	sqlStatement, err := sqlparser.Parse(removeComments(query))
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
		cond, err := whereExec(&c.from, row, c.whereCond)
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
						keys = append(keys, fmt.Sprintf("%s:%v", groupBy, value))
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
			result, err := selectExec(&c.from, group, id, &key, c.selectStmt, GroupBy(c.groupBy))
			if err != nil {
				return nil, err
			}
			collect = append(collect, result)
		}
	} else {
		if len(c.from) > 0 && c.from[0] == nil {
			result, err := selectExec(&c.from, c.doc, id, nil, c.selectStmt, nil)
			if err != nil {
				return nil, err
			}
			collect[0] = result
		} else {
			for index, row := range collect {
				result, err := selectExec(&c.from, row, id, nil, c.selectStmt, nil)
				if err != nil {
					return nil, err
				}
				collect[index] = result
			}
		}
	}
	err := orderBy(c.orderBy, collect)
	if err != nil {
		return nil, err
	}
	for index := range c.selectStmt {
		id := fmt.Sprintf("%d_%d", id, index)
		_cache.Delete(id)
	}
	return collect, nil
}
