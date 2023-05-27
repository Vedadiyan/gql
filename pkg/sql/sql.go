package sql

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/lookup"
	"github.com/vedadiyan/gql/pkg/sentinel"
	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

type Context struct {
	doc        map[string]any
	whereCond  *sqlparser.Where
	selectStmt sqlparser.SelectExprs
	from       []any
	offset     int
	limit      int
	groupBy    map[string]bool
	havingCond *sqlparser.Where
	orderBy    []KeyValue
}

func new(doc cmn.Document, init bool) *Context {
	var _doc cmn.Document
	if init {
		_doc = map[string]any{"$": doc}
	} else {
		_doc = doc
	}
	ctx := Context{
		doc:     _doc,
		offset:  -1,
		limit:   -1,
		groupBy: make(map[string]bool),
		orderBy: make([]KeyValue, 0),
	}
	return &ctx
}

func New(doc cmn.Document) *Context {
	return new(doc, true)
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
	c.havingCond = slct.Having
	c.selectStmt = slct.SelectExprs
	c.whereCond = slct.Where
	for _, order := range slct.OrderBy {
		name, err := cmn.UnWrap[string](ExprReader(nil, nil, order.Expr, true))
		if err != nil {
			return err
		}
		switch order.Direction {
		case sqlparser.AscOrder:
			{
				c.orderBy = append(c.orderBy, KeyValue{
					Key:   name,
					Value: true,
				})

			}
		default:
			{
				c.orderBy = append(c.orderBy, KeyValue{
					Key:   name,
					Value: false,
				})
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
		offset, err := cmn.UnWrap[float64](ExprReader(nil, nil, expr.Offset))
		if err != nil {
			return err
		}
		c.offset = int(offset)
	}
	limit, err := cmn.UnWrap[float64](ExprReader(nil, nil, expr.Rowcount))
	if err != nil {
		return err
	}
	c.limit = int(limit)
	return nil
}
func (c *Context) setGroupBy(expr sqlparser.GroupBy) error {
	for _, groupBy := range expr {
		result, err := cmn.UnWrap[string](ExprReader(nil, nil, groupBy, true))
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
			left := new(c.doc, false)
			err := left.prepare(statementType.Left)
			if err != nil {
				return err
			}
			leftRs, err := left.Exec()
			if err != nil {
				return err
			}
			right := new(c.doc, false)
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
	query = cmn.RemoveComments(query)
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
	if len(c.groupBy) > 0 {
		groupped := make(map[string][]any)
		for _, row := range collect {
			var buffer bytes.Buffer
			keys := make([]string, 0)
			for groupBy := range c.groupBy {
				value, err := lookup.ReadObject(row.(map[string]any), groupBy)
				if err != nil {
					return nil, err
				}
				switch value.(type) {
				case map[string]any, []any:
					{
						return nil, sentinel.UNSUPPORTED_CASE.Extend("only value types can be used in group by")
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
		for _, group := range groupped {
			_array := make([]any, 0)
			cond, err := whereExec(nil, group, c.havingCond)
			if err != nil {
				return nil, err
			}
			if !cond {
				continue
			}
			result, err := selectExec(&c.from, group, id, c.selectStmt)
			if err != nil {
				return nil, err
			}
			// QUICK FIX
			_result := result.(map[string]any)
			groupByName := "_grouped"
			if value, ok := _result["$GROUPBY"]; ok {
				groupByName = value.(string)
				delete(_result, "$GROUPBY")
			}
			for key, value := range _result {
				if _, ok := c.groupBy[key]; ok {
					_result[key] = value.([]any)[0]
					continue
				}
				for index, value := range value.([]any) {
					if index >= len(_array) {
						_array = append(_array, make(map[string]any))
					}
					_array[index].(map[string]any)[key] = value
				}
				delete(_result, key)
			}
			_result[groupByName] = _array
			// END QUICK FIX
			collect = append(collect, _result[groupByName])
		}
		c := 10
		_ = c
	} else {
		if len(c.from) > 0 && c.from[0] == nil {
			result, err := selectExec(&c.from, c.doc, id, c.selectStmt)
			if err != nil {
				return nil, err
			}
			return result, nil
		} else {
			for index, row := range collect {
				result, err := selectExec(&c.from, row, id, c.selectStmt)
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
		cmn.Cache.Delete(id)
	}
	return collect, nil
}
