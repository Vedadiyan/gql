package sql

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
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

func removeComments(query string) string {
	buffer := bytes.NewBufferString("")
	hold := false
	jump := false
	count := 0
	data := strings.FieldsFunc(query, func(r rune) bool {
		return r == '\r' || r == '\n'
	})
	for _, line := range data {
		for _, c := range line {
			if jump {
				jump = !jump
			} else if hold {
				if c == '\\' {
					jump = true
				}
				if c == '\'' {
					hold = false
				}
			} else if c == '\'' {
				hold = true
			} else if c == '-' {
				count++
				if count == 2 {
					break
				}
				continue
			} else {
				count = 0
			}
			buffer.WriteRune(c)
		}
		buffer.WriteString("\r\n")
	}
	return buffer.String()
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
			result, err := execSelect(&c.from, group, id, &key, c.selectStmt, GroupBy(c.groupBy))
			if err != nil {
				return nil, err
			}
			collect = append(collect, result)
		}
	} else {
		if len(c.from) > 0 && c.from[0] == nil {
			result, err := execSelect(&c.from, c.document, id, nil, c.selectStmt, nil)
			if err != nil {
				return nil, err
			}
			collect[0] = result
		} else {
			for index, row := range collect {
				result, err := execSelect(&c.from, row, id, nil, c.selectStmt, nil)
				if err != nil {
					return nil, err
				}
				collect[index] = result
			}
		}
	}
	for index := range c.selectStmt {
		id := fmt.Sprintf("%d_%d", id, index)
		_cache.Delete(id)
	}
	return collect, nil
}

// func getTableName(expr *sqlparser.AliasedTableExpr) (string, string) {
// 	return expr.As.String(), expr.Expr.(sqlparser.TableName).Name.String()
// }

func runJoinComparison(expr *sqlparser.ComparisonExpr, left []any, right []any) ([]any, error) {
	lv, err := unwrap[string](ExprReader(nil, nil, expr.Left, true))
	if err != nil {
		return nil, err
	}
	rv, err := unwrap[string](ExprReader(nil, nil, expr.Right, true))
	if err != nil {
		return nil, err
	}
	lookup := make(map[any][]int)
	for index, row := range left {
		value, err := Select(row.(map[string]any), lv)
		if err != nil {
			return nil, err
		}
		switch valueType := value.(type) {
		case string, float64, bool:
			{
				_, ok := lookup[valueType]
				if !ok {
					lookup[valueType] = make([]int, 0)
				}
				lookup[valueType] = append(lookup[valueType], index)
			}
		default:
			{
				return nil, UNSUPPORTED_CASE.Extend("only value types are valid on join conditions")
			}
		}
	}
	switch expr.Operator {
	case sqlparser.EqualOp:
		{
			collect := make([]any, 0)
			for index, row := range right {
				value, err := Select(row.(map[string]any), rv)
				if err != nil {
					return nil, err
				}
				switch valueType := value.(type) {
				case string, float64, bool:
					{
						value, ok := lookup[valueType]
						if ok {
							for _, association := range value {
								collect = append(collect, []int{association, index})
							}
						}
					}
				default:
					{
						return nil, UNSUPPORTED_CASE.Extend("only value types are valid on join conditions")
					}
				}
			}
			return collect, nil
		}
	case sqlparser.NotEqualOp:
		{
			collect := make([]any, 0)
			for index, row := range right {
				value, err := Select(row.(map[string]any), rv)
				if err != nil {
					return nil, err
				}
				switch valueType := value.(type) {
				case string, float64, bool:
					{
						_, ok := lookup[valueType]
						if !ok {
							collect = append(collect, []int{-1, index})
						}
					}
				default:
					{
						return nil, UNSUPPORTED_CASE.Extend("only value types are valid on join conditions")
					}
				}
			}
			return collect, nil
		}
	}
	return nil, nil
}

func readJoinCond(document map[string]any, expr sqlparser.Expr, left []any, right []any) ([]any, error) {
	switch joinCondition := expr.(type) {
	case *sqlparser.ComparisonExpr:
		{
			return runJoinComparison(joinCondition, left, right)
		}
	case *sqlparser.AndExpr:
		{
			l, err := readJoinCond(document, joinCondition.Left, left, right)
			if err != nil {
				return nil, err
			}
			r, err := readJoinCond(document, joinCondition.Right, left, right)
			if err != nil {
				return nil, err
			}
			if len(r) > 0 && r[0].([]int)[0] == -1 {
				tmp := l
				l = r
				r = tmp
			}
			flag := false
			lookup := make(map[string]bool)
			for _, value := range l {
				val := value.([]int)
				if val[0] != -1 {
					lookup[fmt.Sprintf("%d-%d", val[0], val[1])] = true
					continue
				}
				flag = true
				lookup[fmt.Sprintf("%d", val[1])] = true
			}
			collect := make([]any, 0)
			for _, value := range r {
				val := value.([]int)
				if !flag {
					_, ok := lookup[fmt.Sprintf("%d-%d", val[0], val[1])]
					if ok {
						collect = append(collect, value)
					}
					continue
				}
				_, ok := lookup[fmt.Sprintf("%d", val[1])]
				if ok {
					collect = append(collect, value)
				}
			}
			return collect, nil
		}
	case *sqlparser.OrExpr:
		{
			l, err := readJoinCond(document, joinCondition.Left, left, right)
			if err != nil {
				return nil, err
			}
			r, err := readJoinCond(document, joinCondition.Right, left, right)
			if err != nil {
				return nil, err
			}
			if len(l) > len(r) {
				return l, nil
			}
			return r, nil
		}
	}
	return nil, nil
}

func readJoinExpr(document map[string]any, expr *sqlparser.JoinTableExpr) ([]any, error) {
	left, err := readTableExpr(document, expr.LeftExpr)
	if err != nil {
		return nil, err
	}
	right, err := readTableExpr(document, expr.RightExpr)
	if err != nil {
		return nil, err
	}
	rs, err := readJoinCond(document, expr.Condition.On, left, right)
	if err != nil {
		return nil, err
	}
	collect := make([]any, 0)
	for _, value := range rs {
		val := value.([]int)
		out := make(map[string]any)
		if val[0] != -1 {
			for key, value := range left[val[0]].(map[string]any) {
				out[key] = value
			}
		}
		for key, value := range right[val[1]].(map[string]any) {
			out[key] = value
		}
		collect = append(collect, out)
	}
	return collect, nil
}

func readTableExpr(document map[string]any, expr sqlparser.TableExpr) ([]any, error) {
	switch fromExprType := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		{
			result, err := readAliasedTableExpr(document, fromExprType)
			if err != nil {
				return nil, err
			}
			return result, nil
		}
	case *sqlparser.JoinTableExpr:
		{
			return readJoinExpr(document, fromExprType)
		}
	default:
		{
			return nil, UNSUPPORTED_CASE.Extend("invalid from")
		}
	}
}

func (c *Context) setFrom(expr sqlparser.TableExpr) error {
	result, err := readTableExpr(c.document, expr)
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

func Copy(array []any) any {
	cp := make([]any, len(array))
	for index, item := range array {
		switch itemType := item.(type) {
		case *[]any:
			{
				cp[index] = Copy(*itemType)
			}
		case map[string]any:
			{
				mapper := make(map[string]any, len(itemType))
				for key, value := range itemType {
					switch valueType := value.(type) {
					case []any:
						{
							mapper[key] = Copy(valueType)
						}
					case *[]any:
						{
							mapper[key] = Copy(*valueType)
						}
					default:
						{
							mapper[key] = valueType
						}
					}
				}
				cp[index] = mapper
			}
		default:
			{
				cp[index] = itemType
			}
		}
	}
	return cp
}

func (c *Context) prepare(statement sqlparser.Statement) error {
	slct, ok := statement.(*sqlparser.Select)
	if !ok {
		return fmt.Errorf("invalid statement")
	}
	if slct.With != nil {
		data := make(map[string]any)
		for _, cte := range slct.With.Ctes {
			document := make(map[string]any)
			for key, value := range c.document {
				document[key] = value
			}
			for key, value := range data {
				document[key] = value
			}
			sql := New(document)
			err := sql.prepare(cte.Subquery.Select)
			if err != nil {
				return err
			}
			rs, err := sql.Exec()
			if err != nil {
				return err
			}
			data[cte.ID.String()] = rs
		}
		c.document = data
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
	case *sqlparser.DerivedTable:
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
	for index, expr := range exprs {
		switch exprType := expr.(type) {
		case *sqlparser.StarExpr:
			{
				for key, value := range readStarExpr(row, key, index) {
					output[key] = value
				}
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
