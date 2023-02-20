package sql

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

type Bucket *[]any

func tableExpr(doc Document, expr sqlparser.TableExpr) ([]any, error) {
	switch fromExprType := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		{
			result, err := aliasedTableExpr(doc, fromExprType)
			if err != nil {
				return nil, err
			}
			return result, nil
		}
	case *sqlparser.JoinTableExpr:
		{
			return joinExpr(doc, fromExprType)
		}
	default:
		{
			return nil, UNSUPPORTED_CASE.Extend("invalid from")
		}
	}
}
func aliasedTableExpr(doc map[string]any, expr *sqlparser.AliasedTableExpr) ([]any, error) {
	switch exprType := expr.Expr.(type) {
	case sqlparser.TableName:
		{
			objName := exprType.Name
			from, err := From(doc, objName.String())
			if err != nil {
				return nil, err
			}

			return readFrom(expr, from)
		}
	case *sqlparser.DerivedTable:
		{
			innerCtx := New(doc)
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
func aliasedExpr(expr *sqlparser.AliasedExpr) (string, error) {
	if expr.As.String() != "" {
		return expr.As.String(), nil
	}
	return unwrap[string](ExprReader(nil, nil, expr.Expr, true))
}
func starExpr(row any, key *string, index int) map[string]any {
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
func joinExpr(doc map[string]any, expr *sqlparser.JoinTableExpr) ([]any, error) {
	left, err := tableExpr(doc, expr.LeftExpr)
	if err != nil {
		return nil, err
	}
	right, err := tableExpr(doc, expr.RightExpr)
	if err != nil {
		return nil, err
	}
	join := Join{
		left:  left,
		right: right,
	}
	rs, err := join.ReadCondition(doc, expr.Condition.On)
	if err != nil {
		return nil, err
	}
	switch expr.Join {
	case sqlparser.NormalJoinType:
		{
			return joinNormalExpr(rs, left, right)
		}
	case sqlparser.LeftJoinType:
		{
			return joinLeftExpr(rs, left, right)
		}
	case sqlparser.RightJoinType:
		{
			return joinRightExpr(rs, left, right)
		}
	}
	return nil, UNSUPPORTED_CASE
}
func joinNormalExpr(jrr JoinRawResult, l Left, r Right) ([]any, error) {
	collect := make([]any, 0)
	for _, value := range jrr {
		out := make(map[string]any)
		for key, value := range l[value.left].(map[string]any) {
			out[key] = value
		}
		for key, value := range r[value.right].(map[string]any) {
			out[key] = value
		}
		collect = append(collect, out)
	}
	return collect, nil
}
func joinLeftExpr(jrr JoinRawResult, l Left, r Right) ([]any, error) {
	lookup := CreateIndexedLookup(jrr, l, true)
	return joinExec(lookup, l, r), nil
}
func joinRightExpr(jrr JoinRawResult, l Left, r Right) ([]any, error) {
	lookup := CreateIndexedLookup(jrr, r, false)
	return joinExec(lookup, r, l), nil
}
func cteExpr(doc Document, expr *sqlparser.With) (Document, error) {
	data := make(map[string]any)
	for _, cte := range expr.Ctes {
		doc := make(Document)
		for key, value := range doc {
			doc[key] = value
		}
		for key, value := range data {
			doc[key] = value
		}
		sql := New(doc)
		err := sql.prepare(cte.Subquery.Select)
		if err != nil {
			return nil, err
		}
		rs, err := sql.Exec()
		if err != nil {
			return nil, err
		}
		data[cte.ID.String()] = rs
	}
	return data, nil
}
func andExpr(b Bucket, row any, expr *sqlparser.AndExpr) (any, error) {
	l, err := unwrap[bool](ExprReader(b, row, expr.Left))
	if err != nil {
		return false, err
	}
	r, err := unwrap[bool](ExprReader(b, row, expr.Right))
	if err != nil {
		return false, err
	}
	return l && r, nil
}
func orExpr(b Bucket, row any, expr *sqlparser.OrExpr) (any, error) {
	l, err := unwrap[bool](ExprReader(b, row, expr.Left))
	if err != nil {
		return false, err
	}
	r, err := unwrap[bool](ExprReader(b, row, expr.Right))
	if err != nil {
		return false, err
	}
	return l || r, nil
}
func comparisonExpr(b Bucket, row any, expr *sqlparser.ComparisonExpr) (bool, error) {
	l := ExprReader(b, row, expr.Left)
	r := ExprReader(b, row, expr.Right)
	switch expr.Operator {
	case sqlparser.EqualOp, sqlparser.NotEqualOp:
		{
			return equalityCompare(l, r, expr.Operator.ToString())
		}
	case sqlparser.GreaterThanOp, sqlparser.LessThanOp, sqlparser.GreaterEqualOp, sqlparser.LessEqualOp:
		{
			return numericCompare(l, r, expr.Operator.ToString())
		}
	case sqlparser.InOp, sqlparser.NotInOp:
		{
			return inComparison(l, r, expr.Operator.ToString())
		}
	case sqlparser.LikeOp:
		{
			return regexComparison(l, r.(string))
		}
	case sqlparser.NotLikeOp:
		{
			b, err := regexComparison(l, r.(string))
			if err != nil {
				return false, err
			}
			return !b, nil
		}
	}
	return false, UNDEFINED_OPERATOR.Extend(expr.Operator.ToString())
}

func rangeExpr(b Bucket, row any, expr *sqlparser.BetweenExpr) (bool, error) {
	value, err := unwrap[float64](ExprReader(b, row, expr.Left))
	if err != nil {
		return false, err
	}
	from, err := unwrap[float64](ExprReader(b, row, expr.From))
	if err != nil {
		return false, err
	}
	to, err := unwrap[float64](ExprReader(b, row, expr.To))
	if err != nil {
		return false, err
	}
	switch expr.IsBetween {
	case true:
		{
			return (value > from) && (value < to), nil
		}
	case false:
		{
			return (value < from) && (value > to), nil
		}
	}
	return false, UNDEFINED_OPERATOR
}

func binaryExpr(b Bucket, row any, expr *sqlparser.BinaryExpr) (float64, error) {
	left, err := unwrap[float64](ExprReader(b, row, expr.Left))
	if err != nil {
		return 0, err
	}
	right, err := unwrap[float64](ExprReader(b, row, expr.Right))
	if err != nil {
		return 0, err
	}
	switch expr.Operator {
	case sqlparser.PlusOp:
		{
			return left + right, nil
		}
	case sqlparser.MinusOp:
		{
			return left - right, nil
		}
	case sqlparser.MultOp:
		{
			return left * right, nil
		}
	case sqlparser.DivOp:
		{
			return left / right, nil
		}
	case sqlparser.IntDivOp:
		{
			return float64(int64(left) / int64(right)), nil
		}
	case sqlparser.ModOp:
		{
			return math.Mod(left, right), nil
		}
	case sqlparser.BitAndOp:
		{
			return float64(int64(left) & int64(right)), nil
		}
	case sqlparser.BitOrOp:
		{
			return float64(int64(left) | int64(right)), nil
		}
	case sqlparser.BitXorOp:
		{
			return float64(int64(left) ^ int64(right)), nil
		}
	case sqlparser.ShiftLeftOp:
		{
			return float64(int64(left) >> int64(right)), nil
		}
	case sqlparser.ShiftRightOp:
		{
			return float64(int64(left) << int64(right)), nil
		}
	}
	return 0, UNDEFINED_OPERATOR.Extend(expr.Operator.ToString())
}

func sqlValExpr(expr *sqlparser.Literal) (any, error) {
	switch expr.Type {
	case sqlparser.StrVal:
		{
			return string(expr.Val), nil
		}
	default:
		{
			return strconv.ParseFloat(string(expr.Val), 64)
		}
	}
}

func isExpr(b Bucket, row any, expr *sqlparser.IsExpr) (any, error) {
	left, err := unwrapAny(ExprReader(b, row, expr.Left))
	if err != nil {
		return nil, err
	}
	switch expr.Right {
	case sqlparser.IsNullOp, sqlparser.IsNotNullOp:
		{
			return isNull(left, expr.Right.ToString())
		}
	case sqlparser.IsTrueOp, sqlparser.IsNotTrueOp, sqlparser.IsFalseOp, sqlparser.IsNotFalseOp:
		{
			return boolComparison(left, expr.Right.ToString())
		}
	}
	return false, UNDEFINED_OPERATOR.Extend(expr.Right.ToString())
}

func notExpr(b Bucket, row any, expr *sqlparser.NotExpr) (bool, error) {
	value, err := unwrap[bool](ExprReader(b, row, expr))
	if err != nil {
		return false, err
	}
	return !value, nil
}

func subStrExpr(b Bucket, row any, expr *sqlparser.SubstrExpr) (string, error) {
	val, err := unwrap[string](ExprReader(b, row, expr.Name))
	if err != nil {
		return "", err
	}
	from, err := unwrap[float64](ExprReader(b, row, expr.From))
	if err != nil {
		return "", err
	}
	to, err := unwrap[float64](ExprReader(b, row, expr.To))
	if err != nil {
		return "", err
	}
	return string(val[int(from):int(to)]), nil
}

func unaryExpr(expr *sqlparser.UnaryExpr) (float64, error) {
	val, err := unwrap[float64](expr.Expr)
	if err != nil {
		return 0, err
	}
	switch expr.Operator.ToString() {
	case "-":
		{
			return -1 * val, nil
		}
	case "~":
		{
			return float64(^int64(val)), nil
		}
	}
	return 0, UNDEFINED_OPERATOR.Extend(expr.Operator.ToString())
}

func valueTupleExpr(b Bucket, row any, expr sqlparser.ValTuple) ([]any, error) {
	values := make([]any, 0)
	for _, value := range expr {
		val, err := unwrapAny(ExprReader(b, row, value))
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func funcExpr(b Bucket, row any, expr *sqlparser.FuncExpr) (any, error) {
	fn := expr.Name.String()
	args := make([]any, 0)
	for _, expr := range expr.Exprs {
		aliasExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			panic("type mismatch")
		}
		boolVal, ok := aliasExpr.Expr.(sqlparser.BoolVal)
		if !ok {
			val := ExprReader(b, row, aliasExpr.Expr, true)
			args = append(args, val)
			continue
		}
		args = append(args, bool(boolVal))
	}
	function, ok := _functions[strings.ToLower(fn)]
	if !ok {
		return nil, INVALID_FUNCTION.Extend(fn)
	}
	return function(b, row, args), nil
}

func colExpr(row any, expr *sqlparser.ColName, opt ...any) (any, error) {
	if isColumnName(opt...) {
		return expr.Name.String(), nil
	}
	switch r := row.(type) {
	case map[string]any:
		{
			out, err := readObject(r, expr.Name.String())
			if err != nil {
				if errors.Is(err, KEY_NOT_FOUND) {
					return nil, nil
				}
				return nil, err
			}
			return toResult(out), nil
		}
	case []any:
		{
			groupBy, _ := hasGroupBy(opt...)
			_ = groupBy
			output := make([]any, 0)
			for _, row := range r {
				result, err := colExpr(row, expr, opt...)
				if err != nil {
					return nil, err
				}
				switch result.(type) {
				case map[string]any, []any:
					{
						output = append(output, result)
					}
				default:
					{
						_, ok := groupBy[expr.Name.String()]
						if ok {
							return result, nil
						}
						output = append(output, result)

					}
				}

			}
			return toResult(output), nil
		}
	}
	return nil, nil
}

func ExprReader(b Bucket, row any, expr sqlparser.Expr, opt ...any) any {
	switch t := expr.(type) {
	case *sqlparser.AndExpr:
		{
			return wrap(andExpr(b, row, t))
		}
	case *sqlparser.OrExpr:
		{
			return wrap(orExpr(b, row, t))
		}
	case *sqlparser.ComparisonExpr:
		{
			return wrap(comparisonExpr(b, row, t))
		}
	case *sqlparser.BetweenExpr:
		{
			return wrap(rangeExpr(b, row, t))
		}
	case *sqlparser.BinaryExpr:
		{
			return wrap(binaryExpr(b, row, t))
		}
	case *sqlparser.Literal:
		{
			return wrap(sqlValExpr(t))
		}
	case *sqlparser.NullVal:
		{
			return nil
		}
	case *sqlparser.IsExpr:
		{
			return wrap(isExpr(b, row, t))
		}
	case *sqlparser.NotExpr:
		{
			return wrap(notExpr(b, row, t))
		}
	// case *sqlparser.ParenExpr:
	// 	{
	// 		return ExprReader(jo, row, t.Expr)
	// 	}
	case *sqlparser.SubstrExpr:
		{
			return wrap(subStrExpr(b, row, t))
		}
	case *sqlparser.UnaryExpr:
		{
			return wrap(unaryExpr(t))
		}
	case sqlparser.ValTuple:
		{
			return wrap(valueTupleExpr(b, row, t))
		}
	case *sqlparser.FuncExpr:
		{
			if isSpecialFunction(t) && len(opt) > 0 {
				id := opt[0].(string)
				value, ok := _cache.Load(id)
				if ok {
					if err, ok := value.(error); ok {
						return wrap(nil, err)
					}
					return wrap(value, nil)
				}
				value, err := funcExpr(b, row, t)
				if err != nil {
					_cache.Store(id, err)
					return wrap(nil, err)
				}
				_cache.Store(id, value)
				return wrap(value, nil)
			}
			return wrap(funcExpr(b, row, t))
		}
	case *sqlparser.ColName:
		{
			if len(opt) > 0 && opt[0] == 1 {
				switch rowType := row.(type) {
				case map[string]any:
					{
						return wrap(Select(rowType, t.Name.String()))
					}
				default:
					{
						return wrap(row, nil)
					}
				}

			}
			return wrap(colExpr(row, t, opt...))
		}
	case sqlparser.BoolVal:
		{
			return wrap(bool(t), nil)
		}
	case *sqlparser.CaseExpr:
		{
			for _, i := range t.Whens {
				cond, err := unwrap[bool](ExprReader(b, row, i.Cond, opt...))
				if err != nil {
					return wrap(nil, err)
				}
				if cond {
					return ExprReader(b, row, i.Val, opt...)
				}
			}
			return ExprReader(b, row, t.Else, opt...)
		}
	case *sqlparser.Subquery:
		{
			switch rowType := row.(type) {
			case map[string]any:
				{
					context := New(rowType)
					err := context.prepare(t.Select)
					if err != nil {
						return wrap(nil, err)
					}
					return wrap(context.Exec())
				}
			case []any:
				{
					output := make([]any, 0)
					for _, item := range rowType {
						value := ExprReader(b, item, expr, opt...)
						if err, ok := value.(error); ok {
							return wrap(nil, err)
						}
						output = append(output, value)
					}
					return wrap(output, nil)
				}
			}

		}
	case *sqlparser.ExistsExpr:
		{

		}
	case *sqlparser.MatchExpr:
		{

		}
	}
	return nil
}
