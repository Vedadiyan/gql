package sql

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

func andExpr(jo *[]any, row any, expr *sqlparser.AndExpr) (any, error) {
	l, err := unwrap[bool](ExprReader(jo, row, expr.Left))
	if err != nil {
		return false, err
	}
	r, err := unwrap[bool](ExprReader(jo, row, expr.Right))
	if err != nil {
		return false, err
	}
	return l && r, nil
}

func orExpr(jo *[]any, row any, expr *sqlparser.OrExpr) (any, error) {
	l, err := unwrap[bool](ExprReader(jo, row, expr.Left))
	if err != nil {
		return false, err
	}
	r, err := unwrap[bool](ExprReader(jo, row, expr.Right))
	if err != nil {
		return false, err
	}
	return l || r, nil
}

func comparisonExpr(jo *[]any, row any, expr *sqlparser.ComparisonExpr) (bool, error) {
	l := ExprReader(jo, row, expr.Left)
	r := ExprReader(jo, row, expr.Right)
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

func rangeExpr(jo *[]any, row any, expr *sqlparser.BetweenExpr) (bool, error) {
	value, err := unwrap[float64](ExprReader(jo, row, expr.Left))
	if err != nil {
		return false, err
	}
	from, err := unwrap[float64](ExprReader(jo, row, expr.From))
	if err != nil {
		return false, err
	}
	to, err := unwrap[float64](ExprReader(jo, row, expr.To))
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

func binaryExpr(jo *[]any, row any, expr *sqlparser.BinaryExpr) (float64, error) {
	left, err := unwrap[float64](ExprReader(jo, row, expr.Left))
	if err != nil {
		return 0, err
	}
	right, err := unwrap[float64](ExprReader(jo, row, expr.Right))
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

func isExpr(jo *[]any, row any, expr *sqlparser.IsExpr) (any, error) {
	left, err := unwrapAny(ExprReader(jo, row, expr.Left))
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

func notExpr(jo *[]any, row any, expr *sqlparser.NotExpr) (bool, error) {
	value, err := unwrap[bool](ExprReader(jo, row, expr))
	if err != nil {
		return false, err
	}
	return !value, nil
}

func subStrExpr(jo *[]any, row any, expr *sqlparser.SubstrExpr) (string, error) {
	val, err := unwrap[string](ExprReader(jo, row, expr.Name))
	if err != nil {
		return "", err
	}
	from, err := unwrap[float64](ExprReader(jo, row, expr.From))
	if err != nil {
		return "", err
	}
	to, err := unwrap[float64](ExprReader(jo, row, expr.To))
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

func valueTupleExpr(jo *[]any, row any, expr sqlparser.ValTuple) ([]any, error) {
	values := make([]any, 0)
	for _, value := range expr {
		val, err := unwrapAny(ExprReader(jo, row, value))
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func funcExpr(jo *[]any, row any, expr *sqlparser.FuncExpr) (any, error) {
	fn := expr.Name.String()
	args := make([]any, 0)
	for _, expr := range expr.Exprs {
		aliasExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			panic("type mismatch")
		}
		boolVal, ok := aliasExpr.Expr.(sqlparser.BoolVal)
		if !ok {
			val := ExprReader(jo, row, aliasExpr.Expr, true)
			args = append(args, val)
			continue
		}
		args = append(args, bool(boolVal))
	}
	function, ok := _functions[strings.ToLower(fn)]
	if !ok {
		return nil, INVALID_FUNCTION.Extend(fn)
	}
	return function(jo, row, args), nil
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

func ExprReader(jo *[]any, row any, expr sqlparser.Expr, opt ...any) any {
	switch t := expr.(type) {
	case *sqlparser.AndExpr:
		{
			return wrap(andExpr(jo, row, t))
		}
	case *sqlparser.OrExpr:
		{
			return wrap(orExpr(jo, row, t))
		}
	case *sqlparser.ComparisonExpr:
		{
			return wrap(comparisonExpr(jo, row, t))
		}
	case *sqlparser.BetweenExpr:
		{
			return wrap(rangeExpr(jo, row, t))
		}
	case *sqlparser.BinaryExpr:
		{
			return wrap(binaryExpr(jo, row, t))
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
			return wrap(isExpr(jo, row, t))
		}
	case *sqlparser.NotExpr:
		{
			return wrap(notExpr(jo, row, t))
		}
	// case *sqlparser.ParenExpr:
	// 	{
	// 		return ExprReader(jo, row, t.Expr)
	// 	}
	case *sqlparser.SubstrExpr:
		{
			return wrap(subStrExpr(jo, row, t))
		}
	case *sqlparser.UnaryExpr:
		{
			return wrap(unaryExpr(t))
		}
	case sqlparser.ValTuple:
		{
			return wrap(valueTupleExpr(jo, row, t))
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
				value, err := funcExpr(jo, row, t)
				if err != nil {
					_cache.Store(id, err)
					return wrap(nil, err)
				}
				_cache.Store(id, value)
				return wrap(value, nil)
			}
			return wrap(funcExpr(jo, row, t))
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
				cond, err := unwrap[bool](ExprReader(jo, row, i.Cond, opt...))
				if err != nil {
					return wrap(nil, err)
				}
				if cond {
					return ExprReader(jo, row, i.Val, opt...)
				}
			}
			return ExprReader(jo, row, t.Else, opt...)
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
						value := ExprReader(jo, item, expr, opt...)
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
