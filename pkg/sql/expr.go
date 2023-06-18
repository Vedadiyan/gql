package sql

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/lookup"
	"github.com/vedadiyan/gql/pkg/sentinel"
	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

func tableExpr(doc cmn.Document, expr sqlparser.TableExpr) ([]any, error) {
	switch t := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		{
			result, err := aliasedTableExpr(doc, t)
			if err != nil {
				return nil, err
			}
			return result, nil
		}
	case *sqlparser.JoinTableExpr:
		{
			return joinExpr(doc, t)
		}
	default:
		{
			return nil, sentinel.UNSUPPORTED_CASE.Extend("invalid from")
		}
	}
}
func aliasedTableExpr(doc cmn.Document, expr *sqlparser.AliasedTableExpr) ([]any, error) {
	switch t := expr.Expr.(type) {
	case sqlparser.TableName:
		{
			objName := t.Name
			if fn, ok := doc[objName.String()].(func() (any, error)); ok {
				res, err := fn()
				if err != nil {
					return nil, err
				}
				doc[objName.String()] = res
			}
			from, err := cmn.From(doc, objName.String())
			if err != nil {
				return nil, err
			}

			return cmn.ReadFrom(expr, from)
		}
	case *sqlparser.DerivedTable:
		{
			ctx := new(doc, false)
			err := ctx.prepare(t.Select)
			if err != nil {
				return nil, err
			}
			from, err := ctx.Exec()
			if err != nil {
				return nil, err
			}
			return cmn.ReadFrom(expr, from)
		}
	default:
		{
			return nil, sentinel.UNSUPPORTED_CASE.Extend("invalid from")
		}
	}
}

func aggregatedFuncExpr(bucket cmn.Bucket, alias string, cache map[string]any, expr sqlparser.AggrFunc) (any, error) {
	result, ok := cache[alias]
	if ok {
		return result, nil
	}
	switch t := expr.(type) {
	case *sqlparser.CountStar:
		{
			result, err := funcExprByName(bucket, nil, "count")
			if err != nil {
				return nil, err
			}
			cache[alias] = result
			return result, nil
		}
	case *sqlparser.Avg:
		{
			readArg, err := cmn.UnWrap[any](ExprReader(nil, nil, t.Arg, true))
			if err != nil {
				return nil, err
			}
			result, err := funcExprByName(bucket, bucket, "avg", readArg)
			if err != nil {
				return nil, err
			}
			cache[alias] = result
			return result, nil
		}
	case *sqlparser.Min:
		{
			readArg, err := cmn.UnWrap[any](ExprReader(nil, nil, t.Arg, true))
			if err != nil {
				return nil, err
			}
			result, err := funcExprByName(bucket, bucket, "min", readArg)
			if err != nil {
				return nil, err
			}
			cache[alias] = result
			return result, nil
		}
	case *sqlparser.Max:
		{
			readArg, err := cmn.UnWrap[any](ExprReader(nil, nil, t.Arg, true))
			if err != nil {
				return nil, err
			}
			result, err := funcExprByName(bucket, bucket, "max", readArg)
			if err != nil {
				return nil, err
			}
			cache[alias] = result
			return result, nil
		}
	case *sqlparser.Sum:
		{
			readArg, err := cmn.UnWrap[any](ExprReader(nil, nil, t.Arg, true))
			if err != nil {
				return nil, err
			}
			result, err := funcExprByName(bucket, bucket, "sum", readArg)
			if err != nil {
				return nil, err
			}
			cache[alias] = result
			return result, nil
		}
	}
	return nil, sentinel.UNSUPPORTED_CASE
}

type Aliased interface {
	Name() string
	Result() any
}

type AliasedBase struct {
	name   string
	result any
}

type NormalAliased struct {
	AliasedBase
}

func (normalAliased NormalAliased) Name() string {
	return normalAliased.name
}

func (normalAliased NormalAliased) Result() any {
	return normalAliased.result
}

type FunctionAliased struct {
	AliasedBase
}

func (functionAliased FunctionAliased) Name() string {
	return functionAliased.name
}

func (functionAliased FunctionAliased) Result() any {
	return functionAliased.result
}

func aliasedExpr(bucket cmn.Bucket, row any, colIndex int, cache map[string]any, expr *sqlparser.AliasedExpr) (Aliased, error) {
	alias := expr.As.String()
	if len(alias) == 0 {
		alias = fmt.Sprintf("col_%d", colIndex)
	}
	switch t := expr.Expr.(type) {
	case sqlparser.AggrFunc:
		{
			result, err := aggregatedFuncExpr(bucket, alias, cache, t)
			if err != nil {
				return nil, err
			}
			return &NormalAliased{AliasedBase{name: alias, result: result}}, nil
		}
	case *sqlparser.FuncExpr:
		{
			if cmn.IsSpecialFunction(t) {
				result, ok := cache[alias]
				if !ok {
					r, err := funcExpr(bucket, row, t)
					if err != nil {
						return nil, err
					}
					result = r
					cache[alias] = result
				}
				if len(expr.As.String()) == 0 {
					return &FunctionAliased{AliasedBase{name: "", result: result}}, nil
				}
				return &FunctionAliased{AliasedBase{name: alias, result: result}}, nil
			}
			result, err := funcExpr(bucket, row, t)
			if err != nil {
				return nil, err
			}
			if len(expr.As.String()) == 0 {
				return &FunctionAliased{AliasedBase{name: "", result: result}}, nil
			}
			return &FunctionAliased{AliasedBase{name: alias, result: result}}, nil
		}
	default:
		{
			if expr.As.String() == "" {
				_alias, err := cmn.UnWrap[any](ExprReader(nil, nil, expr.Expr, true))
				if err != nil {
					return nil, err
				}
				alias = _alias.(string)
			}
			result, err := cmn.UnWrap[any](ExprReader(bucket, row, expr.Expr))
			if err != nil {
				return nil, err
			}
			return &NormalAliased{AliasedBase{name: alias, result: result}}, nil
		}
	}
}
func starExpr(row any, index int) map[string]any {
	switch t := row.(type) {
	case map[string]any:
		{
			return t
		}
	default:
		{
			return map[string]any{
				fmt.Sprintf("col_%d", index): row,
			}
		}
	}
}
func joinExpr(doc cmn.Document, expr *sqlparser.JoinTableExpr) ([]any, error) {
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
	ascns, err := join.ReadCondition(doc, expr.Condition.On)
	if err != nil {
		return nil, err
	}
	switch expr.Join {
	case sqlparser.NormalJoinType:
		{
			return joinNormalExpr(ascns, left, right)
		}
	case sqlparser.LeftJoinType:
		{
			return joinLeftExpr(ascns, left, right)
		}
	case sqlparser.RightJoinType:
		{
			return joinRightExpr(ascns, left, right)
		}
	}
	return nil, sentinel.UNSUPPORTED_CASE
}
func joinNormalExpr(jrr JoinRawResult, l Left, r Right) ([]any, error) {
	output := make([]any, 0)
	for _, value := range jrr {
		out := make(map[string]any)
		for key, value := range l[value.left].(map[string]any) {
			out[key] = value
		}
		for key, value := range r[value.right].(map[string]any) {
			out[key] = value
		}
		output = append(output, out)
	}
	return output, nil
}
func joinLeftExpr(jrr JoinRawResult, l Left, r Right) ([]any, error) {
	lookup := CreateIndexedLookup(jrr, l, true)
	return joinExec(lookup, l, r), nil
}
func joinRightExpr(jrr JoinRawResult, l Left, r Right) ([]any, error) {
	lookup := CreateIndexedLookup(jrr, r, false)
	return joinExec(lookup, r, l), nil
}
func cteExpr(doc cmn.Document, expr *sqlparser.With) (cmn.Document, error) {
	output := make(map[string]any)
	for _, cte := range expr.Ctes {
		local := *cte
		output[local.ID.String()] = func() (any, error) {
			sql := new(doc, false)
			err := sql.prepare(local.Subquery.Select)
			if err != nil {
				return nil, err
			}
			return sql.Exec()
		}
	}
	return output, nil
}
func andExpr(b cmn.Bucket, row any, expr *sqlparser.AndExpr) (any, error) {
	l, err := cmn.UnWrap[bool](ExprReader(b, row, expr.Left))
	if err != nil {
		return false, err
	}
	r, err := cmn.UnWrap[bool](ExprReader(b, row, expr.Right))
	if err != nil {
		return false, err
	}
	return l && r, nil
}
func orExpr(b cmn.Bucket, row any, expr *sqlparser.OrExpr) (any, error) {
	l, err := cmn.UnWrap[bool](ExprReader(b, row, expr.Left))
	if err != nil {
		return false, err
	}
	r, err := cmn.UnWrap[bool](ExprReader(b, row, expr.Right))
	if err != nil {
		return false, err
	}
	return l || r, nil
}
func comparisonExpr(b cmn.Bucket, row any, expr *sqlparser.ComparisonExpr) (bool, error) {
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
	return false, sentinel.UNDEFINED_OPERATOR.Extend(expr.Operator.ToString())
}

func rangeExpr(b cmn.Bucket, row any, expr *sqlparser.BetweenExpr) (bool, error) {
	value, err := cmn.UnWrap[float64](ExprReader(b, row, expr.Left))
	if err != nil {
		return false, err
	}
	from, err := cmn.UnWrap[float64](ExprReader(b, row, expr.From))
	if err != nil {
		return false, err
	}
	to, err := cmn.UnWrap[float64](ExprReader(b, row, expr.To))
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
	return false, sentinel.UNDEFINED_OPERATOR
}

func binaryExpr(b cmn.Bucket, row any, expr *sqlparser.BinaryExpr) (float64, error) {
	left, err := cmn.UnWrap[float64](ExprReader(b, row, expr.Left))
	if err != nil {
		return 0, err
	}
	right, err := cmn.UnWrap[float64](ExprReader(b, row, expr.Right))
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
	return 0, sentinel.UNDEFINED_OPERATOR.Extend(expr.Operator.ToString())
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

func isExpr(b cmn.Bucket, row any, expr *sqlparser.IsExpr) (any, error) {
	left, err := cmn.UnWrapAny(ExprReader(b, row, expr.Left))
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
	return false, sentinel.UNDEFINED_OPERATOR.Extend(expr.Right.ToString())
}

func notExpr(b cmn.Bucket, row any, expr *sqlparser.NotExpr) (bool, error) {
	value, err := cmn.UnWrap[bool](ExprReader(b, row, expr))
	if err != nil {
		return false, err
	}
	return !value, nil
}

func subStrExpr(b cmn.Bucket, row any, expr *sqlparser.SubstrExpr) (string, error) {
	val, err := cmn.UnWrap[string](ExprReader(b, row, expr.Name))
	if err != nil {
		return "", err
	}
	from, err := cmn.UnWrap[float64](ExprReader(b, row, expr.From))
	if err != nil {
		return "", err
	}
	to, err := cmn.UnWrap[float64](ExprReader(b, row, expr.To))
	if err != nil {
		return "", err
	}
	return string(val[int(from):int(to)]), nil
}

func unaryExpr(expr *sqlparser.UnaryExpr) (float64, error) {
	val, err := cmn.UnWrap[float64](expr.Expr)
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
	return 0, sentinel.UNDEFINED_OPERATOR.Extend(expr.Operator.ToString())
}

func valueTupleExpr(b cmn.Bucket, row any, expr sqlparser.ValTuple) ([]any, error) {
	values := make([]any, 0)
	for _, value := range expr {
		val, err := cmn.UnWrapAny(ExprReader(b, row, value))
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func funcExpr(b cmn.Bucket, row any, expr *sqlparser.FuncExpr) (any, error) {
	fn := expr.Name.String()
	args := make([]any, 0)
	for _, expr := range expr.Exprs {
		aliasExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, sentinel.INVALID_TYPE
		}
		boolVal, ok := aliasExpr.Expr.(sqlparser.BoolVal)
		if !ok {
			val := ExprReader(b, row, aliasExpr.Expr, true)
			args = append(args, val)
			continue
		}
		args = append(args, bool(boolVal))
	}
	function, ok := cmn.Functions[strings.ToLower(fn)]
	if !ok {
		return nil, sentinel.INVALID_FUNCTION.Extend(fn)
	}
	return function(b, row, args)
}

func funcExprByName(b cmn.Bucket, row any, fn string, args ...any) (any, error) {
	function, ok := cmn.Functions[strings.ToLower(fn)]
	if !ok {
		return nil, sentinel.INVALID_FUNCTION.Extend(fn)
	}
	return function(b, row, args)
}

func colExpr(row any, expr *sqlparser.ColName, opt ...any) (any, error) {
	if cmn.IsColumnName(opt...) {
		return expr.Name.String(), nil
	}
	switch r := row.(type) {
	case map[string]any:
		{
			out, err := lookup.ReadObject(r, expr.Name.String())
			if err != nil {
				if errors.Is(err, sentinel.KEY_NOT_FOUND) {
					return nil, nil
				}
				return nil, err
			}
			return lookup.ToResult(out, false), nil
		}
	default:
		{
			out, err := lookup.ReadObject(map[string]any{"$": r}, "$."+expr.Name.String())
			if err != nil {
				if errors.Is(err, sentinel.KEY_NOT_FOUND) {
					return nil, nil
				}
				return nil, err
			}
			return lookup.ToResult(out, false), nil
		}
	}
}

func ExprReader(b cmn.Bucket, row any, expr sqlparser.Expr, opt ...any) any {
	switch t := expr.(type) {
	case *sqlparser.AndExpr:
		{
			return cmn.Wrap(andExpr(b, row, t))
		}
	case *sqlparser.OrExpr:
		{
			return cmn.Wrap(orExpr(b, row, t))
		}
	case *sqlparser.ComparisonExpr:
		{
			return cmn.Wrap(comparisonExpr(b, row, t))
		}
	case *sqlparser.BetweenExpr:
		{
			return cmn.Wrap(rangeExpr(b, row, t))
		}
	case *sqlparser.BinaryExpr:
		{
			return cmn.Wrap(binaryExpr(b, row, t))
		}
	case *sqlparser.Literal:
		{
			return cmn.Wrap(sqlValExpr(t))
		}
	case *sqlparser.NullVal:
		{
			return nil
		}
	case *sqlparser.IsExpr:
		{
			return cmn.Wrap(isExpr(b, row, t))
		}
	case *sqlparser.NotExpr:
		{
			return cmn.Wrap(notExpr(b, row, t))
		}
	case *sqlparser.SubstrExpr:
		{
			return cmn.Wrap(subStrExpr(b, row, t))
		}
	case *sqlparser.UnaryExpr:
		{
			return cmn.Wrap(unaryExpr(t))
		}
	case sqlparser.ValTuple:
		{
			return cmn.Wrap(valueTupleExpr(b, row, t))
		}
	case *sqlparser.FuncExpr:
		{
			return cmn.Wrap(funcExpr(b, row, t))
		}
	case *sqlparser.ColName:
		{
			if len(opt) > 0 && opt[0] == 1 {
				switch rowType := row.(type) {
				case map[string]any:
					{
						return cmn.Wrap(cmn.Select(rowType, t.Name.String()))
					}
				default:
					{
						return cmn.Wrap(row, nil)
					}
				}

			}
			return cmn.Wrap(colExpr(row, t, opt...))
		}
	case sqlparser.BoolVal:
		{
			return cmn.Wrap(bool(t), nil)
		}
	case *sqlparser.CaseExpr:
		{
			for _, i := range t.Whens {
				cond, err := cmn.UnWrap[bool](ExprReader(b, row, i.Cond, opt...))
				if err != nil {
					return cmn.Wrap(nil, err)
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
					context := new(rowType, false)
					err := context.prepare(t.Select)
					if err != nil {
						return cmn.Wrap(nil, err)
					}
					return cmn.Wrap(context.Exec())
				}
			case []any:
				{
					output := make([]any, 0)
					for _, item := range rowType {
						value := ExprReader(b, item, expr, opt...)
						if err, ok := value.(error); ok {
							return cmn.Wrap(nil, err)
						}
						output = append(output, value)
					}
					return cmn.Wrap(output, nil)
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
