package sql

import (
	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

const (
	_ONLY_VALUE_TYPES_ALLOWED = "only value types are valid on join conditions"
)

type JoinFunc func(objType any, rightIdx int, list *JoinRawResult) error
type LookupTable map[any][]int
type IndexedLookup map[int][]int
type Right = []any
type Left = []any
type JoinRawResult = []Association
type Operator = sqlparser.ComparisonExprOperator

type Association struct {
	left  int
	right int
}
type Join struct {
	lookup    LookupTable
	left      Left
	right     Right
	leftExpr  sqlparser.Expr
	rightExpr sqlparser.Expr
}

func (a Association) Get(left bool) int {
	if left {
		return a.left
	}
	return a.right
}
func (j *Join) ReadCondition(document map[string]any, expr sqlparser.Expr) (JoinRawResult, error) {
	switch joinCondition := expr.(type) {
	case *sqlparser.ComparisonExpr:
		{
			return j.Compare(joinCondition)
		}
	case *sqlparser.AndExpr:
		{
			return j.And(document, joinCondition)
		}
	case *sqlparser.OrExpr:
		{
			return j.Or(document, joinCondition)
		}
	}
	return nil, nil
}
func (j *Join) Compare(expr *sqlparser.ComparisonExpr) (JoinRawResult, error) {
	j.leftExpr = expr.Left
	j.rightExpr = expr.Right
	lookup, err := CreateLookupTable(j.left, j.leftExpr)
	if err != nil {
		return nil, err
	}
	j.lookup = lookup
	comparer := func(obj any, rightIdx int, list *JoinRawResult) error {
		switch t := obj.(type) {
		case float64:
			{
				result := JoinComparer(j.lookup, t, rightIdx, expr.Operator)
				*list = append(*list, result...)
				return nil
			}
		case string:
			{
				result := JoinComparer(j.lookup, t, rightIdx, expr.Operator)
				*list = append(*list, result...)
				return nil
			}
		case bool:
			{
				b := BoolToFloat64(t)
				result := JoinComparer(j.lookup, b, rightIdx, expr.Operator)
				*list = append(*list, result...)
				return nil
			}
		default:
			{
				return UNSUPPORTED_CASE
			}
		}
	}
	return JoinComparerFunc(j.right, j.rightExpr, comparer)
}
func (j *Join) And(document map[string]any, expr *sqlparser.AndExpr) (JoinRawResult, error) {
	leftAscns, err := j.ReadCondition(document, expr.Left)
	if err != nil {
		return nil, err
	}
	rightAscns, err := j.ReadCondition(document, expr.Right)
	if err != nil {
		return nil, err
	}
	lookup := make(map[Association]bool)
	for _, ascn := range leftAscns {
		lookup[ascn] = true
	}
	jrr := make(JoinRawResult, 0)
	for _, ascn := range rightAscns {
		_, ok := lookup[ascn]
		if ok {
			jrr = append(jrr, ascn)
		}
	}
	return jrr, nil
}
func (j *Join) Or(document map[string]any, expr *sqlparser.OrExpr) (JoinRawResult, error) {
	leftAscns, err := j.ReadCondition(document, expr.Left)
	if err != nil {
		return nil, err
	}
	rightAscns, err := j.ReadCondition(document, expr.Right)
	if err != nil {
		return nil, err
	}
	lookup := make(map[Association]bool)
	for _, ascn := range leftAscns {
		lookup[ascn] = true
	}
	for _, ascn := range rightAscns {
		lookup[ascn] = true
	}
	jrr := make(JoinRawResult, 0)
	for key := range lookup {
		jrr = append(jrr, key)
	}
	return jrr, nil
}
func JoinComparerFunc(right Right, rExpr sqlparser.Expr, jFn JoinFunc) (JoinRawResult, error) {
	jrr := make(JoinRawResult, 0)
	for rid, row := range right {
		obj, err := unwrap[any](ExprReader(nil, row, rExpr))
		if err != nil {
			return nil, err
		}
		switch t := obj.(type) {
		case string, float64, bool:
			{
				err := jFn(t, rid, &jrr)
				if err != nil {
					return nil, err
				}
			}
		default:
			{
				return nil, UNSUPPORTED_CASE.Extend(_ONLY_VALUE_TYPES_ALLOWED)
			}
		}
	}
	return jrr, nil
}
func JoinComparer[T float64 | string](lt LookupTable, obj T, rightIdx int, op Operator) JoinRawResult {
	jrr := make(JoinRawResult, 0)
	for key, value := range lt {
		ok, err := SimpleGenericComparison(obj, key, op)
		if err != nil {
			continue
		}
		if ok {
			for _, index := range value {
				jrr = append(jrr, Association{
					left:  index,
					right: rightIdx,
				})
			}
		}
	}
	return jrr
}
func CreateLookupTable(left Left, leftExpr sqlparser.Expr) (LookupTable, error) {
	lt := make(LookupTable)
	for index, row := range left {
		obj, err := unwrap[any](ExprReader(nil, row, leftExpr))
		if err != nil {
			return nil, err
		}
		switch objType := obj.(type) {
		case string, float64:
			{
				_, ok := lt[objType]
				if !ok {
					lt[objType] = make([]int, 0)
				}
				lt[objType] = append(lt[objType], index)
			}
		case bool:
			{
				objType := BoolToFloat64(objType)
				_, ok := lt[objType]
				if !ok {
					lt[objType] = make([]int, 0)
				}
				lt[objType] = append(lt[objType], index)
			}
		default:
			{
				return nil, UNSUPPORTED_CASE.Extend(_ONLY_VALUE_TYPES_ALLOWED)
			}
		}
	}
	return lt, nil
}
func CreateIndexedLookup(jrr JoinRawResult, list []any, left bool) IndexedLookup {
	il := make(IndexedLookup)
	for _, value := range jrr {
		a := value.Get(left)
		b := value.Get(!left)
		_, ok := il[a]
		if !ok {
			il[a] = make([]int, 0)
		}
		il[a] = append(il[a], b)
	}
	for index := range list {
		_, ok := il[index]
		if !ok {
			il[index] = make([]int, 0)
		}
	}
	return il
}
func JoinNormalExpr(jrr JoinRawResult, l Left, r Right) ([]any, error) {
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
func JoinLeftExpr(jrr JoinRawResult, l Left, r Right) ([]any, error) {
	lookup := CreateIndexedLookup(jrr, l, true)
	return JoinExec(lookup, l, r), nil
}
func JoinRightExpr(jrr JoinRawResult, l Left, r Right) ([]any, error) {
	lookup := CreateIndexedLookup(jrr, r, false)
	return JoinExec(lookup, r, l), nil
}
func JoinExec(fl IndexedLookup, l Left, r Right) []any {
	result := make([]any, 0)
	for index, value := range fl {
		if len(value) > 0 {
			for _, v := range value {
				out := make(map[string]any)
				for key, value := range r[index].(map[string]any) {
					out[key] = value
				}
				for key, value := range l[v].(map[string]any) {
					out[key] = value
				}
				result = append(result, out)
			}
			continue
		}
		out := make(map[string]any)
		for key, value := range r[index].(map[string]any) {
			out[key] = value
		}
		for key := range l[0].(map[string]any) {
			out[key] = nil
		}
		result = append(result, out)
	}
	return result
}
func ReadJoinExpr(document map[string]any, expr *sqlparser.JoinTableExpr) ([]any, error) {
	left, err := readTableExpr(document, expr.LeftExpr)
	if err != nil {
		return nil, err
	}
	right, err := readTableExpr(document, expr.RightExpr)
	if err != nil {
		return nil, err
	}
	join := Join{
		left:  left,
		right: right,
	}
	rs, err := join.ReadCondition(document, expr.Condition.On)
	if err != nil {
		return nil, err
	}
	switch expr.Join {
	case sqlparser.NormalJoinType:
		{
			return JoinNormalExpr(rs, left, right)
		}
	case sqlparser.LeftJoinType:
		{
			return JoinLeftExpr(rs, left, right)
		}
	case sqlparser.RightJoinType:
		{
			return JoinRightExpr(rs, left, right)
		}
	}
	return nil, UNSUPPORTED_CASE
}
