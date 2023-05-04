package sql

import (
	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/sentinel"
	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

const (
	_ONLY_VALUE_TYPES_ALLOWED = "only value types are valid on join conditions"
)

type JoinFunc func(v any, idx int, list *JoinRawResult) error
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
func (j *Join) ReadCondition(doc map[string]any, expr sqlparser.Expr) (JoinRawResult, error) {
	switch joinCondition := expr.(type) {
	case *sqlparser.ComparisonExpr:
		{
			return j.Compare(joinCondition)
		}
	case *sqlparser.AndExpr:
		{
			return j.And(doc, joinCondition)
		}
	case *sqlparser.OrExpr:
		{
			return j.Or(doc, joinCondition)
		}
	}
	return nil, nil
}
func (j *Join) Compare(expr *sqlparser.ComparisonExpr) (JoinRawResult, error) {
	j.leftExpr = expr.Left
	j.rightExpr = expr.Right
	// BUGGY CODE: CANNOT DETERMINE LEFT OR RIGHT
	lookup, err := CreateLookupTable(j.left, j.leftExpr)
	if err != nil {
		return nil, err
	}
	j.lookup = lookup
	comparer := func(v any, idx int, list *JoinRawResult) error {
		switch t := v.(type) {
		case float64:
			{
				result := JoinComparer(j.lookup, t, idx, expr.Operator)
				*list = append(*list, result...)
				return nil
			}
		case string:
			{
				result := JoinComparer(j.lookup, t, idx, expr.Operator)
				*list = append(*list, result...)
				return nil
			}
		case bool:
			{
				b := cmn.BoolToFloat64(t)
				result := JoinComparer(j.lookup, b, idx, expr.Operator)
				*list = append(*list, result...)
				return nil
			}
		default:
			{
				return sentinel.UNSUPPORTED_CASE
			}
		}
	}
	return JoinComparerFunc(j.right, j.rightExpr, comparer)
}
func (j *Join) And(doc map[string]any, expr *sqlparser.AndExpr) (JoinRawResult, error) {
	leftAscns, err := j.ReadCondition(doc, expr.Left)
	if err != nil {
		return nil, err
	}
	rightAscns, err := j.ReadCondition(doc, expr.Right)
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
func (j *Join) Or(doc map[string]any, expr *sqlparser.OrExpr) (JoinRawResult, error) {
	leftAscns, err := j.ReadCondition(doc, expr.Left)
	if err != nil {
		return nil, err
	}
	rightAscns, err := j.ReadCondition(doc, expr.Right)
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
		obj, err := cmn.UnWrap[any](ExprReader(nil, row, rExpr))
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
				return nil, sentinel.UNSUPPORTED_CASE.Extend(_ONLY_VALUE_TYPES_ALLOWED)
			}
		}
	}
	return jrr, nil
}
func JoinComparer[T float64 | string](lt LookupTable, v T, idx int, op Operator) JoinRawResult {
	jrr := make(JoinRawResult, 0)
	for key, value := range lt {
		ok, err := SimpleGenericComparison(v, key, op)
		if err != nil {
			continue
		}
		if ok {
			for _, index := range value {
				jrr = append(jrr, Association{
					left:  index,
					right: idx,
				})
			}
		}
	}
	return jrr
}
func CreateLookupTable(left Left, leftExpr sqlparser.Expr) (LookupTable, error) {
	lt := make(LookupTable)
	for index, row := range left {
		obj, err := cmn.UnWrap[any](ExprReader(nil, row, leftExpr))
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
				objType := cmn.BoolToFloat64(objType)
				_, ok := lt[objType]
				if !ok {
					lt[objType] = make([]int, 0)
				}
				lt[objType] = append(lt[objType], index)
			}
		default:
			{
				return nil, sentinel.UNSUPPORTED_CASE.Extend(_ONLY_VALUE_TYPES_ALLOWED)
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
