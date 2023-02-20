package sql

import (
	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

type joinFn func(objType any, rightIdx int, list *[]association)

type association struct {
	left  int
	right int
}
type join struct {
	lookup    map[any][]int
	left      []any
	right     []any
	leftExpr  sqlparser.Expr
	rightExpr sqlparser.Expr
}

func (a association) get(left bool) int {
	if left {
		return a.left
	}
	return a.right
}
func (j *join) joinComparisonFunc(jFn joinFn) ([]association, error) {
	list := make([]association, 0)
	for rightIdx, row := range j.right {
		obj, err := unwrap[any](ExprReader(nil, row, j.rightExpr))
		if err != nil {
			return nil, err
		}
		switch objType := obj.(type) {
		case string, float64, bool:
			{
				jFn(objType, rightIdx, &list)
			}
		default:
			{
				return nil, UNSUPPORTED_CASE.Extend("only value types are valid on join conditions")
			}
		}
	}
	return list, nil
}

func (j join) joinOnEuqal(objType any, rightIdx int, list *[]association) {
	value, ok := j.lookup[objType]
	if ok {
		for _, leftIdx := range value {
			*list = append(*list, association{
				left:  leftIdx,
				right: rightIdx,
			})
		}
	}
}

func (j join) joinOnNotEqual(objType any, rightIdx int, list *[]association) {
	_, ok := j.lookup[objType]
	if !ok {
		for i := 0; i < len(j.left); i++ {
			*list = append(*list, association{
				left:  i,
				right: rightIdx,
			})
		}
	}
}

func jc[T float64 | string](a T, b2 any, op sqlparser.ComparisonExprOperator) (bool, error) {
	b, ok := b2.(T)
	if !ok {
		return false, INVALID_CAST
	}
	switch op {
	case sqlparser.EqualOp:
		{
			return a == b, nil
		}
	case sqlparser.NotEqualOp:
		{
			return a != b, nil
		}
	case sqlparser.GreaterThanOp:
		{
			return a > b, nil
		}
	case sqlparser.LessThanOp:
		{
			return a < b, nil
		}
	case sqlparser.GreaterEqualOp:
		{
			return a >= b, nil
		}
	case sqlparser.LessEqualOp:
		{
			return a <= b, nil
		}
	default:
		{
			return false, UNSUPPORTED_CASE
		}
	}
}

func To[T any](obj any) (T, error) {
	val, ok := obj.(T)
	if !ok {
		return *new(T), INVALID_CAST
	}
	return val, nil
}

func runJC[T float64 | string](lookup map[any][]int, obj T, rightIdx int, op sqlparser.ComparisonExprOperator) []association {
	list := make([]association, 0)
	for key, value := range lookup {
		ok, err := jc(obj, key, op)
		if err != nil {
			continue
		}
		if ok {
			for _, index := range value {
				list = append(list, association{
					left:  index,
					right: rightIdx,
				})
			}
		}
	}
	return list
}

func (j *join) joinComparison(expr *sqlparser.ComparisonExpr) ([]association, error) {
	j.leftExpr = expr.Left
	j.rightExpr = expr.Right
	lookup, err := leftToLookUp(j.left, j.leftExpr)
	if err != nil {
		return nil, err
	}
	j.lookup = lookup
	return j.joinComparisonFunc(func(obj any, rightIdx int, list *[]association) {
		switch objType := obj.(type) {
		case float64:
			{
				*list = append(*list, runJC(j.lookup, objType, rightIdx, expr.Operator)...)
			}
		case string:
			{
				*list = append(*list, runJC(j.lookup, objType, rightIdx, expr.Operator)...)
			}
		default:
			{
				panic(UNSUPPORTED_CASE)
			}
		}
	})
}
func (j *join) joinAnd(document map[string]any, expr *sqlparser.AndExpr) ([]association, error) {
	leftAscns, err := j.readJoinCond(document, expr.Left)
	if err != nil {
		return nil, err
	}
	rightAscns, err := j.readJoinCond(document, expr.Right)
	if err != nil {
		return nil, err
	}
	lookup := make(map[association]bool)
	for _, ascn := range leftAscns {
		lookup[ascn] = true
	}
	list := make([]association, 0)
	for _, ascn := range rightAscns {
		_, ok := lookup[ascn]
		if ok {
			list = append(list, ascn)
		}
	}
	return list, nil
}
func (j *join) joinOr(document map[string]any, expr *sqlparser.OrExpr) ([]association, error) {
	leftAscns, err := j.readJoinCond(document, expr.Left)
	if err != nil {
		return nil, err
	}
	rightAscns, err := j.readJoinCond(document, expr.Right)
	if err != nil {
		return nil, err
	}
	lookup := make(map[association]bool)
	for _, ascn := range leftAscns {
		lookup[ascn] = true
	}
	for _, ascn := range rightAscns {
		lookup[ascn] = true
	}
	list := make([]association, 0)
	for key := range lookup {
		list = append(list, key)
	}
	return list, nil
}
func (j *join) readJoinCond(document map[string]any, expr sqlparser.Expr) ([]association, error) {
	switch joinCondition := expr.(type) {
	case *sqlparser.ComparisonExpr:
		{
			return j.joinComparison(joinCondition)
		}
	case *sqlparser.AndExpr:
		{
			return j.joinAnd(document, joinCondition)
		}
	case *sqlparser.OrExpr:
		{
			return j.joinOr(document, joinCondition)
		}
	}
	return nil, nil
}
func leftToLookUp(left []any, leftExpr sqlparser.Expr) (map[any][]int, error) {
	lookup := make(map[any][]int)
	for index, row := range left {
		obj, err := unwrap[any](ExprReader(nil, row, leftExpr))
		if err != nil {
			return nil, err
		}
		switch objType := obj.(type) {
		case string, float64, bool:
			{
				_, ok := lookup[objType]
				if !ok {
					lookup[objType] = make([]int, 0)
				}
				lookup[objType] = append(lookup[objType], index)
			}
		default:
			{
				return nil, UNSUPPORTED_CASE.Extend("only value types are valid on join conditions")
			}
		}
	}
	return lookup, nil
}
func joinLookup(rs []association, list []any, left bool) map[int][]int {
	lookup := make(map[int][]int)
	for _, value := range rs {
		a := value.get(left)
		b := value.get(!left)
		_, ok := lookup[a]
		if !ok {
			lookup[a] = make([]int, 0)
		}
		lookup[a] = append(lookup[a], b)
	}
	for index := range list {
		_, ok := lookup[index]
		if !ok {
			lookup[index] = make([]int, 0)
		}
	}
	return lookup
}
func normalJoinExpr(rs []association, left []any, right []any) ([]any, error) {
	collect := make([]any, 0)
	for _, value := range rs {
		out := make(map[string]any)
		for key, value := range left[value.left].(map[string]any) {
			out[key] = value
		}
		for key, value := range right[value.right].(map[string]any) {
			out[key] = value
		}
		collect = append(collect, out)
	}
	return collect, nil
}
func leftJoinExpr(rs []association, left []any, right []any) ([]any, error) {
	lookup := joinLookup(rs, left, true)
	return execJoin(lookup, left, right), nil
}
func rightJoinExpr(rs []association, left []any, right []any) ([]any, error) {
	lookup := joinLookup(rs, right, false)
	return execJoin(lookup, right, left), nil
}
func execJoin(lookup map[int][]int, left []any, right []any) []any {
	collect := make([]any, 0)
	for index, value := range lookup {
		if len(value) > 0 {
			for _, v := range value {
				out := make(map[string]any)
				for key, value := range right[index].(map[string]any) {
					out[key] = value
				}
				for key, value := range left[v].(map[string]any) {
					out[key] = value
				}
				collect = append(collect, out)
			}
			continue
		}
		out := make(map[string]any)
		for key, value := range right[index].(map[string]any) {
			out[key] = value
		}
		for key := range left[0].(map[string]any) {
			out[key] = nil
		}
		collect = append(collect, out)
	}
	return collect
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
	join := join{
		left:  left,
		right: right,
	}
	rs, err := join.readJoinCond(document, expr.Condition.On)
	if err != nil {
		return nil, err
	}
	switch expr.Join {
	case sqlparser.NormalJoinType:
		{
			return normalJoinExpr(rs, left, right)
		}
	case sqlparser.LeftJoinType:
		{
			return leftJoinExpr(rs, left, right)
		}
	case sqlparser.RightJoinType:
		{
			return rightJoinExpr(rs, left, right)
		}
	}
	return nil, UNSUPPORTED_CASE
}
