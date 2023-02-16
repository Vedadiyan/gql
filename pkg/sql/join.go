package sql

import (
	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

type association struct {
	left  int
	right int
}

type join struct {
	lookup    map[any][]int
	left      []any
	right     []any
	leftName  string
	rightName string
}

func (j *join) joinComparisonFunc(fn func(lookup map[any][]int, objType any, rightIdx int, list *[]association)) ([]association, error) {
	list := make([]association, 0)
	for rightIdx, row := range j.right {
		obj, err := Select(row.(map[string]any), j.rightName)
		if err != nil {
			return nil, err
		}
		switch objType := obj.(type) {
		case string, float64, bool:
			{
				fn(j.lookup, objType, rightIdx, &list)
			}
		default:
			{
				return nil, UNSUPPORTED_CASE.Extend("only value types are valid on join conditions")
			}
		}
	}
	return list, nil
}

func leftToLookUp(left []any, leftName string) (map[any][]int, error) {
	lookup := make(map[any][]int)
	for index, row := range left {
		obj, err := Select(row.(map[string]any), leftName)
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

func (j *join) joinComparison(expr *sqlparser.ComparisonExpr) ([]association, error) {
	leftName, err := unwrap[string](ExprReader(nil, nil, expr.Left, true))
	if err != nil {
		return nil, err
	}
	j.leftName = leftName
	rightName, err := unwrap[string](ExprReader(nil, nil, expr.Right, true))
	if err != nil {
		return nil, err
	}
	j.rightName = rightName
	lookup, err := leftToLookUp(j.left, leftName)
	if err != nil {
		return nil, err
	}
	j.lookup = lookup
	switch expr.Operator {
	case sqlparser.EqualOp:
		{
			return j.joinComparisonFunc(func(lookup map[any][]int, objType any, rightIdx int, list *[]association) {
				value, ok := lookup[objType]
				if ok {
					for _, leftIdx := range value {
						*list = append(*list, association{
							left:  leftIdx,
							right: rightIdx,
						})
					}
				}
			})
		}
	case sqlparser.NotEqualOp:
		{
			return j.joinComparisonFunc(func(lookup map[any][]int, objType any, rightIdx int, list *[]association) {
				_, ok := lookup[objType]
				if !ok {
					for i := 0; i < len(j.left); i++ {
						*list = append(*list, association{
							left:  i,
							right: rightIdx,
						})
					}
				}
			})
		}
	}
	return nil, nil
}

func (j *join) joinAnd(document map[string]any, expr *sqlparser.AndExpr) ([]association, error) {
	leftAssociations, err := j.readJoinCond(document, expr.Left)
	if err != nil {
		return nil, err
	}
	rightAssociations, err := j.readJoinCond(document, expr.Right)
	if err != nil {
		return nil, err
	}
	lookup := make(map[association]bool)
	for _, association := range leftAssociations {
		lookup[association] = true
	}
	list := make([]association, 0)
	for _, association := range rightAssociations {
		_, ok := lookup[association]
		if ok {
			list = append(list, association)
		}
	}
	return list, nil
}

func (j *join) joinOr(document map[string]any, expr *sqlparser.OrExpr) ([]association, error) {
	leftAssociations, err := j.readJoinCond(document, expr.Left)
	if err != nil {
		return nil, err
	}
	rightAssociations, err := j.readJoinCond(document, expr.Right)
	if err != nil {
		return nil, err
	}
	lookup := make(map[association]bool)
	for _, association := range leftAssociations {
		lookup[association] = true
	}
	for _, association := range rightAssociations {
		lookup[association] = true
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
	case sqlparser.LeftJoinType:
		{
			lookup := make(map[int][]int)
			for _, value := range rs {
				_, ok := lookup[value.left]
				if !ok {
					lookup[value.left] = make([]int, 0)
				}
				lookup[value.left] = append(lookup[value.left], value.right)
			}
			for index := range left {
				_, ok := lookup[index]
				if !ok {
					lookup[index] = make([]int, 0)
				}
			}
			collect := make([]any, 0)
			for index, value := range lookup {
				if len(value) > 0 {
					for _, v := range value {
						out := make(map[string]any)
						for key, value := range left[index].(map[string]any) {
							out[key] = value
						}
						for key, value := range right[v].(map[string]any) {
							out[key] = value
						}
						collect = append(collect, out)
					}
					continue
				}
				out := make(map[string]any)
				for key, value := range left[index].(map[string]any) {
					out[key] = value
				}
				for key := range right[0].(map[string]any) {
					out[key] = nil
				}
				collect = append(collect, out)
			}
			return collect, nil
		}
	case sqlparser.RightJoinType:
		{
			lookup := make(map[int][]int)
			for _, value := range rs {
				_, ok := lookup[value.right]
				if !ok {
					lookup[value.right] = make([]int, 0)
				}
				lookup[value.right] = append(lookup[value.right], value.left)
			}
			for index := range right {
				_, ok := lookup[index]
				if !ok {
					lookup[index] = make([]int, 0)
				}
			}
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
			return collect, nil
		}
	}
	return nil, UNSUPPORTED_CASE
}
