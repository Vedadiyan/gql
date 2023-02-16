package sql

import (
	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

type association struct {
	left  int
	right int
}

func joinComparisonFunc(lookup map[any][]int, left []any, right []any, leftName string, rightName string, fn func(lookup map[any][]int, objType any, rightIdx int, list *[]association)) ([]association, error) {
	list := make([]association, 0)
	for rightIdx, row := range right {
		obj, err := Select(row.(map[string]any), rightName)
		if err != nil {
			return nil, err
		}
		switch objType := obj.(type) {
		case string, float64, bool:
			{
				fn(lookup, objType, rightIdx, &list)
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

func joinComparison(expr *sqlparser.ComparisonExpr, left []any, right []any) ([]association, error) {
	leftName, err := unwrap[string](ExprReader(nil, nil, expr.Left, true))
	if err != nil {
		return nil, err
	}
	rightName, err := unwrap[string](ExprReader(nil, nil, expr.Right, true))
	if err != nil {
		return nil, err
	}
	lookup, err := leftToLookUp(left, leftName)
	if err != nil {
		return nil, err
	}
	switch expr.Operator {
	case sqlparser.EqualOp:
		{
			return joinComparisonFunc(lookup, left, right, leftName, rightName, func(lookup map[any][]int, objType any, rightIdx int, list *[]association) {
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
			return joinComparisonFunc(lookup, left, right, leftName, rightName, func(lookup map[any][]int, objType any, rightIdx int, list *[]association) {
				_, ok := lookup[objType]
				if !ok {
					for i := 0; i < len(left); i++ {
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

func joinAnd(document map[string]any, expr *sqlparser.AndExpr, left []any, right []any) ([]association, error) {
	leftAssociations, err := readJoinCond(document, expr.Left, left, right)
	if err != nil {
		return nil, err
	}
	rightAssociations, err := readJoinCond(document, expr.Right, left, right)
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

func joinOr(document map[string]any, expr *sqlparser.OrExpr, left []any, right []any) ([]association, error) {
	leftAssociations, err := readJoinCond(document, expr.Left, left, right)
	if err != nil {
		return nil, err
	}
	rightAssociations, err := readJoinCond(document, expr.Right, left, right)
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

func readJoinCond(document map[string]any, expr sqlparser.Expr, left []any, right []any) ([]association, error) {
	switch joinCondition := expr.(type) {
	case *sqlparser.ComparisonExpr:
		{
			return joinComparison(joinCondition, left, right)
		}
	case *sqlparser.AndExpr:
		{
			return joinAnd(document, joinCondition, left, right)
		}
	case *sqlparser.OrExpr:
		{
			return joinOr(document, joinCondition, left, right)
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
