package sql

import (
	"fmt"
	"regexp"
	"strings"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/sentinel"
	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

func SimpleGenericComparison[T float64 | string](a T, b2 any, op sqlparser.ComparisonExprOperator) (bool, error) {
	b, ok := b2.(T)
	if !ok {
		return false, sentinel.INVALID_CAST
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
			return false, sentinel.UNSUPPORTED_CASE
		}
	}
}

// TODO
// function needs revision
func equalityCompare(left any, right any, op string) (bool, error) {
	lv, err := cmn.UnWrapAny(left)
	if err != nil {
		return false, err
	}
	rv, err := cmn.UnWrapAny(right)
	if err != nil {
		return false, err
	}
	switch op {
	case "=":
		{
			array, ok := lv.([]any)
			if !ok {
				return fmt.Sprintf("%v", lv) == fmt.Sprintf("%v", rv), nil
			}
			for _, val := range array {
				if fmt.Sprintf("%v", val) == fmt.Sprintf("%v", rv) {
					return true, nil
				}
			}
			return false, nil
		}
	case "<>":
		fallthrough
	case "!=":
		{
			array, ok := lv.([]any)
			if !ok {
				return fmt.Sprintf("%v", lv) != fmt.Sprintf("%v", rv), nil
			}
			for _, val := range array {
				if fmt.Sprintf("%v", val) != fmt.Sprintf("%v", rv) {
					return true, nil
				}
			}
			return false, nil

		}
	}
	return false, sentinel.UNDEFINED_OPERATOR.Extend(op)
}

func getFloat64Nullable(value any) (*float64, error) {
	if value == nil {
		return nil, nil
	}
	val, err := cmn.UnWrap[float64](value)
	if err != nil {
		return nil, err
	}
	return &val, nil
}

func numericCompare(left any, right any, operator string) (bool, error) {
	lv, err := getFloat64Nullable(left)
	if err != nil {
		return false, err
	}
	rv, err := getFloat64Nullable(right)
	if err != nil {
		return false, err
	}
	switch operator {
	case ">":
		{
			if lv == nil || rv == nil {
				return false, nil
			}
			return *lv > *rv, nil
		}
	case "<":
		{
			if lv == nil || rv == nil {
				return false, nil
			}
			return *lv < *rv, nil
		}
	case "<=":
		{
			if lv == nil && rv == nil {
				return true, nil
			}
			if lv == nil || rv == nil {
				return false, nil
			}
			return *lv <= *rv, nil
		}
	case ">=":
		{
			if lv == nil && rv == nil {
				return true, nil
			}
			if lv == nil || rv == nil {
				return false, nil
			}
			return *lv >= *rv, nil
		}
	}
	return false, sentinel.UNDEFINED_OPERATOR.Extend(operator)
}

func inComparison(left any, right any, operator string) (bool, error) {
	lv, err := cmn.UnWrapAny(left)
	if err != nil {
		return false, err
	}
	rv, err := cmn.UnWrap[[]any](right)
	if err != nil {
		return false, err
	}
	switch operator {
	case "in":
		{
			for _, value := range rv {
				if lv == value {
					return true, nil
				}
			}
			return false, nil
		}
	case "not in":
		{
			for _, value := range rv {
				if lv == value {
					return false, nil
				}
			}
			return true, nil
		}
	}
	return false, sentinel.UNDEFINED_OPERATOR.Extend(operator)
}

func isNull(left any, operator string) (bool, error) {
	switch operator {
	case "is null":
		{
			return left == nil, nil
		}
	case "is not null":
		{
			return left != nil, nil
		}
	}
	return false, sentinel.UNDEFINED_OPERATOR.Extend(operator)
}

func boolComparison(left any, operator string) (bool, error) {
	value, err := cmn.UnWrap[bool](left)
	if err != nil {
		return false, err
	}
	switch operator {
	case "is true":
		{
			return value, nil
		}
	case "is not true":
		{
			return !value, nil
		}
	case "is false":
		{
			return !value, nil
		}
	case "is not false":
		{
			return value, nil
		}
	}
	return false, sentinel.UNDEFINED_OPERATOR.Extend(operator)
}

func regexComparison(left any, pattern string) (bool, error) {
	regExpr := strings.ReplaceAll(strings.ToLower(pattern), "_", ".")
	regExpr = strings.ReplaceAll(regExpr, "%", ".*")
	regExpr = "^" + regExpr + "$"
	if array, ok := left.([]any); ok {
		for _, val := range array {
			b, err := regexp.Match(regExpr, []byte(strings.ToLower(fmt.Sprintf("%v", val))))
			if err != nil {
				return false, err
			}
			if b {
				return true, nil
			}
		}
		return false, nil
	}
	return regexp.Match(regExpr, []byte(strings.ToLower(fmt.Sprintf("%v", left))))
}
