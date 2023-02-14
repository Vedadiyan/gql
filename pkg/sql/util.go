package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func wrap(val any, err error) any {
	if err != nil {
		return err
	}
	return val
}

func unwrap[T any](val any) (output T, err error) {
	if val == nil {
		return *new(T), nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	err, ok := val.(error)
	if ok {
		return *new(T), SQLError(err.Error())
	}
	rs, ok := val.(T)
	if !ok {
		r := new(T)
		return *new(T), INVALID_CAST.Extend(fmt.Sprintf("%T cannot be cast to %T", val, r))
	}
	return rs, nil
}
func unwrapAny(val any) (any, error) {
	err, ok := val.(error)
	if ok {
		return nil, SQLError(err.Error())
	}
	return val, nil
}

func isColumnName(opt ...any) bool {
	return len(opt) > 0 && opt[0] == true
}

func hasGroupBy(opt ...any) (GroupBy, bool) {
	for _, value := range opt {
		if groupBy, ok := value.(GroupBy); ok {
			return groupBy, true
		}
	}
	return nil, false
}

func isIndex(key string) bool {
	return strings.HasPrefix(key, "{") && strings.HasSuffix(key, "}")
}

func index(key string) (int, error) {
	str := strings.TrimSuffix(strings.TrimPrefix(key, "{"), "}")
	if str == "?" {
		return -1, nil
	}
	index, err := strconv.ParseInt(str, 10, 32)
	return int(index), err
}

func isSpecialFunction(expr *sqlparser.FuncExpr) bool {
	return expr.Name.String() == "ONCE"
}
