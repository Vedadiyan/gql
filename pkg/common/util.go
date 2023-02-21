package common

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/vedadiyan/gql/pkg/sentinel"
	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

func Wrap(val any, err error) any {
	if err != nil {
		return err
	}
	return val
}

func UnWrap[T any](val any) (output T, err error) {
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
		return *new(T), sentinel.SQLError(err.Error())
	}
	rs, ok := val.(T)
	if !ok {
		r := new(T)
		return *new(T), sentinel.INVALID_CAST.Extend(fmt.Sprintf("%T cannot be cast to %T", val, r))
	}
	return rs, nil
}
func UnWrapAny(val any) (any, error) {
	err, ok := val.(error)
	if ok {
		return nil, sentinel.SQLError(err.Error())
	}
	return val, nil
}

func IsColumnName(opt ...any) bool {
	return len(opt) > 0 && opt[0] == true
}

func HasGroupBy(opt ...any) (GroupBy, bool) {
	for _, value := range opt {
		if groupBy, ok := value.(GroupBy); ok {
			return groupBy, true
		}
	}
	return nil, false
}

func IsIndex(key string) bool {
	return strings.HasPrefix(key, "{") && strings.HasSuffix(key, "}")
}
func GetIndex(str string) string {
	str = strings.TrimPrefix(str, "{")
	str = strings.TrimSuffix(str, "}")
	return str
}
func IsWildCard(str string) bool {
	return str == "?"
}
func IsWildCardIndex(index int) bool {
	return index == -1
}
func IsEOA(i int, len int) bool {
	return i < len-1
}
func ToArray(v any) []any {
	array, ok := v.([]any)
	if !ok {
		return []any{v}
	}
	return array
}
func Index(key string) (int, error) {
	str := GetIndex(key)
	if IsWildCard(str) {
		return -1, nil
	}
	index, err := strconv.Atoi(str)
	return index, err
}

func IsSpecialFunction(expr *sqlparser.FuncExpr) bool {
	return expr.Name.String() == "ONCE"
}

func RemoveComments(query string) string {
	buffer := bytes.NewBufferString("")
	hold := false
	jump := false
	count := 0
	data := strings.FieldsFunc(query, func(r rune) bool {
		return r == '\r' || r == '\n'
	})
	for _, line := range data {
		for _, c := range line {
			if jump {
				jump = !jump
			} else if hold {
				if c == '\\' {
					jump = true
				}
				if c == '\'' {
					hold = false
				}
			} else if c == '\'' {
				hold = true
			} else if c == '-' {
				count++
				if count == 2 {
					break
				}
				continue
			} else {
				count = 0
			}
			buffer.WriteRune(c)
		}
		buffer.WriteString("\r\n")
	}
	return buffer.String()
}

func BoolToFloat64(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func To[T any](obj any) (T, error) {
	val, ok := obj.(T)
	if !ok {
		return *new(T), sentinel.INVALID_CAST
	}
	return val, nil
}
