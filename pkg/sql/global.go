package sql

import "sync"

type Function func(jo *[]any, row any, args []any) any

var (
	_functions map[string]Function
	_cache     sync.Map
)

func init() {
	_functions = make(map[string]Function)
}

func RegisterFunction(name string, fn Function) {
	_functions[name] = fn
}
