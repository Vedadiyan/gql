package common

import "sync"

type Function func(jo *[]any, row any, args []any) (any, error)

var (
	Functions map[string]Function
	Cache     sync.Map
)

func init() {
	Functions = make(map[string]Function)
}

func RegisterFunction(name string, fn Function) {
	Functions[name] = fn
}
