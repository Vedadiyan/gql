package functions

func Expand(in []any) []any {
	out := make([]any, 0)
	for _, item := range in {
		array, ok := item.([]any)
		if ok {
			out = append(out, Expand(array)...)
			continue
		}
		out = append(out, item)
	}
	return out
}
