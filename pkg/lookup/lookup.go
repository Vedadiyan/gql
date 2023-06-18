package lookup

import (
	"strconv"
	"strings"
)

func ReadObject(row map[string]any, key string) (any, error) {
	ref := any(row)
	keys := strings.Split(key, ".")
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		if strings.HasPrefix(key, "{") && strings.HasSuffix(key, "}") {
			arr, ok := ref.([]any)
			if !ok {
				return ref, nil
			}
			if key == "{?}" {
				if i < len(keys)-1 && !strings.HasPrefix(keys[i+1], "{") && !strings.HasSuffix(keys[i+1], "}") {
					array := make([]any, 0)
					for _, v := range arr {
						switch t := v.(type) {
						case map[string]any:
							{
								switch t := t[keys[i+1]].(type) {
								case []any:
									{
										array = append(array, t...)
									}
								default:
									{
										array = append(array, t)
									}
								}
							}
						case []any:
							{
								for _, v := range t {
									switch t := v.(type) {
									case map[string]any:
										{
											array = append(array, t[keys[i+1]])
										}
									case []any:
										{
											array = append(array, t...)
										}
									default:
										{
											array = append(array, t)
										}
									}
								}
							}
						}

					}
					ref = array
					i++
					continue
				}
				array := make([]any, 0)
				for _, v := range arr {
					switch t := v.(type) {
					case map[string]any:
						{
							array = append(array, t[keys[i+1]])
						}
					case []any:
						{
							array = append(array, t...)
						}
					default:
						{
							array = append(array, t)
						}
					}
				}
				ref = array
				continue
			}
			key = strings.TrimPrefix(key, "{")
			key = strings.TrimSuffix(key, "}")
			index, err := strconv.ParseInt(key, 10, 32)
			if err != nil {
				return nil, err
			}

			if int(index) >= len(arr) {
				continue
			}
			if i < len(keys)-1 && !strings.HasPrefix(keys[i+1], "{") && !strings.HasSuffix(keys[i+1], "}") {
				array := make([]any, 0)
				switch t := arr[index].(type) {
				case map[string]any:
					{
						switch t := t[keys[i+1]].(type) {
						case []any:
							{
								array = append(array, t...)
							}
						default:
							{
								array = append(array, t)
							}
						}
					}
				case []any:
					{
						for _, v := range t {
							switch t := v.(type) {
							case map[string]any:
								{
									array = append(array, t[keys[i+1]])
								}
							case []any:
								{
									array = append(array, t...)
								}
							default:
								{
									array = append(array, t)
								}
							}
						}
					}
				}
				ref = array
				i++
				continue
			}
			ref = arr[index]
			continue
		}
		switch t := ref.(type) {
		case map[string]any:
			{
				// Lazy CTE execution
				if fn, ok := t[key].(func() (any, error)); ok {
					res, err := fn()
					if err != nil {
						return nil, err
					}
					t[key] = res
				}
				ref = t[key]
			}
		case []any:
			{
				array := make([]any, 0)
				for _, v := range t {
					switch t := v.(type) {
					case map[string]any:
						{
							array = append(array, t[key])
						}
					case []any:
						{
							array = append(array, t...)
						}
					default:
						{
							array = append(array, t)
						}
					}
				}
				ref = array
			}
		}
	}
	return ref, nil
}
func ToResult(obj any, recuring bool) any {
	return obj
}
