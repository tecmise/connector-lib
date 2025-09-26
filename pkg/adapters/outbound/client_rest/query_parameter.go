package client_rest

import (
	"fmt"
)

type QueryParameter struct {
	Name  string
	Value interface{}
}

func stringQueryParameter(name, value string, index *int) string {
	if index != nil {
		return fmt.Sprintf("&%s[%d]=%s", name, *index, value)
	}
	return fmt.Sprintf("&%s=%s", name, value)
}

func intQueryParameter(name string, value int, index *int) string {
	if index != nil {
		return fmt.Sprintf("&%s[%d]=%d", name, *index, value)
	}
	return fmt.Sprintf("&%s=%d", name, value)
}

func boolQueryParameter(name string, value bool, index *int) string {
	if index != nil {
		return fmt.Sprintf("&%s[%d]=%t", name, *index, value)
	}
	return fmt.Sprintf("&%s=%t", name, value)
}

func floatQueryParameter(name string, value float64, index *int) string {
	if index != nil {
		return fmt.Sprintf("&%s[%d]=%f", name, *index, value)
	}
	return fmt.Sprintf("&%s=%f", name, value)
}

func int64QueryParameter(name string, value int64, index *int) string {
	if index != nil {
		return fmt.Sprintf("&%s[%d]=%d", name, *index, value)
	}
	return fmt.Sprintf("&%s=%d", name, value)
}

func uintQueryParameter(name string, value uint, index *int) string {
	if index != nil {
		return fmt.Sprintf("&%s[%d]=%d", name, *index, value)
	}
	return fmt.Sprintf("&%s=%d", name, value)
}

func GetQueryParameters(inputs ...QueryParameter) string {
	var query string

	for _, input := range inputs {
		if input.Value == nil || input.Name == "" {
			continue
		}

		switch v := input.Value.(type) {

		case []string:
			for i, val := range v {
				query += stringQueryParameter(input.Name, val, &i)
			}
		case []int:
			for i, val := range v {
				query += intQueryParameter(input.Name, val, &i)
			}

		case []bool:
			for i, val := range v {
				query += boolQueryParameter(input.Name, val, &i)
			}
		case []float64:
			for i, val := range v {
				query += floatQueryParameter(input.Name, val, &i)
			}
		case []int64:
			for i, val := range v {
				query += int64QueryParameter(input.Name, val, &i)
			}
		case []uint:
			for i, val := range v {
				query += uintQueryParameter(input.Name, val, &i)
			}

		case string:
			query += stringQueryParameter(input.Name, v, nil)
		case int:
			query += intQueryParameter(input.Name, v, nil)
		case bool:
			query += boolQueryParameter(input.Name, v, nil)
		case float64:
			query += floatQueryParameter(input.Name, v, nil)
		case float32:
			query += floatQueryParameter(input.Name, float64(v), nil)
		case int32:
			query += int64QueryParameter(input.Name, int64(v), nil)
		case int64:
			query += int64QueryParameter(input.Name, v, nil)
		case uint:
			query += uintQueryParameter(input.Name, v, nil)
		case uint32:
			query += uintQueryParameter(input.Name, uint(v), nil)
		case uint64:
			query += uintQueryParameter(input.Name, uint(v), nil)

		default:
			query += fmt.Sprintf("&%s=%v", input.Name, v)
		}
	}

	if len(query) > 0 {
		return query[1:]
	} else {
		return ""
	}
}
