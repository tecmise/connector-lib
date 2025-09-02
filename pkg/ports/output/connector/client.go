package connector

type (
	Result[T any] struct {
		Code    int `json:"code"`
		Content T   `json:"content"`
	}
)
