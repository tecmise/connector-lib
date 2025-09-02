package connector

type ListResponse[T any] struct {
	Data            []T  `json:"data"`
	Total           int  `json:"total"`
	Page            int  `json:"page"`
	Limit           int  `json:"limit"`
	TotalPages      int  `json:"totalPages"`
	HasNextPage     bool `json:"hasNextPage"`
	HasPreviousPage bool `json:"hasPreviousPage"`
}
