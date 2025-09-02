package connector

type (
	Call[T any] interface {
		Find(parameter Parameter, response *T) error
		List(parameter Parameter, response *[]T) error
		Page(parameter Parameter, response *ListResponse[T]) error
		Ids(parameter Parameter, response *[]int64) error
		Strings(parameter Parameter, response *[]string) error
		Create(parameter Parameter, response *T) error
		Update(parameter Parameter, response *T) error
		Inative(parameter Parameter, response *T) error
	}
)
