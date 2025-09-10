package request

import "github.com/go-playground/validator/v10"

type (
	Validatable interface {
		Validate(functions ...CustomValidator) error
	}

	CustomValidator struct {
		Name   string
		Method func(fl validator.FieldLevel) bool
	}
)
