package request

import (
	"errors"
	"github.com/go-playground/validator/v10"
)

type (
	Validatable interface {
		Validate(functions ...CustomValidator) error
	}

	CustomValidator struct {
		Name   string
		Method func(fl validator.FieldLevel) bool
	}
)

func ValidateObject(req Validatable, validations ...CustomValidator) error {
	if req == nil {
		return errors.New("the request cannot be nil")
	}
	if err := req.Validate(validations...); err != nil {
		return err
	}
	return nil
}
