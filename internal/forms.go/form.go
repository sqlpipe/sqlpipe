package forms

import (
	"net/url"

	"github.com/calmitchell617/sqlpipe/internal/validator"
)

type Form struct {
	url.Values
	Validator *validator.Validator
}

func New(data url.Values) *Form {
	return &Form{
		data,
		validator.New(),
	}
}

func (f *Form) Valid() bool {
	return len(f.Validator.Errors) == 0
}
