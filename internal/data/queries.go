package data

import (
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type Query struct {
	Source Source `json:"source"`
	Query  string `json:"query"`
}

func ValidateQuery(v *validator.Validator, query *Query) {
	ValidateSource(v, query.Source)
	v.Check(query.Query != "", "query", "must be provided")
}
