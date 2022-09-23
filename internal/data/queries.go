package data

import (
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type Query struct {
	Target DataSystem `json:"target"`
	Query  string     `json:"query"`
}

func ValidateQuery(v *validator.Validator, query *Query) {
	ValidateDataSystem(v, query.Target, "target")
	v.Check(query.Query != "", "query", "must be provided")
}
