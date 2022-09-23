package data

import (
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type Transfer struct {
	Source DataSystem `json:"source"`
	Target DataSystem `json:"target"`
	Query  string     `json:"query"`
}

func ValidateTransfer(v *validator.Validator, transfer *Transfer) {
	ValidateDataSystem(v, transfer.Source, "source")
	ValidateDataSystem(v, transfer.Target, "target")
	v.Check(transfer.Query != "", "query", "must be provided")
}
