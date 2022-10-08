package data

import (
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type Transfer struct {
	Source          Source `json:"source"`
	Target          Target `json:"target"`
	Query           string `json:"query"`
	DropTargetTable bool   `json:"drop_target_table"`
}

func ValidateTransfer(v *validator.Validator, transfer *Transfer) {
	ValidateSource(v, transfer.Source)
	ValidateTarget(v, transfer.Target)
	v.Check(transfer.Query != "", "query", "must be provided")
}
