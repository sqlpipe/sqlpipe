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
	v.Check(transfer.Target.Table != "", "target.table", "must be provided")
	v.Check(transfer.Target.RowsPerInsertQuery != 0, "target.rows_per_insert_query", "must be provided")

	switch transfer.Target.SystemType {
	case "oracle", "mysql":
	default:
		v.Check(transfer.Target.Schema != "", "target.schema", "must be provided")
	}
}
