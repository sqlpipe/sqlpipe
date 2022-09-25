package data

import (
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type Export struct {
	Source    Source    `json:"source"`
	CsvTarget CsvTarget `json:"csv_target"`
	Query     string    `json:"query"`
}

func ValidateExport(v *validator.Validator, export *Export) {
	ValidateSource(v, export.Source)
	ValidateCsvTarget(v, export.CsvTarget)
	v.Check(export.Query != "", "query", "must be provided")
}
