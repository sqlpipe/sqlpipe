package data

import (
	"github.com/sqlpipe/sqlpipe/internal/validator"
	"github.com/sqlpipe/sqlpipe/pkg"
)

type CsvExport struct {
	Source        Source `json:"source"`
	WriteLocation string `json:"write_location"`
	Query         string `json:"query"`
}

func ValidateCsvExport(v *validator.Validator, csvExport *CsvExport) {
	ValidateSource(v, csvExport.Source)
	if csvExport.WriteLocation != "" {
		v.Check(pkg.IsValidPath(csvExport.WriteLocation), "write_location", "must be a valid file path")
	}
	v.Check(csvExport.Query != "", "query", "must be provided")
}
