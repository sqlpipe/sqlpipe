package data

import (
	"database/sql"
	"io/ioutil"
	"os"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type Target struct {
	SystemType       string `json:"system_type"`
	OdbcDsn          string `json:"odbc_dsn"`
	Schema           string `json:"schema"`
	Table            string `json:"table"`
	CsvWriteLocation string `json:"csv_write_location"`
	RowsPerWrite     int    `json:"rows_per_write"`
	Db               sql.DB `json:"-"`
}

func ValidateTarget(v *validator.Validator, target Target) {
	v.Check(target.SystemType != "", "target.system_type", "must be provided")
	v.Check(target.RowsPerWrite != 0, "target.rows_per_write", "must be provided")

	switch target.SystemType {
	case "csv":
		v.Check(target.CsvWriteLocation != "", "target.csv_write_location", "must be provided")
		v.Check(IsValidPath(target.CsvWriteLocation), "target.csv_write_location", "must be a valid file path")
	default:
		v.Check(target.OdbcDsn != "", "target.odbc_dsn", "must be provided")
		v.Check(target.Table != "", "target.table", "must be provided")
	}
}

func IsValidPath(fp string) bool {
	// Check if file already exists
	if _, err := os.Stat(fp); err == nil {
		return true
	}

	// Attempt to create it
	var d []byte
	if err := ioutil.WriteFile(fp, d, 0644); err == nil {
		os.Remove(fp) // And delete it
		return true
	}

	return false
}
