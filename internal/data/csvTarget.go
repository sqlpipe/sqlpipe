package data

import (
	"io/ioutil"
	"os"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type CsvTarget struct {
	CsvWriteLocation string `json:"csv_write_location"`
}

func ValidateCsvTarget(v *validator.Validator, csvTarget CsvTarget) {
	v.Check(csvTarget.CsvWriteLocation != "", "csv_target->csv_write_location", "must be provided")
	v.Check(IsValidPath(csvTarget.CsvWriteLocation), "csv_target->csv_write_location", "must be a valid file path")
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
