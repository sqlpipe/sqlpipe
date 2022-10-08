package data

import (
	"database/sql"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type Target struct {
	SystemType string  `json:"system_type"`
	OdbcDsn    string  `json:"odbc_dsn"`
	Schema     string  `json:"schema"`
	Table      string  `json:"table"`
	Db         *sql.DB `json:"-"`
}

func ValidateTarget(v *validator.Validator, target Target) {
	v.Check(target.SystemType != "", "target->system_type", "must be provided")
	v.Check(target.OdbcDsn != "", "target->odbc_dsn", "must be provided")
	v.Check(target.Table != "", "target->table", "must be provided")
}
