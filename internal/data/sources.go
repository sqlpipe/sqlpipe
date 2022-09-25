package data

import (
	"database/sql"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type Source struct {
	OdbcDsn string `json:"odbc_dsn"`
	Db      sql.DB `json:"-"`
}

func ValidateSource(v *validator.Validator, source Source) {
	v.Check(source.OdbcDsn != "", "source->odbc_dsn", "must be provided")
}
