package data

import (
	"database/sql"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type Source struct {
	SystemType string `json:"system_type"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	DbName     string `json:"db_name"`
	Schema     string `json:"schema"`
	Db         sql.DB `json:"-"`
}

func ValidateSource(v *validator.Validator, source Source) {
	v.Check(source.Host != "", "source_host", "must be provided")
	v.Check(source.Port != 0, "source_port", "must be provided")
	v.Check(source.Username != "", "source_username", "must be provided")
	v.Check(source.Password != "", "source_password", "must be provided")
	v.Check(source.DbName != "", "source_db_name", "must be provided")
}
