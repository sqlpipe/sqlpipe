package data

import (
	"database/sql"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type Target struct {
	SystemType string `json:"system_type"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	DbName     string `json:"db_name"`
	Schema     string `json:"schema"`
	Db         sql.DB `json:"-"`
}

func ValidateTarget(v *validator.Validator, target Target) {

}
