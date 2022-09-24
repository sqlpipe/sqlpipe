package data

import (
	"database/sql"
	"fmt"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type DataSystem struct {
	SystemType         string `json:"system_type"`
	DriverName         string `json:"driver_name"`
	Host               string `json:"host"`
	Port               int    `json:"port"`
	AccountId          string `json:"account_id"`
	Username           string `json:"username"`
	Password           string `json:"password"`
	DbName             string `json:"db_name"`
	Schema             string `json:"schema"`
	Table              string `json:"table"`
	Db                 sql.DB `json:"-"`
	Writers            string `json:"writers"`
	RowsPerInsertQuery int    `json:"rows_per_insert_query"`
}

func ValidateDataSystem(v *validator.Validator, dataSystem DataSystem, sourceOrTarget string) {
	v.Check(dataSystem.SystemType != "", fmt.Sprintf("%v.system_type", sourceOrTarget), "must be provided")
	v.Check(dataSystem.DriverName != "", fmt.Sprintf("%v.driver_name", sourceOrTarget), "must be provided")
	v.Check(dataSystem.Username != "", fmt.Sprintf("%v.username", sourceOrTarget), "must be provided")
	v.Check(dataSystem.Password != "", fmt.Sprintf("%v.password", sourceOrTarget), "must be provided")
	v.Check(dataSystem.DbName != "", fmt.Sprintf("%v.db_name", sourceOrTarget), "must be provided")
	switch dataSystem.SystemType {
	case "snowflake":
		v.Check(dataSystem.AccountId != "", fmt.Sprintf("%v.account_id", sourceOrTarget), "must be provided for snowflake")
	default:
		v.Check(dataSystem.Host != "", fmt.Sprintf("%v.host", sourceOrTarget), "must be provided")
		v.Check(dataSystem.Port != 0, fmt.Sprintf("%v.port", sourceOrTarget), "must be provided")
	}
}
