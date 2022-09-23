package data

import (
	"database/sql"
	"fmt"

	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type DataSystem struct {
	DriverName string `json:"driver_name"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	DbName     string `json:"db_name"`
	Schema     string `json:"schema"`
	Db         sql.DB `json:"-"`
}

func ValidateDataSystem(v *validator.Validator, dataSystem DataSystem, sourceOrTarget string) {
	v.Check(dataSystem.DriverName != "", fmt.Sprintf("%v.driver_name", sourceOrTarget), "must be provided")
	v.Check(dataSystem.Host != "", fmt.Sprintf("%v.host", sourceOrTarget), "must be provided")
	v.Check(dataSystem.Port != 0, fmt.Sprintf("%v.port", sourceOrTarget), "must be provided")
	v.Check(dataSystem.Username != "", fmt.Sprintf("%v.username", sourceOrTarget), "must be provided")
	v.Check(dataSystem.Password != "", fmt.Sprintf("%v.password", sourceOrTarget), "must be provided")
	v.Check(dataSystem.DbName != "", fmt.Sprintf("%v.db_name", sourceOrTarget), "must be provided")
}
