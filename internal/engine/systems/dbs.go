package systems

import (
	"database/sql"

	"github.com/sqlpipe/sqlpipe/internal/engine/systems/dbs"
)

var DropTableCommandStarters = map[string]string{
	"postgresql": "drop table if exists",
	"mysql":      "drop table if exists",
}

var NullStrings = map[string]string{
	"postgresql": "null",
	"mysql":      "null",
}

var CreateFormatters = map[string]map[string]func(column *sql.ColumnType, terminator string) (string, error){
	"postgresql": dbs.PostgresqlCreateFormatters,
	"mysql":      dbs.MysqlCreateFormatters,
}

var ValFormatters = map[string]map[string]func(value interface{}, terminator string, nullString string) (string, error){
	"postgresql": dbs.PostgresqlValFormatters,
	"mysql":      dbs.MysqlValFormatters,
}
