package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type System struct {
	systemType string
	connection *sql.DB
}

func newSystem(systemType, connectionString string) (*System, error) {
	connection, err := sql.Open(systemType, connectionString)
	if err != nil {
		return nil, fmt.Errorf("error opening connection to -> %v", systemType, err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pingCancel()
	err = connection.PingContext(pingCtx)
	if err != nil {
		return nil, fmt.Errorf("error pinging %v -> %v", systemType, err)
	}

	return &System{systemType: systemType, connection: connection}, nil
}

func (system *System) dropTable(schema, table string) error {
	switch system.systemType {
	case "postgresql":
		return system.postgresqlDropTable(schema, table)
	case "mysql":
		return system.mysqlDropTable(schema)
	case "mssql":
		return system.mssqlDropTable(schema, table)
	case "oracle":
		return system.oracleDropTable(schema, table)
	case "snowflake":
		return system.snowflakeDropTable(schema, table)
	default:
		panic("unknown system type")
	}
}

func (system *System) createTable(schema, table string, columnInfo ColumnInfo) error {
	switch system.systemType {
	case "postgresql":
	case "mysql":
		// return system.mssqlCreateTable(schema, table, columnInfo)
	case "mssql":
	case "oracle":
	case "snowflake":
	default:
		panic("unknown system type")
	}
	return nil
}
