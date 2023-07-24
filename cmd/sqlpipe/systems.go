package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type System struct {
	systemType string
	name       string
	driverName string
	connection *sql.DB
}

func newSystem(systemType, name, connectionString, driverName string) (*System, error) {
	connection, err := sql.Open(driverName, connectionString)
	if err != nil {
		return nil, fmt.Errorf("error opening connection to %v -> %v", systemType, err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pingCancel()
	err = connection.PingContext(pingCtx)
	if err != nil {
		return nil, fmt.Errorf("error pinging %v -> %v", name, err)
	}

	return &System{
		systemType: systemType,
		name:       name,
		driverName: driverName,
		connection: connection,
	}, nil
}

func (system *System) dropTable(schema, table string) error {
	switch system.systemType {
	case "postgresql":
		return system.postgresqlDropTable(schema, table)
	case "mysql":
		return system.mysqlDropTable(table)
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

func (system *System) createTable(schema, table string, columnInfo []ColumnInfo) error {
	switch system.systemType {
	case "postgresql":
		return system.postgresqlCreateTable(schema, table, columnInfo)
	case "mysql":
		return system.mysqlCreateTable(table, columnInfo)
	case "mssql":
		return system.mssqlCreateTable(schema, table, columnInfo)
	case "oracle":
		return system.oracleCreateTable(schema, table, columnInfo)
	case "snowflake":
		return system.snowflakeCreateTable(schema, table, columnInfo)

	default:
		panic("unknown system type")
	}
	return nil
}
