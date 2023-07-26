package main

import (
	"database/sql"
	"fmt"
)

type System interface {
	dropTable(schema, table string) error
	createTable(schema, table string, columnInfo []ColumnInfo) error
	query(query string) (*sql.Rows, error)
	getColumnInfo(rows *sql.Rows) ([]ColumnInfo, error)
	writeCsv(rows *sql.Rows, columnInfo []ColumnInfo, transferId string) (tmpDir string, err error)
}

func newSystem(name, systemType, connectionString string) (System, error) {
	switch systemType {
	case "postgresql":
		return newPostgresql(name, connectionString)
	case "mssql":
		return newMssql(name, connectionString)
	default:
		return nil, fmt.Errorf("unsupported system type %v", systemType)
	}
}
