package main

import (
	"database/sql"
	"fmt"
)

type System interface {
	dropTable(schema, table string) (err error)
	createTable(schema, table string, columnInfo []ColumnInfo) error
	query(query string) (*sql.Rows, error)
	exec(query string) (err error)
	getColumnInfo(rows *sql.Rows) ([]ColumnInfo, error)
	getPipeFileFormatters() map[string]func(interface{}) (string, error)
	dbTypeToPipeType(databaseTypeName string, columnType sql.ColumnType) (pipeType string, err error)
	pipeTypeToCreateType(columnInfo ColumnInfo) (createType string, err error)
	createPipeFiles(rows *sql.Rows, columnInfo []ColumnInfo, transferId string) (pipeFilesDir string, err error)
	insertPipeFiles(tmpDir, transferId string, columnInfo []ColumnInfo, table, schema string) error
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
