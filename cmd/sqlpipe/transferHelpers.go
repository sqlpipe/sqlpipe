package main

import (
	"database/sql"
	"fmt"
)

func setSourceAndTargetName(transfer Transfer) Transfer {
	if transfer.SourceName == "" {
		transfer.SourceName = transfer.SourceType
	}
	if transfer.TargetName == "" {
		transfer.TargetName = transfer.TargetType
	}

	return transfer
}

var intermediateTypes = map[string]map[string]string{
	"postgresql": postgresqlIntermediateTypes,
	// "mysql":      mysqlIntermediateTypes,
	// "mssql":      mssqlIntermediateTypes,
	// "oracle":     oracleIntermediateTypes,
	// "snowflake":  snowflakeIntermediateTypes,
}

type ColumnInfo struct {
	name         string
	databaseType string
	scanType     string
	decimalOk    bool
	precision    int64
	scale        int64
	lengthOk     bool
	length       int64
	nullableOk   bool
	nullable     bool
}

func getColumnInfo(rows *sql.Rows) ([]ColumnInfo, error) {
	columnInfo := []ColumnInfo{}

	columnNames, err := rows.Columns()
	if err != nil {
		return columnInfo, fmt.Errorf("error getting column names -> %v", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return columnInfo, fmt.Errorf("error getting column types -> %v", err)
	}

	numCols := len(columnNames)

	for i := 0; i < numCols; i++ {
		precision, scale, decimalOk := columnTypes[i].DecimalSize()
		length, lengthOk := columnTypes[i].Length()
		nullable, nullableOk := columnTypes[i].Nullable()
		scanType := getScanType(columnTypes[i])

		columnInfo = append(columnInfo, ColumnInfo{
			name:         columnNames[i],
			databaseType: columnTypes[i].DatabaseTypeName(),
			scanType:     scanType,
			decimalOk:    decimalOk,
			precision:    precision,
			scale:        scale,
			lengthOk:     lengthOk,
			length:       length,
			nullableOk:   nullableOk,
			nullable:     nullable,
		})
	}

	return columnInfo, nil
}

func getScanType(columnType *sql.ColumnType) (scanType string) {
	defer func() {
		if r := recover(); r != nil {
			infoLog.Printf("panic in getScanType -> %v", r)
			scanType = ""
		}
	}()

	scanType = columnType.ScanType().String()
	return
}
