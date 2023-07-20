package main

import (
	"database/sql"
	"fmt"
)

func runTransfer(transfer Transfer) {
	sourceSystem, err := newSystem(transfer.SourceType, transfer.SourceConnectionString)
	if err != nil {
		transferError(transfer, fmt.Errorf("error creating source system -> %v", err))
		return
	}

	targetSystem, err := newSystem(transfer.TargetType, transfer.TargetConnectionString)
	if err != nil {
		transferError(transfer, fmt.Errorf("error creating target system -> %v", err))
		return
	}

	if transfer.DropTargetTable {
		err = targetSystem.dropTable(transfer.TargetSchema, transfer.TargetTable)
		if err != nil {
			transferError(transfer, fmt.Errorf("error dropping target table -> %v", err))
			return
		}
	}

	rows, err := sourceSystem.connection.Query(transfer.Query)
	if err != nil {
		transferError(transfer, fmt.Errorf("error querying source -> %v", err))
		return
	}

	columnInfo, err := getColumnInfo(rows)
	if err != nil {
		transferError(transfer, fmt.Errorf("error getting source column info -> %v", err))
		return
	}

	if transfer.CreateTargetTable {
		err = targetSystem.createTable(transfer.TargetSchema, transfer.TargetTable, columnInfo)
	}

	// values := make([]interface{}, len(columns))
	// valuePtrs := make([]interface{}, len(columns))
	// for i := range columns {
	// 	valuePtrs[i] = &values[i]
	// }
}

type ColumnInfo struct {
	names     []string
	types     []string
	decimalOk []bool
	precision []int64
	scale     []int64
	lengthOk  []bool
	length    []int64
}

var intermediateTypes = map[string]map[string]string{
	"postgresql": postgresqlIntermediateTypes,
	// "mysql":      mysqlIntermediateTypes,
	// "mssql":      mssqlIntermediateTypes,
	// "oracle":     oracleIntermediateTypes,
	// "snowflake":  snowflakeIntermediateTypes,
}

func getColumnInfo(rows *sql.Rows) (ColumnInfo, error) {
	columnInfo := ColumnInfo{}

	columns, err := rows.Columns()
	if err != nil {
		return columnInfo, fmt.Errorf("error getting column names -> %v", err)
	}

	columnInfo = ColumnInfo{
		names:     make([]string, len(columns)),
		types:     make([]string, len(columns)),
		decimalOk: make([]bool, len(columns)),
		precision: make([]int64, len(columns)),
		scale:     make([]int64, len(columns)),
		lengthOk:  make([]bool, len(columns)),
		length:    make([]int64, len(columns)),
	}

	for i := range columns {
		columnInfo.names[i] = columns[i]
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return columnInfo, fmt.Errorf("error getting column types -> %v", err)
	}

	for i, column := range columnTypes {
		columnInfo.types[i] = column.DatabaseTypeName()
		precision, scale, ok := column.DecimalSize()
		if ok {
			columnInfo.decimalOk[i] = true
			columnInfo.precision[i] = precision
			columnInfo.scale[i] = scale
		}
		length, ok := column.Length()
		if ok {
			columnInfo.lengthOk[i] = true
			columnInfo.length[i] = length
		}
	}

	return columnInfo, nil
}
