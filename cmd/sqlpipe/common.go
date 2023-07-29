package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func openDbCommon(name, connectionString, driverName string) (db *sql.DB, err error) {
	db, err = sql.Open(driverName, connectionString)
	if err != nil {
		return nil, fmt.Errorf("error opening connection to %v :: %v", name, err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pingCancel()
	err = db.PingContext(pingCtx)
	if err != nil {
		return nil, fmt.Errorf("error pinging %v :: %v", name, err)
	}

	return db, nil
}

func dropTableIfExistsCommon(schema, table string, system System) error {
	query := fmt.Sprintf("drop table if exists %v.%v", schema, table)
	err := system.exec(query)
	if err != nil {
		err = fmt.Errorf("error dropping %v.%v :: %v", schema, table, err)
	}
	return err
}

func getColumnInfoCommon(rows *sql.Rows, system System) ([]ColumnInfo, error) {
	columnInfo := []ColumnInfo{}

	columnNames, err := rows.Columns()
	if err != nil {
		return columnInfo, fmt.Errorf("error getting column names :: %v", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return columnInfo, fmt.Errorf("error getting column types :: %v", err)
	}

	numCols := len(columnNames)

	for i := 0; i < numCols; i++ {
		precision, scale, decimalOk := columnTypes[i].DecimalSize()
		length, lengthOk := columnTypes[i].Length()
		nullable, nullableOk := columnTypes[i].Nullable()
		scanType := getScanType(columnTypes[i])

		pipeType, err := system.dbTypeToPipeType(columnTypes[i].DatabaseTypeName(), *columnTypes[i])
		if err != nil {
			return columnInfo, fmt.Errorf("error getting pipeTypes :: %v", err)
		}

		columnInfo = append(columnInfo, ColumnInfo{
			name:       columnNames[i],
			pipeType:   pipeType,
			scanType:   scanType,
			decimalOk:  decimalOk,
			precision:  precision,
			scale:      scale,
			lengthOk:   lengthOk,
			length:     length,
			nullableOk: nullableOk,
			nullable:   nullable,
		})
	}

	return columnInfo, nil
}

func createTableCommon(schema, table string, columnInfo []ColumnInfo, system System) error {
	var queryBuilder = strings.Builder{}

	queryBuilder.WriteString("create table ")
	if schema != "" {
		queryBuilder.WriteString(schema)
		queryBuilder.WriteString(".")
	}
	queryBuilder.WriteString(table)
	queryBuilder.WriteString(" (")

	for i := range columnInfo {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		queryBuilder.WriteString(columnInfo[i].name)
		queryBuilder.WriteString(" ")
		createType, err := system.pipeTypeToCreateType(columnInfo[i])
		if err != nil {
			return fmt.Errorf("error getting create type for column %v :: %v", columnInfo[i].name, err)
		}
		queryBuilder.WriteString(createType)
	}
	queryBuilder.WriteString(")")

	err := system.exec(queryBuilder.String())
	if err != nil {
		return fmt.Errorf("error running create table %v.%v :: %v", schema, table, err)
	}
	return nil
}

func createPipeFilesCommon(rows *sql.Rows, columnInfo []ColumnInfo, transferId string, system System) (transferDir string, err error) {
	tempDir := os.TempDir()
	transferDir = filepath.Join(tempDir, fmt.Sprintf("sqlpipe-transfer-%v", transferId))
	err = os.Mkdir(transferDir, os.ModePerm)
	if err != nil {
		return "", errors.New("error creating transfer dir")
	}

	pipeFilesDirPath := filepath.Join(transferDir, "pipe-files")

	err = os.Mkdir(pipeFilesDirPath, os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("unable to create temp dir for pipe files:: %v", err)
	}

	pipeFileFormatters := system.getPipeFileFormatters()

	pipeFile, err := os.CreateTemp(pipeFilesDirPath, "")
	if err != nil {
		return "", fmt.Errorf("error creating temp file :: %v", err)
	}
	defer pipeFile.Close()

	csvWriter := csv.NewWriter(pipeFile)
	csvRow := make([]string, len(columnInfo))
	csvLength := 0

	values := make([]interface{}, len(columnInfo))
	valuePtrs := make([]interface{}, len(columnInfo))
	for i := range columnInfo {
		valuePtrs[i] = &values[i]
	}

	dataInRam := false
	for i := 0; rows.Next(); i++ {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return "", fmt.Errorf("error scanning row :: %v", err)
		}

		for j := range columnInfo {
			if values[j] == nil {
				csvRow[j] = "<nil>"
				csvLength += 5
			} else {
				stringVal, err := pipeFileFormatters[columnInfo[j].pipeType](values[j])
				if err != nil {
					return "", fmt.Errorf("error while formatting pipe type %v :: %v", columnInfo[j].pipeType, err)
				}
				csvRow[j] = stringVal
				csvLength += len(stringVal)
			}
		}

		err = csvWriter.Write(csvRow)
		if err != nil {
			return "", fmt.Errorf("error writing csv row :: %v", err)
		}
		dataInRam = true

		if csvLength > 4096 {
			csvWriter.Flush()

			err = pipeFile.Close()
			if err != nil {
				return "", fmt.Errorf("error closing pipe file :: %v", err)
			}

			pipeFile, err = os.CreateTemp(pipeFilesDirPath, "")
			if err != nil {
				return "", fmt.Errorf("error creating temp file :: %v", err)
			}
			defer pipeFile.Close()

			csvWriter = csv.NewWriter(pipeFile)
			dataInRam = false
		}
	}

	if dataInRam {
		csvWriter.Flush()
	}

	infoLog.Printf("pipe file written at %v", pipeFilesDirPath)

	return transferDir, nil
}
