package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

type System interface {
	dropTable(schema, table string) (err error)
	createTable(transfer *Transfer) error
	query(query string) (*sql.Rows, error)
	exec(query string) (err error)
	getColumnInfo(transfer *Transfer) ([]ColumnInfo, error)
	getPipeFileFormatters() (map[string]func(interface{}) (string, error), error)
	dbTypeToPipeType(databaseTypeName string, columnType sql.ColumnType, transfer *Transfer) (pipeType string, err error)
	pipeTypeToCreateType(columnInfo ColumnInfo) (createType string, err error)
	createPipeFiles(transfer *Transfer, transferErrGroup *errgroup.Group) (out <-chan string, err error)
	insertPipeFiles(transfer *Transfer, in <-chan string, transferErrGroup *errgroup.Group) error
}

func newSystem(name, systemType, connectionString string, timezone string) (System, error) {
	switch systemType {
	case "postgresql":
		return newPostgresql(name, connectionString)
	case "mssql":
		return newMssql(name, connectionString)
	case "mysql":
		return newMysql(name, connectionString, timezone)
	case "oracle":
		return newOracle(name, connectionString)
	default:
		return nil, fmt.Errorf("unsupported system type %v", systemType)
	}
}

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

	if schema != "" {
		schema = fmt.Sprintf("%v.", schema)
	}

	query := fmt.Sprintf("drop table if exists %v%v", schema, table)
	err := system.exec(query)
	if err != nil {
		err = fmt.Errorf("error dropping %v%v :: %v", schema, table, err)
	}

	infoLog.Printf("dropped %v%v", schema, table)

	return err
}

func getScanType(columnType *sql.ColumnType) (scanType string, err error) {
	defer func() {
		if r := recover(); r != nil {
			scanType = ""
			err = fmt.Errorf("panic caught while trying to get scantype for db type %v :: %v", columnType.DatabaseTypeName(), r)
		}
	}()

	scanType = columnType.ScanType().String()
	return scanType, err
}

func getColumnInfoCommon(transfer *Transfer) ([]ColumnInfo, error) {
	columnInfo := []ColumnInfo{}

	columnNames, err := transfer.Rows.Columns()
	if err != nil {
		return columnInfo, fmt.Errorf("error getting column names :: %v", err)
	}

	columnTypes, err := transfer.Rows.ColumnTypes()
	if err != nil {
		return columnInfo, fmt.Errorf("error getting column types :: %v", err)
	}

	numCols := len(columnNames)

	for i := 0; i < numCols; i++ {
		precision, scale, decimalOk := columnTypes[i].DecimalSize()
		length, lengthOk := columnTypes[i].Length()
		nullable, nullableOk := columnTypes[i].Nullable()

		scanType, err := getScanType(columnTypes[i])
		if err != nil {
			warningLog.Printf("error getting scantype for column %v :: %v", columnNames[i], err)
		}

		pipeType, err := transfer.Source.dbTypeToPipeType(columnTypes[i].DatabaseTypeName(), *columnTypes[i], transfer)
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

func createTableCommon(transfer *Transfer) error {
	createSchema := transfer.TargetSchema

	var queryBuilder = strings.Builder{}

	queryBuilder.WriteString("create table ")
	if createSchema != "" {
		createSchema = fmt.Sprintf("%v.", createSchema)
		queryBuilder.WriteString(createSchema)
	}
	queryBuilder.WriteString(transfer.TargetTable)
	queryBuilder.WriteString(" (")

	for i := range transfer.ColumnInfo {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		queryBuilder.WriteString(transfer.ColumnInfo[i].name)
		queryBuilder.WriteString(" ")
		createType, err := transfer.Target.pipeTypeToCreateType(transfer.ColumnInfo[i])
		if err != nil {
			return fmt.Errorf("error getting create type for column %v :: %v", transfer.ColumnInfo[i].name, err)
		}
		queryBuilder.WriteString(createType)
	}
	queryBuilder.WriteString(")")

	err := transfer.Target.exec(queryBuilder.String())
	if err != nil {
		return fmt.Errorf("error running create table %v.%v :: %v", createSchema, transfer.TargetTable, err)
	}

	infoLog.Printf("created table %v%v", createSchema, transfer.TargetTable)

	return nil
}

func createPipeFilesCommon(transfer *Transfer, transferErrGroup *errgroup.Group) (<-chan string, error) {
	var err error
	out := make(chan string)

	transferErrGroup.Go(func() error {

		defer close(out)

		pipeFilesDirPath := filepath.Join(transfer.TmpDir, "pipe-files")

		err = os.Mkdir(pipeFilesDirPath, os.ModePerm)
		if err != nil {
			return fmt.Errorf("unable to create temp dir for pipe files:: %v", err)
		}

		pipeFileFormatters, err := transfer.Source.getPipeFileFormatters()
		if err != nil {
			return fmt.Errorf("error getting pipe file formatters :: %v", err)
		}

		pipeFileNum := 1

		pipeFile, err := os.Create(filepath.Join(pipeFilesDirPath, fmt.Sprintf("%b.pipe", pipeFileNum)))
		if err != nil {
			return fmt.Errorf("error creating temp file :: %v", err)
		}
		defer pipeFile.Close()

		csvWriter := csv.NewWriter(pipeFile)
		csvRow := make([]string, len(transfer.ColumnInfo))
		csvLength := 0

		values := make([]interface{}, len(transfer.ColumnInfo))
		valuePtrs := make([]interface{}, len(transfer.ColumnInfo))
		for i := range transfer.ColumnInfo {
			valuePtrs[i] = &values[i]
		}

		dataInRam := false

		for transfer.Rows.Next() {
			err := transfer.Rows.Scan(valuePtrs...)
			if err != nil {
				return fmt.Errorf("error scanning row :: %v", err)
			}

			for j := range transfer.ColumnInfo {
				if values[j] == nil {
					csvRow[j] = transfer.Null
					csvLength += 5
				} else {
					stringVal, err := pipeFileFormatters[transfer.ColumnInfo[j].pipeType](values[j])
					if err != nil {
						return fmt.Errorf("error while formatting pipe type %v :: %v", transfer.ColumnInfo[j].pipeType, err)
					}
					csvRow[j] = stringVal
					csvLength += len(stringVal)
				}
			}

			err = csvWriter.Write(csvRow)
			if err != nil {
				return fmt.Errorf("error writing csv row :: %v", err)
			}
			dataInRam = true

			if csvLength > 10_000_000 {
				csvWriter.Flush()

				err = pipeFile.Close()
				if err != nil {
					return fmt.Errorf("error closing pipe file :: %v", err)
				}

				out <- pipeFile.Name()

				pipeFileNum++
				// create the file names in binary so it sorts correctly
				pipeFile, err = os.Create(filepath.Join(pipeFilesDirPath, fmt.Sprintf("%b.pipe", pipeFileNum)))
				if err != nil {
					return fmt.Errorf("error creating temp file :: %v", err)
				}
				defer pipeFile.Close()

				csvWriter = csv.NewWriter(pipeFile)
				dataInRam = false
				csvLength = 0
			}
		}

		if dataInRam {
			csvWriter.Flush()

			err = pipeFile.Close()
			if err != nil {
				return fmt.Errorf("error closing pipe file :: %v", err)
			}

			out <- pipeFile.Name()
		}

		infoLog.Printf("pipe files written at %v", pipeFilesDirPath)
		return nil
	})

	return out, nil
}
