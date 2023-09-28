package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type System interface {
	// The system interface abstracts differences between different database systems

	// *******************
	// ** sql execution **
	// *******************

	// runs a query and returns rows
	query(query string) (rows *sql.Rows, err error)

	// runs a query and returns nothing
	exec(query string) (err error)

	// *******************
	// ** initial setup **
	// *******************

	// drops a table if it exists
	dropTableIfExists(transfer Transfer) (dropped string, err error)

	// gets info about each column, including pipe type, length, precision, scale, etc
	getColumnInfo(rows *sql.Rows) (columnInfo []ColumnInfo, err error)

	// creates a table in the target system
	createTable(columnInfo []ColumnInfo, transfer Transfer) (created string, err error)

	// called within getColumnInfo to get db specific mapping from db types to pipe types
	dbTypeToPipeType(
		columnType *sql.ColumnType,
		databaseTypeName string,
	) (
		pipeType string,
		err error,
	)

	// called within createTable to convert pipe types into db specific create table types
	pipeTypeToCreateType(columnInfo ColumnInfo, transfer Transfer) (createType string, err error)

	// *******************
	// ** data movement **
	// *******************

	// creates pipe file and sends it to an out channel for consumption by insertPipeFiles
	createPipeFiles(
		ctx context.Context,
		errorChannel chan<- error,
		columnInfo []ColumnInfo,
		transfer Transfer,
		rows *sql.Rows,
	) <-chan string

	// called within createPipeFiles to write pipe file values from db specific formatters
	getPipeFileFormatters() (
		pipeFileFormatters map[string]func(interface{}) (pipeFileValue string, err error),
	)

	// inserts pipe files into target system
	insertPipeFiles(
		ctx context.Context,
		pipeFileChannel <-chan string,
		errorChannel chan<- error,
		columnInfo []ColumnInfo,
		transfer Transfer,
	) (
		err error,
	)

	// (optional) called within insertPipeFiles to convert pipe file values to final csv values
	convertPipeFiles(
		ctx context.Context,
		pipeFileChannel <-chan string,
		errorChannel chan<- error,
		columnInfo []ColumnInfo,
		transfer Transfer,
	) <-chan string

	// (optional) called within insertPipeFiles to reformat pipe file values to values
	// that db specific clients can understand in a csv
	getFinalCsvFormatters() (
		finalCsvFormatters map[string]func(string) (finalCsvValue string, err error),
	)

	// (optional) called within convertPipeFiles to format pipe file values to final csv values
	insertFinalCsvs(
		ctx context.Context,
		finalCsvChannel <-chan string,
		transfer Transfer,
	) (
		err error,
	)

	// (optional) called within insertFinalCsvs to run db specific insert commands
	runInsertCmd(
		ctx context.Context,
		finalCsvLocation string,
		transfer Transfer,
	) (
		err error,
	)
}

func newSystem(name, systemType, connectionString string) (system System, err error) {
	// creates a new system

	switch systemType {
	case TypeMSSQL:
		return newMssql(name, connectionString)
	case TypeMySQL:
		return newMysql(name, connectionString)
	case TypeOracle:
		return newOracle(name, connectionString)
	case TypePostgreSQL:
		return newPostgresql(name, connectionString)
	case TypeSnowflake:
		return newSnowflake(name, connectionString)
	default:
		return system, fmt.Errorf("unsupported system type %v", systemType)
	}
}

func openDbCommon(name, connectionString, driverName string) (db *sql.DB, err error) {
	// opens a connection to a a system and pings it

	db, err = sql.Open(driverName, connectionString)
	if err != nil {
		return nil, fmt.Errorf("error opening connection to %v :: %v", name, err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	err = db.PingContext(pingCtx)
	if err != nil {
		return nil, fmt.Errorf("error pinging %v :: %v", name, err)
	}

	return db, nil

}

func dropTableIfExistsCommon(transfer Transfer, system System) (dropped string, err error) {
	// drop a table if it exists

	schema := transfer.TargetSchema
	if schema != "" {
		schema = fmt.Sprintf("%v.", schema)
	}

	dropped = fmt.Sprintf("%v%v", schema, transfer.TargetTable)

	err = system.exec(fmt.Sprintf("drop table if exists %v", dropped))
	if err != nil {
		return "", fmt.Errorf("error dropping %v%v :: %v", schema, transfer.TargetTable,
			err)
	}

	return dropped, nil
}

func safeGetScanType(columnType *sql.ColumnType) (scanType string, err error) {
	// safely gets scan type of a column

	defer func() {
		if r := recover(); r != nil {
			scanType = ""
			err = fmt.Errorf("panic caught while trying getting %v scan type :: %v",
				columnType.DatabaseTypeName(), r)
		}
	}()

	scanType = columnType.ScanType().String()

	return scanType, err
}

func safeGetDbTypeName(columnType *sql.ColumnType) (dbTypeName string, err error) {
	// safely gets scan type of a column

	defer func() {
		if r := recover(); r != nil {
			dbTypeName = ""
			err = fmt.Errorf("panic caught while trying getting %v db type name :: %v",
				columnType.Name(), r)
		}
	}()

	return columnType.DatabaseTypeName(), err
}

type ColumnInfo struct {
	name       string
	pipeType   string
	scanType   string
	decimalOk  bool
	precision  int64
	scale      int64
	lengthOk   bool
	length     int64
	nullableOk bool
	nullable   bool
}

func getColumnInfoCommon(rows *sql.Rows, source System) (columnInfo []ColumnInfo, err error) {
	// gets / consolidates info about columns, including
	// pipe type, length, precision / scale, etc

	columnInfo = []ColumnInfo{}

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

		dbTypeName, err := safeGetDbTypeName(columnTypes[i])
		if err != nil {
			return columnInfo, fmt.Errorf("error getting dbTypeName :: %v", err)
		}

		pipeType, err := source.dbTypeToPipeType(columnTypes[i], dbTypeName)
		if err != nil {
			return columnInfo, fmt.Errorf("error getting pipeTypes :: %v", err)
		}

		scanType, err := safeGetScanType(columnTypes[i])
		if err != nil {
			warningLog.Printf("error getting scantype for column %v :: %v",
				columnNames[i], err)
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

func createTableCommon(
	columnInfo []ColumnInfo,
	transfer Transfer,
	target System,
) (
	created string,
	err error,
) {
	// uses columnInfo to create a table in the target system

	targetSchema := transfer.TargetSchema
	if targetSchema != "" {
		targetSchema = fmt.Sprintf("%v.", targetSchema)
	}

	created = fmt.Sprintf("%v%v", targetSchema, transfer.TargetTable)

	createSchema := targetSchema

	var queryBuilder = strings.Builder{}

	queryBuilder.WriteString("create table ")
	queryBuilder.WriteString(created)
	queryBuilder.WriteString(" (")

	for i := range columnInfo {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		queryBuilder.WriteString(columnInfo[i].name)
		queryBuilder.WriteString(" ")
		createType, err := target.pipeTypeToCreateType(columnInfo[i], transfer)
		if err != nil {
			err = fmt.Errorf("error getting create type for column %v :: %v",
				columnInfo[i].name, err)
			return "", err
		}
		queryBuilder.WriteString(createType)
	}
	queryBuilder.WriteString(")")

	err = target.exec(queryBuilder.String())
	if err != nil {
		err = fmt.Errorf("error running create table %v.%v :: %v",
			createSchema, transfer.TargetTable, err)
		return "", err
	}

	return created, nil
}

func createPipeFilesCommon(
	ctx context.Context,
	errorChannel chan<- error,
	columnInfo []ColumnInfo,
	transfer Transfer,
	rows *sql.Rows,
	source System,
) <-chan string {

	pipeFileChannel := make(chan string)

	go func() {

		defer close(pipeFileChannel)

		pipeFileFormatters := source.getPipeFileFormatters()

		pipeFileNum := 0

		pipeFile, err := os.Create(
			filepath.Join(transfer.PipeFileDir, fmt.Sprintf("%b.pipe", pipeFileNum)))
		if err != nil {
			errorChannel <- fmt.Errorf("error creating temp file :: %v", err)
			return
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

		for rows.Next() {
			err := rows.Scan(valuePtrs...)
			if err != nil {
				errorChannel <- fmt.Errorf("error scanning row :: %v", err)
				return
			}

			for j := range columnInfo {
				if values[j] == nil {
					csvRow[j] = transfer.Null
					csvLength += len(transfer.Null)
				} else {
					stringVal, err := pipeFileFormatters[columnInfo[j].pipeType](values[j])
					if err != nil {
						errorChannel <- fmt.Errorf(
							"error while formatting pipe type %v :: %v",
							columnInfo[j].pipeType, err,
						)
						return
					}
					csvRow[j] = stringVal
					csvLength += len(stringVal)
				}
			}

			err = csvWriter.Write(csvRow)
			if err != nil {
				errorChannel <- fmt.Errorf("error writing csv row :: %v", err)
				return
			}
			dataInRam = true

			if csvLength > 10_000_000 {

				csvWriter.Flush()

				err = pipeFile.Close()
				if err != nil {
					errorChannel <- fmt.Errorf("error closing pipe file :: %v", err)
					return
				}

				select {
				case <-ctx.Done():
					return
				default:
					pipeFileChannel <- pipeFile.Name()
				}

				pipeFileNum++
				pipeFileName := filepath.Join(
					transfer.PipeFileDir, fmt.Sprintf("%b.pipe", pipeFileNum))

				pipeFile, err = os.Create(pipeFileName)
				if err != nil {
					errorChannel <- fmt.Errorf("error creating temp file :: %v", err)
					return
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
				errorChannel <- fmt.Errorf("error closing pipe file :: %v", err)
				return
			}

			select {
			case <-ctx.Done():
				return
			default:
				pipeFileChannel <- pipeFile.Name()
			}

		}

		infoLog.Printf("transfer %v finished writing pipe files", transfer.Id)
	}()

	return pipeFileChannel
}

func convertPipeFilesCommon(
	ctx context.Context,
	pipeFileChannel <-chan string,
	errorChannel chan<- error,
	columnInfo []ColumnInfo,
	transfer Transfer,
	target System,
) <-chan string {

	finalCsvChannel := make(chan string)

	finalCsvFormatters := target.getFinalCsvFormatters()

	go func() {

		defer close(finalCsvChannel)

		for pipeFileName := range pipeFileChannel {
			pipeFile, err := os.Open(pipeFileName)
			if err != nil {
				errorChannel <- fmt.Errorf("error opening pipeFile :: %v", err)
				return
			}
			defer func() {
				pipeFile.Close()
				if !transfer.KeepFiles {
					os.Remove(pipeFileName)
				}
			}()

			fileNum, err := getFileNum(pipeFileName)
			if err != nil {
				errorChannel <- fmt.Errorf(
					"error getting file number :: %v", err)
				return
			}

			csvFileName := filepath.Join(transfer.FinalCsvDir, fmt.Sprintf(
				"%v.csv", fileNum))
			csvFile, err := os.Create(csvFileName)
			if err != nil {
				errorChannel <- fmt.Errorf(
					"error creating final csv file :: %v", err)
				return
			}
			defer csvFile.Close()

			csvReader := csv.NewReader(pipeFile)
			csvWriter := csv.NewWriter(csvFile)

			for {
				row, err := csvReader.Read()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					errorChannel <- fmt.Errorf(
						"error reading csv values in %v :: %v", pipeFile.Name(), err)
					return
				}

				for i := range row {
					if row[i] != transfer.Null {
						row[i], err = finalCsvFormatters[columnInfo[i].pipeType](row[i])
						if err != nil {
							errorChannel <- fmt.Errorf(
								"error formatting pipe file to final csv :: %v", err)
							return
						}
					}
				}

				err = csvWriter.Write(row)
				if err != nil {
					errorChannel <- fmt.Errorf(
						"error writing csv row :: %v", err)
					return
				}
			}

			err = pipeFile.Close()
			if err != nil {
				errorChannel <- fmt.Errorf("error closing pipeFile :: %v", err)
				return
			}

			csvWriter.Flush()

			err = csvFile.Close()
			if err != nil {
				errorChannel <- fmt.Errorf(
					"error closing final csv file :: %v", err)
				return
			}

			select {
			case <-ctx.Done():
				return
			default:
				finalCsvChannel <- csvFile.Name()
			}
		}
		infoLog.Printf("transfer %v finished converting pipe files to final csvs", transfer.Id)
	}()
	return finalCsvChannel
}

func insertFinalCsvsCommon(
	ctx context.Context,
	finalCsvChannel <-chan string,
	transfer Transfer,
	target System,
) (
	err error,
) {
	// inserts final csvs into the target system

	for finalCsvLocation := range finalCsvChannel {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		default:

			if !transfer.KeepFiles {
				defer os.Remove(finalCsvLocation)
			}

			err = target.runInsertCmd(ctx, finalCsvLocation, transfer)

			if err != nil {
				return fmt.Errorf("error inserting final csv :: %v", err)
			}
		}
	}

	infoLog.Printf("transfer %v finished inserting final csvs", transfer.Id)

	return nil
}
