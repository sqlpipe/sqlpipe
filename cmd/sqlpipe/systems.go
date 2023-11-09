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
	"unicode"

	"golang.org/x/sync/errgroup"
)

type System interface {
	// The system interface abstracts differences between different database systems

	// *********************
	// ** database basics **
	// *********************

	query(query string) (rows *sql.Rows, err error)
	queryRow(query string) (row *sql.Row)
	exec(query string) (err error)

	closeConnectionPool(printError bool) (err error)

	// getNowSyntax() string
	getSystemName() string
	schemaRequired() bool
	isReservedKeyword(word string) (isReserved bool)
	escape(objectName string) (escaped string)
	getPrimaryKeysRows(schema, table string) (rows *sql.Rows, err error)
	getTableColumnInfosRows(schema, table string) (rows *sql.Rows, err error)
	IsTableNotFoundError(err error) (isTableNotFound bool)

	// -----------------
	// -- translators --
	// -----------------

	dbTypeToPipeType(databaseTypeName string) (pipeType string, err error)
	driverTypeToPipeType(columnType *sql.ColumnType, databaseTypeName string) (pipeType string, err error)
	pipeTypeToCreateType(columnInfo ColumnInfo) (createType string, err error)

	getPipeFileFormatters() (pipeFileFormatters map[string]func(interface{}) (pipeFileValue string, err error))
	getSqlFormatters() (sqlFormatters map[string]func(string) (sqlValue string, err error))
	getFinalCsvFormatters() (finalCsvFormatters map[string]func(string) (finalCsvValue string, err error))

	// -------------------
	// -- DDL overrides --
	// -------------------

	createSchemaIfNotExistsOverride(schema string) (overridden bool, err error)
	createTableIfNotExistsOverride(schema, table string, columnInfo []ColumnInfo, incremental bool) (overridden bool, err error)
	dropTableIfExistsOverride(schema, table string) (overridden bool, err error)

	// *******************
	// ** Data movement **
	// *******************

	createPipeFilesOverride(pipeFileInfoChannel chan PipeFileInfo, columnInfo []ColumnInfo, transfer Transfer, rows *sql.Rows) (pipeFileChannel chan PipeFileInfo, overridden bool)
	convertPipeFilesOverride(pipeFileInfoChannel <-chan PipeFileInfo, finalCsvInfoChannel chan FinalCsvInfo, transfer Transfer, columnInfo []ColumnInfo) (finalCsvChannel chan FinalCsvInfo, overridden bool)
	insertPipeFilesOverride(columnInfo []ColumnInfo, transfer Transfer, pipeFileInfoChannel <-chan PipeFileInfo, vacuumTable string) (overridden bool, err error)
	insertFinalCsvsOverride(transfer Transfer) (overridden bool, err error)
	runInsertCmd(finalCsvInfo FinalCsvInfo, transfer Transfer, schema, table string) (err error)
	getIncrementalTimeOverride(schema, table, incrementalColumn string, intialLoad bool) (incrementalTime time.Time, overridden bool, initialLoad bool, err error)
}

func newSystem(connectionInfo ConnectionInfo) (system System, err error) {
	// creates a new system

	switch connectionInfo.Type {
	case TypePostgreSQL:
		return newPostgresql(connectionInfo)
	case TypeMSSQL:
		return newMssql(connectionInfo)
	case TypeMySQL:
		return newMysql(connectionInfo)
	case TypeOracle:
		return newOracle(connectionInfo)
	case TypeSnowflake:
		return newSnowflake(connectionInfo)
	default:
		return system, fmt.Errorf("unsupported system type %v", connectionInfo.Type)
	}
}

func openConnectionPool(name, connectionString, driverName string) (connectionPool *sql.DB, err error) {

	connectionPool, err = sql.Open(driverName, connectionString)
	if err != nil {
		return nil, fmt.Errorf("error opening connection to %v :: %v", name, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = connectionPool.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("error pinging %v :: %v", name, err)
	}

	return connectionPool, nil
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

func getQueryColumnInfos(rows *sql.Rows, source System) (columnInfo []ColumnInfo, err error) {
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

		pipeType, err := source.driverTypeToPipeType(columnTypes[i], dbTypeName)
		if err != nil {
			return columnInfo, fmt.Errorf("error getting pipeTypes :: %v", err)
		}

		scanType, err := safeGetScanType(columnTypes[i])
		if err != nil {
			warningLog.Printf("error getting scantype for column %v :: %v",
				columnNames[i], err)
		}

		columnInfo = append(columnInfo, ColumnInfo{
			Name:       columnNames[i],
			PipeType:   pipeType,
			ScanType:   scanType,
			DecimalOk:  decimalOk,
			Precision:  precision,
			Scale:      scale,
			LengthOk:   lengthOk,
			Length:     length,
			NullableOk: nullableOk,
			Nullable:   nullable,
		})
	}

	return columnInfo, nil
}

func createTableIfNotExists(
	schema, table string,
	columnInfos []ColumnInfo,
	target System,
	incremental bool,
) (
	err error,
) {

	overridden, err := target.createTableIfNotExistsOverride(schema, table, columnInfos, incremental)
	if overridden {
		return err
	}

	schemaPeriodTable := getSchemaPeriodTable(schema, table, target, true)

	escapedPrimaryKeys := []string{}

	for i := range columnInfos {
		if columnInfos[i].IsPrimaryKey {
			escapedPrimaryKeys = append(escapedPrimaryKeys, escapeIfNeeded(columnInfos[i].Name, target))
		}
	}

	var queryBuilder = strings.Builder{}

	queryBuilder.WriteString("create table if not exists ")
	queryBuilder.WriteString(schemaPeriodTable)
	queryBuilder.WriteString(" (")

	for i := range columnInfos {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}

		escapedName := escapeIfNeeded(columnInfos[i].Name, target)

		queryBuilder.WriteString(escapedName)
		queryBuilder.WriteString(" ")

		createType, err := target.pipeTypeToCreateType(columnInfos[i])
		if err != nil {
			return fmt.Errorf("error getting create type for column %v :: %v", columnInfos[i].Name, err)
		}

		queryBuilder.WriteString(createType)
	}

	if incremental && len(escapedPrimaryKeys) > 0 {
		queryBuilder.WriteString(", primary key (")
		queryBuilder.WriteString(strings.Join(escapedPrimaryKeys, ","))
		queryBuilder.WriteString(")")
	}

	queryBuilder.WriteString(")")

	err = target.exec(queryBuilder.String())
	if err != nil {
		return fmt.Errorf("error running create table %v :: %v", schemaPeriodTable, err)
	}

	infoLog.Printf("created table %v if not exists in %v", schemaPeriodTable, target.getSystemName())

	return nil
}

// func createTempTable(
// 	schema, table string,
// 	columnInfos []ColumnInfo,
// 	system System,
// 	transfer Transfer,
// ) (
// 	conn *sql.Conn,
// 	err error,
// ) {

// 	// overridden, err := system.createTempTableOverride(schema, table, columnInfos)
// 	// if overridden {
// 	// 	return conn, err
// 	// }

// 	tempTable := fmt.Sprintf("%v_%v", table, transfer.Id)

// 	schemaPeriodTable := getSchemaPeriodTable(schema, tempTable, system, true)

// 	var queryBuilder = strings.Builder{}

// 	queryBuilder.WriteString("create temp table ")
// 	queryBuilder.WriteString(schemaPeriodTable)
// 	queryBuilder.WriteString(" (")

// 	for i := range columnInfos {
// 		if i > 0 {
// 			queryBuilder.WriteString(", ")
// 		}

// 		escapedName := escapeIfNeeded(columnInfos[i].Name, system)

// 		queryBuilder.WriteString(escapedName)
// 		queryBuilder.WriteString(" ")

// 		createType, err := system.pipeTypeToCreateType(columnInfos[i])
// 		if err != nil {
// 			return conn, fmt.Errorf("error getting create type for column %v :: %v", columnInfos[i].Name, err)
// 		}

// 		queryBuilder.WriteString(createType)
// 	}

// 	queryBuilder.WriteString(")")

// 	conn, err = system.getConn()
// 	if err != nil {
// 		return conn, fmt.Errorf("error getting connection :: %v", err)
// 	}

// 	_, err = conn.ExecContext(transfer.Context, queryBuilder.String())
// 	if err != nil {
// 		conn.Close()
// 		return nil, fmt.Errorf("error running create table %v :: %v", schemaPeriodTable, err)
// 	}

// 	infoLog.Printf("created temp table %v in %v", schemaPeriodTable, system.getSystemName())

// 	return conn, nil
// }

type PipeFileInfo struct {
	FilePath   string
	PkFilePath string
}

func createPipeFiles(
	columnInfos []ColumnInfo,
	transfer Transfer,
	rows *sql.Rows,
	source System,
	target System,
	incremental bool,
) <-chan PipeFileInfo {

	pipeFileInfoChannel := make(chan PipeFileInfo)

	pipeFileInfoChannel, overridden := source.createPipeFilesOverride(pipeFileInfoChannel, columnInfos, transfer, rows)
	if overridden {
		return pipeFileInfoChannel
	}

	go func() {

		defer close(pipeFileInfoChannel)

		pipeFileFormatters := source.getPipeFileFormatters()

		pipeFileNum := 0

		pipeFile, err := os.Create(
			filepath.Join(transfer.PipeFileDir, fmt.Sprintf("%032b.pipe", pipeFileNum)))
		if err != nil {
			transfer.Error = fmt.Sprintf("error creating temp file :: %v", err)
			transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
			errorLog.Println(transfer.Error)
			return
		}
		defer pipeFile.Close()

		var numPks int
		var pkWriter *csv.Writer
		var pkFile *os.File

		if incremental {
			pkFile, err = os.Create(
				filepath.Join(transfer.PipeFileDir, fmt.Sprintf("%032bpk.pipe", pipeFileNum)))
			if err != nil {
				transfer.Error = fmt.Sprintf("error creating temp file :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}
			defer pkFile.Close()
			pkWriter = csv.NewWriter(pkFile)

			for i := range columnInfos {
				if columnInfos[i].IsPrimaryKey {
					numPks++
				}
			}
		}

		numCols := len(columnInfos)

		csvWriter := csv.NewWriter(pipeFile)
		csvLength := 0

		values := make([]interface{}, numCols)
		valuePtrs := make([]interface{}, numCols)
		for i := 0; i < numCols; i++ {
			valuePtrs[i] = &values[i]
		}

		dataInRam := false
		csvRow := make([]string, numCols)
		pkRow := make([]string, numPks)

		eg := errgroup.Group{}

		for rows.Next() {

			err := rows.Scan(valuePtrs...)
			if err != nil {
				transfer.Error = fmt.Sprintf("error scanning row :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			eg.Go(func() error {
				for i := 0; i < numCols; i++ {
					if values[i] == nil {
						csvRow[i] = transfer.Null
						csvLength += len(transfer.Null)
					} else {
						csvRow[i], err = pipeFileFormatters[columnInfos[i].PipeType](values[i])
						if err != nil {
							err = fmt.Errorf("error formatting pipe file :: %v", err)
							transfer.Error = err.Error()
							transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
							errorLog.Println(transfer.Error)
							return err
						}
						csvLength += len(csvRow[i])
					}
				}
				return nil
			})

			eg.Go(func() error {
				if incremental {
					j := 0
					for i := 0; i < numCols; i++ {
						if columnInfos[i].IsPrimaryKey {
							if values[i] == nil {
								pkRow[j] = transfer.Null
							} else {
								pkRow[j], err = pipeFileFormatters[columnInfos[i].PipeType](values[i])
								if err != nil {
									err = fmt.Errorf("error formatting pipe file :: %v", err)
									transfer.Error = err.Error()
									transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
									errorLog.Println(transfer.Error)
									return err
								}
							}
							j++
						}
					}
				}
				return nil
			})

			err = eg.Wait()
			if err != nil {
				transfer.Error = fmt.Sprintf("error formatting pipe file :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			eg.Go(func() error {
				err = csvWriter.Write(csvRow)
				if err != nil {
					err = fmt.Errorf("error writing csv row :: %v", err)
					transfer.Error = err.Error()
					transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
					errorLog.Println(transfer.Error)
					return err
				}
				return nil
			})

			eg.Go(func() error {
				if incremental {
					err = pkWriter.Write(pkRow)
					if err != nil {
						err = fmt.Errorf("error writing pk row :: %v", err)
						transfer.Error = err.Error()
						transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
						errorLog.Println(transfer.Error)
						return err
					}
				}
				return nil
			})

			err = eg.Wait()
			if err != nil {
				transfer.Error = fmt.Sprintf("error writing pipe file :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			dataInRam = true

			if csvLength > 10_000_000 {

				eg.Go(func() error {
					csvWriter.Flush()

					err = pipeFile.Close()
					if err != nil {
						err = fmt.Errorf("error closing pipe file :: %v", err)
						transfer.Error = err.Error()
						transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
						errorLog.Println(transfer.Error)
						return err
					}
					return nil
				})

				eg.Go(func() error {
					if incremental {
						pkWriter.Flush()

						err = pkFile.Close()
						if err != nil {
							err = fmt.Errorf("error closing pk file :: %v", err)
							transfer.Error = err.Error()
							transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
							errorLog.Println(transfer.Error)
							return err
						}
					}
					return nil
				})

				err = eg.Wait()
				if err != nil {
					transfer.Error = fmt.Sprintf("error writing pipe file :: %v", err)
					transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
					errorLog.Println(transfer.Error)
					return
				}

				select {
				case <-transfer.Context.Done():
					return
				default:
				}

				pkFilePath := ""

				if incremental {
					pkFilePath = pkFile.Name()
				}

				pipeFileInfo := PipeFileInfo{
					FilePath:   pipeFile.Name(),
					PkFilePath: pkFilePath,
				}

				pipeFileInfoChannel <- pipeFileInfo

				pipeFileNum++

				eg.Go(func() error {
					pipeFileName := filepath.Join(
						transfer.PipeFileDir, fmt.Sprintf("%032b.pipe", pipeFileNum))

					pipeFile, err = os.Create(pipeFileName)
					if err != nil {
						err = fmt.Errorf("error creating temp file :: %v", err)
						transfer.Error = err.Error()
						transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
						errorLog.Println(transfer.Error)
						return err
					}
					csvWriter = csv.NewWriter(pipeFile)

					return nil
				})
				defer pipeFile.Close()
				dataInRam = false
				csvLength = 0

				eg.Go(func() error {
					if incremental {
						pkFileName := filepath.Join(
							transfer.PipeFileDir, fmt.Sprintf("%032bpk.pipe", pipeFileNum))

						pkFile, err = os.Create(pkFileName)
						if err != nil {
							err = fmt.Errorf("error creating temp file :: %v", err)
							transfer.Error = err.Error()
							transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
							errorLog.Println(transfer.Error)
							return err
						}

						pkWriter = csv.NewWriter(pkFile)
					}

					return nil
				})
				defer pkFile.Close()

				err = eg.Wait()
				if err != nil {
					transfer.Error = fmt.Sprintf("error writing pipe file :: %v", err)
					transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
					errorLog.Println(transfer.Error)
					return
				}

			}
		}

		if err := rows.Err(); err != nil {
			transfer.Error = fmt.Sprintf("error iterating rows :: %v", err)
			transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
			return
		}

		if dataInRam {

			eg.Go(func() error {
				csvWriter.Flush()

				err = pipeFile.Close()
				if err != nil {
					err = fmt.Errorf("error closing pipe file :: %v", err)
					transfer.Error = err.Error()
					transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
					errorLog.Println(transfer.Error)
					return err
				}
				return nil
			})

			eg.Go(func() error {
				if incremental {
					pkWriter.Flush()

					err = pkFile.Close()
					if err != nil {
						err = fmt.Errorf("error closing pk file :: %v", err)
						transfer.Error = err.Error()
						transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
						errorLog.Println(transfer.Error)
						return err
					}
				}
				return nil
			})

			err = eg.Wait()
			if err != nil {
				transfer.Error = fmt.Sprintf("error writing pipe file :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			pkFilePath := ""

			if incremental {
				pkFilePath = pkFile.Name()
			}

			pipeFileInfo := PipeFileInfo{
				FilePath:   pipeFile.Name(),
				PkFilePath: pkFilePath,
			}

			pipeFileInfoChannel <- pipeFileInfo
		}

		infoLog.Printf("transfer %v finished writing pipe files", transfer.Id)
	}()

	return pipeFileInfoChannel
}

func insertPipeFiles(pipeFileChannel <-chan PipeFileInfo, transfer Transfer, columnInfos []ColumnInfo, target System, vacuumTable string) (err error) {

	overridden, err := target.insertPipeFilesOverride(columnInfos, transfer, pipeFileChannel, vacuumTable)
	if overridden {
		return err
	}

	finalCsvChannel := convertPipeFiles(pipeFileChannel, columnInfos, transfer, target)

	table := transfer.TargetTable

	err = insertFinalCsvs(finalCsvChannel, transfer, target, transfer.TargetSchema, table)
	if err != nil {
		return fmt.Errorf("error inserting final csvs :: %v", err)
	}

	return nil
}

func convertPipeFiles(
	pipeFileInfoChannel <-chan PipeFileInfo,
	columnInfos []ColumnInfo,
	transfer Transfer,
	target System,
) <-chan FinalCsvInfo {

	finalCsvInfoChannel := make(chan FinalCsvInfo)

	finalCsvInfoChannel, overridden := target.convertPipeFilesOverride(pipeFileInfoChannel, finalCsvInfoChannel, transfer, columnInfos)
	if overridden {
		return finalCsvInfoChannel
	}

	finalCsvFormatters := target.getFinalCsvFormatters()

	go func() {

		defer close(finalCsvInfoChannel)

		for pipeFileInfo := range pipeFileInfoChannel {

			pkFilePath := pipeFileInfo.PkFilePath
			defer os.Remove(pkFilePath)

			pipeFilePath := pipeFileInfo.FilePath

			pipeFile, err := os.Open(pipeFilePath)
			if err != nil {
				transfer.Error = fmt.Sprintf("error opening pipeFile :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			fileNum, err := getFileNum(pipeFilePath)
			if err != nil {
				transfer.Error = fmt.Sprintf("error getting fileNum :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			csvFileName := filepath.Join(transfer.FinalCsvDir, fmt.Sprintf("%032v.csv", fileNum))
			csvFile, err := os.Create(csvFileName)
			if err != nil {
				transfer.Error = fmt.Sprintf("error creating csv file :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			csvReader := csv.NewReader(pipeFile)
			csvWriter := csv.NewWriter(csvFile)

			for {
				row, err := csvReader.Read()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					transfer.Error = fmt.Sprintf("error reading csv row :: %v", err)
					transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
					errorLog.Println(transfer.Error)
					return
				}

				for i := range row {
					if row[i] != transfer.Null {
						row[i], err = finalCsvFormatters[columnInfos[i].PipeType](row[i])
						if err != nil {
							transfer.Error = fmt.Sprintf("error formatting final csv :: %v", err)
							transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
							errorLog.Println(transfer.Error)
							return
						}
					}
				}

				err = csvWriter.Write(row)
				if err != nil {
					transfer.Error = fmt.Sprintf("error writing csv row :: %v", err)
					transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
					errorLog.Println(transfer.Error)
					return
				}
			}

			err = pipeFile.Close()
			if err != nil {
				transfer.Error = fmt.Sprintf("error closing pipeFile :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			csvWriter.Flush()

			err = csvFile.Close()
			if err != nil {
				transfer.Error = fmt.Sprintf("error closing csvFile :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			select {
			case <-transfer.Context.Done():
				return
			default:
			}

			finalCsvInfo := FinalCsvInfo{
				FilePath:   csvFile.Name(),
				InsertInfo: csvFile.Name(),
			}

			finalCsvInfoChannel <- finalCsvInfo

			if !transfer.KeepFiles {
				err = os.Remove(pipeFilePath)
				if err != nil {
					transfer.Error = fmt.Sprintf("error removing pipeFile :: %v", err)
					transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
					return
				}
			}
		}

		infoLog.Printf("transfer %v finished converting pipe files to final csvs", transfer.Id)
	}()

	return finalCsvInfoChannel
}

func insertFinalCsvs(
	finalCsvChannel <-chan FinalCsvInfo,
	transfer Transfer,
	target System,
	schema, table string,
) (
	err error,
) {
	// inserts final csvs into the target system

	for finalCsvinfo := range finalCsvChannel {

		select {
		case <-transfer.Context.Done():
			return errors.New("context cancelled")
		default:
		}

		err = target.runInsertCmd(finalCsvinfo, transfer, schema, table)
		if err != nil {
			return fmt.Errorf("error inserting final csv :: %v", err)
		}

		if !transfer.KeepFiles {
			err = os.Remove(finalCsvinfo.FilePath)
			if err != nil {
				return fmt.Errorf("error removing final csv :: %v", err)
			}
		}
	}

	infoLog.Printf("transfer %v finished inserting final csvs", transfer.Id)

	return nil
}

func getSchemaPeriodTable(schema, table string, system System, escapeIfNeededIn bool) (schemaPeriodTable string) {

	if escapeIfNeededIn {
		schema = escapeIfNeeded(schema, system)
		table = escapeIfNeeded(table, system)
	}

	if system.schemaRequired() {
		return fmt.Sprintf("%v.%v", schema, table)
	}

	return table
}

// func getSchemaUnderscoreTable(schema, table string, system System, escapeIfNeededIn bool) (schemaUnderscoreTable string) {

// 	schemaUnderscoreTable = table

// 	if system.schemaRequired() {
// 		schemaUnderscoreTable = fmt.Sprintf("%v_%v", schema, table)
// 	}

// 	if escapeIfNeededIn {
// 		schemaUnderscoreTable = escapeIfNeeded(schemaUnderscoreTable, system)
// 	}

// 	return schemaUnderscoreTable
// }

func needsEscaping(objectName string, system System) (needsEscaping bool) {

	if objectName == "" {
		return false
	}

	if system.isReservedKeyword(objectName) {
		return true
	}

	if containsSpaces(objectName) {
		return true
	}

	firstRune := rune(objectName[0])
	if !(unicode.IsLetter(firstRune) || firstRune == '_' || firstRune == '@' || firstRune == '#') {
		return true
	}

	for _, char := range objectName[1:] {
		if !(unicode.IsLetter(char) || unicode.IsDigit(char) || char == '_') {
			return true
		}
	}

	return false
}

func escapeIfNeeded(objectName string, system System) (objectNameOut string) {
	if needsEscaping(objectName, system) {
		return system.escape(objectName)
	}
	return objectName
}

func createSchemaIfNotExists(schema string, system System) (err error) {
	overridden, err := system.createSchemaIfNotExistsOverride(schema)
	if overridden {
		infoLog.Printf("schema %v created if not exists", schema)
		return err
	}

	query := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %v`, escapeIfNeeded(schema, system))

	err = system.exec(query)
	if err != nil {
		return fmt.Errorf("error creating schema %v :: %v", schema, err)
	}

	infoLog.Printf("schema %v created if not exists in %v", schema, system.getSystemName())

	return nil
}

func dropTableIfExists(schema, table string, system System) (err error) {
	overridden, err := system.dropTableIfExistsOverride(schema, table)
	if overridden {
		return err
	}

	escapedSchemaPeriodTable := getSchemaPeriodTable(schema, table, system, true)

	query := fmt.Sprintf("drop table if exists %v", escapedSchemaPeriodTable)
	err = system.exec(query)
	if err != nil {
		return fmt.Errorf("error dropping table %v :: %v", escapedSchemaPeriodTable, err)
	}

	infoLog.Printf("dropped %v if exists in %v", escapedSchemaPeriodTable, system.getSystemName())

	return nil
}

func deletePks(pipeFilesIn <-chan PipeFileInfo, columnInfos []ColumnInfo, transfer Transfer, target System, incremental, initialLoad bool) <-chan PipeFileInfo {
	if initialLoad || !incremental {
		return pipeFilesIn
	}

	pipeFilesOut := make(chan PipeFileInfo)

	sqlFormatters := target.getSqlFormatters()

	go func() {

		defer close(pipeFilesOut)

		for pipeFileInfo := range pipeFilesIn {

			select {
			case <-transfer.Context.Done():
				return
			default:
			}

			pkColumnInfos := []ColumnInfo{}

			for i := range columnInfos {
				if columnInfos[i].IsPrimaryKey {
					pkColumnInfos = append(pkColumnInfos, columnInfos[i])
				}
			}

			escapedSchemaPeriodTable := getSchemaPeriodTable(transfer.TargetSchema, transfer.TargetTable, target, true)

			queryBuilder := strings.Builder{}

			pkPipeFile, err := os.Open(pipeFileInfo.PkFilePath)
			if err != nil {
				transfer.Error = fmt.Sprintf("error opening pipeFile :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			csvReader := csv.NewReader(pkPipeFile)

			queryBuilder.WriteString("delete from ")
			queryBuilder.WriteString(escapedSchemaPeriodTable)
			queryBuilder.WriteString(" where (")
			for i := range pkColumnInfos {
				if i > 0 {
					queryBuilder.WriteString(",")
				}
				queryBuilder.WriteString(escapeIfNeeded(pkColumnInfos[i].Name, target))
			}
			queryBuilder.WriteString(") in (")

			var rowNum int64 = 0

			for {
				row, err := csvReader.Read()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					transfer.Error = fmt.Sprintf("error reading csv row :: %v", err)
					transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
					errorLog.Println(transfer.Error)
					return
				}

				if rowNum != 0 {
					queryBuilder.WriteString(",")
				}

				queryBuilder.WriteString("(")

				for colNum := range row {

					if colNum > 0 {
						queryBuilder.WriteString(",")
					}

					if row[colNum] == transfer.Null {
						queryBuilder.WriteString("null")
					} else {
						stringVal, err := sqlFormatters[pkColumnInfos[colNum].PipeType](row[colNum])
						if err != nil {
							transfer.Error = fmt.Sprintf("error formatting sql value :: %v", err)
							transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
							errorLog.Println(transfer.Error)
							return
						}
						queryBuilder.WriteString(stringVal)
					}

				}
				queryBuilder.WriteString(")")

				rowNum++

				if rowNum > 5 {
					queryBuilder.WriteString(")")

					err = target.exec(queryBuilder.String())
					if err != nil {
						transfer.Error = fmt.Sprintf("error deleting pks in middle :: %v", err)
						transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
						errorLog.Println(transfer.Error)
						return
					}

					queryBuilder.Reset()

					queryBuilder.WriteString("delete from ")
					queryBuilder.WriteString(escapedSchemaPeriodTable)
					queryBuilder.WriteString(" where (")
					for pkColNum := range pkColumnInfos {
						if pkColNum > 0 {
							queryBuilder.WriteString(",")
						}
						queryBuilder.WriteString(escapeIfNeeded(pkColumnInfos[pkColNum].Name, target))
					}

					queryBuilder.WriteString(") in (")

					rowNum = 0
				}
			}

			queryBuilder.WriteString(")")

			err = pkPipeFile.Close()
			if err != nil {
				transfer.Error = fmt.Sprintf("error closing pipeFile :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			// if we exited on the first row, there was nothing to delete
			if rowNum != 0 {

				err = target.exec(queryBuilder.String())
				if err != nil {
					transfer.Error = fmt.Sprintf("error deleting pks at end :: %v", err)
					transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
					errorLog.Println(transfer.Error)
					return
				}
			}

			pipeFilesOut <- pipeFileInfo

		}
		infoLog.Printf("transfer %v finished deleting pks", transfer.Id)

	}()

	return pipeFilesOut
}

func getTableColumnInfos(schema, table string, system System) (columnInfos []ColumnInfo, err error) {

	rows, err := system.getTableColumnInfosRows(schema, table)
	if err != nil {
		return nil, fmt.Errorf("error getting table column infos rows :: %v", err)
	}

	columnInfos = []ColumnInfo{}

	var columnName string
	var columnType string
	var columnPrecision int64
	var columnScale int64
	var columnLength int64
	var columnIsPrimary bool

	for rows.Next() {
		err := rows.Scan(&columnName, &columnType, &columnPrecision, &columnScale, &columnLength, &columnIsPrimary)
		if err != nil {
			return nil, fmt.Errorf("error scanning table column infos rows :: %v", err)
		}

		pipeType, err := system.dbTypeToPipeType(columnType)
		if err != nil {
			return nil, fmt.Errorf("error getting pipe type for column %v :: %v", columnName, err)
		}

		decimalOk := false
		if columnPrecision > 0 || columnScale > 0 {
			decimalOk = true
		}

		lengthOk := false
		if columnLength > 0 {
			lengthOk = true
		}

		columnInfo := ColumnInfo{
			Name:         columnName,
			PipeType:     pipeType,
			DecimalOk:    decimalOk,
			Precision:    columnPrecision,
			Scale:        columnScale,
			LengthOk:     lengthOk,
			Length:       columnLength,
			IsPrimaryKey: columnIsPrimary,
		}

		columnInfos = append(columnInfos, columnInfo)
	}

	if len(columnInfos) == 0 {
		return nil, fmt.Errorf("no columns found for table %v.%v", schema, table)
	}

	return columnInfos, nil
}
