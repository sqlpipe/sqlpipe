package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"golang.org/x/sync/errgroup"
)

type ConnectionInfo struct {
	Type     string
	Hostname string
	Port     int
	Database string
	Username string
	Password string
}

type FinalCsvInfo struct {
	FilePath   string
	InsertInfo string
}

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
	pipeTypeToCreateType(columnInfo *data.ColumnInfo) (createType string, err error)

	getPipeFileFormatters() (pipeFileFormatters map[string]func(interface{}) (pipeFileValue string, err error))
	getSqlFormatters() (sqlFormatters map[string]func(string) (sqlValue string, err error))
	getFinalCsvFormatters() (finalCsvFormatters map[string]func(string) (finalCsvValue string, err error))

	// -------------------
	// -- DDL overrides --
	// -------------------

	createSchemaIfNotExistsOverride(schema string) (overridden bool, err error)
	createTableIfNotExistsOverride(schema, table string, transferInfo *data.TransferInfo) (overridden bool, err error)
	dropTableIfExistsOverride(schema, table string) (overridden bool, err error)
	createDbIfNotExistsOverride(database string) (overridden bool, err error)

	// *******************
	// ** Data movement **
	// *******************

	createPipeFilesOverride(pipeFileInfoChannel chan PipeFileInfo, transferInfo *data.TransferInfo, rows *sql.Rows) (pipeFileChannel chan PipeFileInfo, overridden bool)
	convertPipeFilesOverride(pipeFileInfoChannel <-chan PipeFileInfo, finalCsvInfoChannel chan FinalCsvInfo, transferInfo *data.TransferInfo) (finalCsvChannel chan FinalCsvInfo, overridden bool)
	insertPipeFilesOverride(transferInfo *data.TransferInfo, pipeFileInfoChannel <-chan PipeFileInfo) (overridden bool, err error)
	insertFinalCsvsOverride(transferInfo *data.TransferInfo) (overridden bool, err error)
	runInsertCmd(finalCsvInfo FinalCsvInfo, transferInfo *data.TransferInfo, schema, table string) (err error)
	getIncrementalTimeOverride(schema, table, incrementalColumn string, intialLoad bool) (incrementalTime time.Time, overridden bool, initialLoad bool, err error)

	// --------------------
	// -- Data discovery --
	// --------------------

	discoverStructure(instanceTransfer *data.InstanceTransfer) (*data.InstanceTransfer, error)
}

func newSystem(connectionInfo ConnectionInfo) (system System, err error) {
	// creates a new system

	switch connectionInfo.Type {
	case "postgresql":
		return newPostgresql(connectionInfo)
	case "mssql":
		return newMssql(connectionInfo)
	case "mysql":
		return newMysql(connectionInfo)
	case "oracle":
		return newOracle(connectionInfo)
	case "snowflake":
		return newSnowflake(connectionInfo)
	default:
		return system, fmt.Errorf("unsupported system type %v", connectionInfo.Type)
	}
}

func openConnectionPool(connectionString, driverName string) (connectionPool *sql.DB, err error) {

	connectionPool, err = sql.Open(driverName, connectionString)
	if err != nil {
		return nil, fmt.Errorf("error opening connection :: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = connectionPool.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("error pinging :: %v", err)
	}

	return connectionPool, nil
}

func createTableIfNotExists(
	transferInfo *data.TransferInfo,
	target System,
) (
	err error,
) {

	overridden, err := target.createTableIfNotExistsOverride(transferInfo.TargetSchema, transferInfo.TargetTable, transferInfo)
	if overridden {
		return err
	}

	schemaPeriodTable := getSchemaPeriodTable(transferInfo.TargetSchema, transferInfo.TargetTable, target, true)

	var queryBuilder = strings.Builder{}

	queryBuilder.WriteString("create table if not exists ")
	queryBuilder.WriteString(schemaPeriodTable)
	queryBuilder.WriteString(" (")

	for i := range transferInfo.ColumnInfos {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}

		escapedName := escapeIfNeeded(transferInfo.ColumnInfos[i].Name, target)

		queryBuilder.WriteString(escapedName)
		queryBuilder.WriteString(" ")

		createType, err := target.pipeTypeToCreateType(transferInfo.ColumnInfos[i])
		if err != nil {
			return fmt.Errorf("error getting create type for column %v :: %v", transferInfo.ColumnInfos[i].Name, err)
		}

		queryBuilder.WriteString(createType)
	}

	queryBuilder.WriteString(")")

	err = target.exec(queryBuilder.String())
	if err != nil {
		return fmt.Errorf("error running create table %v :: %v", schemaPeriodTable, err)
	}

	logger.Info("table created if not exists", "database", transferInfo.TargetDatabase, "staging-database", transferInfo.StagingDbName, "schema", transferInfo.TargetSchema, "table", transferInfo.TargetTable)

	return nil
}

type PipeFileInfo struct {
	FilePath   string
	PkFilePath string
}

func createPipeFiles(
	transferInfo *data.TransferInfo,
	rows *sql.Rows,
	source System,
) <-chan PipeFileInfo {

	pipeFileInfoChannel := make(chan PipeFileInfo)

	pipeFileInfoChannel, overridden := source.createPipeFilesOverride(pipeFileInfoChannel, transferInfo, rows)
	if overridden {
		return pipeFileInfoChannel
	}

	go func() {

		defer close(pipeFileInfoChannel)

		pipeFileFormatters := source.getPipeFileFormatters()

		pipeFileNum := 0

		pipeFile, err := os.Create(
			filepath.Join(transferInfo.PipeFileDir, fmt.Sprintf("%032b.pipe", pipeFileNum)))
		if err != nil {
			transferInfo.Error = fmt.Sprintf("error creating temp file :: %v", err)
			logger.Error(transferInfo.Error)
			return
		}
		defer pipeFile.Close()

		numCols := len(transferInfo.ColumnInfos)

		csvWriter := csv.NewWriter(pipeFile)
		csvLength := 0

		values := make([]interface{}, numCols)
		valuePtrs := make([]interface{}, numCols)
		for i := 0; i < numCols; i++ {
			valuePtrs[i] = &values[i]
		}

		dataInRam := false
		csvRow := make([]string, numCols)

		eg := errgroup.Group{}

		for rows.Next() {

			err := rows.Scan(valuePtrs...)
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error scanning row :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			eg.Go(func() error {
				for i := 0; i < numCols; i++ {
					if values[i] == nil {
						csvRow[i] = transferInfo.Null
						csvLength += len(transferInfo.Null)
					} else {
						csvRow[i], err = pipeFileFormatters[transferInfo.ColumnInfos[i].PipeType](values[i])
						if err != nil {
							err = fmt.Errorf("error formatting pipe file :: %v", err)
							transferInfo.Error = err.Error()
							logger.Error(transferInfo.Error)
							return err
						}
						csvLength += len(csvRow[i])
					}
				}
				return nil
			})

			err = eg.Wait()
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error formatting pipe file :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			eg.Go(func() error {
				err = csvWriter.Write(csvRow)
				if err != nil {
					err = fmt.Errorf("error writing csv row :: %v", err)
					transferInfo.Error = err.Error()
					logger.Error(transferInfo.Error)
					return err
				}
				return nil
			})

			err = eg.Wait()
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error writing pipe file :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			dataInRam = true

			if csvLength > 10_000_000 {

				eg.Go(func() error {
					csvWriter.Flush()

					err = pipeFile.Close()
					if err != nil {
						err = fmt.Errorf("error closing pipe file :: %v", err)
						transferInfo.Error = err.Error()
						logger.Error(transferInfo.Error)
						return err
					}
					return nil
				})

				err = eg.Wait()
				if err != nil {
					transferInfo.Error = fmt.Sprintf("error writing pipe file :: %v", err)
					logger.Error(transferInfo.Error)
					return
				}

				select {
				case <-transferInfo.Context.Done():
					return
				default:
				}

				pkFilePath := ""

				pipeFileInfo := PipeFileInfo{
					FilePath:   pipeFile.Name(),
					PkFilePath: pkFilePath,
				}

				pipeFileInfoChannel <- pipeFileInfo

				pipeFileNum++

				eg.Go(func() error {
					pipeFileName := filepath.Join(
						transferInfo.PipeFileDir, fmt.Sprintf("%032b.pipe", pipeFileNum))

					pipeFile, err = os.Create(pipeFileName)
					if err != nil {
						err = fmt.Errorf("error creating temp file :: %v", err)
						transferInfo.Error = err.Error()
						logger.Error(transferInfo.Error)
						return err
					}
					csvWriter = csv.NewWriter(pipeFile)

					return nil
				})
				defer pipeFile.Close()
				dataInRam = false
				csvLength = 0

				err = eg.Wait()
				if err != nil {
					transferInfo.Error = fmt.Sprintf("error writing pipe file :: %v", err)
					logger.Error(transferInfo.Error)
					return
				}

			}
		}

		if err := rows.Err(); err != nil {
			transferInfo.Error = fmt.Sprintf("error iterating rows :: %v", err)
			return
		}

		if dataInRam {

			eg.Go(func() error {
				csvWriter.Flush()

				err = pipeFile.Close()
				if err != nil {
					err = fmt.Errorf("error closing pipe file :: %v", err)
					transferInfo.Error = err.Error()
					logger.Error(transferInfo.Error)
					return err
				}
				return nil
			})

			err = eg.Wait()
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error writing pipe file :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			pkFilePath := ""

			pipeFileInfo := PipeFileInfo{
				FilePath:   pipeFile.Name(),
				PkFilePath: pkFilePath,
			}

			pipeFileInfoChannel <- pipeFileInfo
		}

		logger.Info(fmt.Sprintf("transfer %v finished writing pipe files", transferInfo.ID))
	}()

	return pipeFileInfoChannel
}

func insertPipeFiles(pipeFileChannel <-chan PipeFileInfo, transferInfo *data.TransferInfo, target System) (err error) {

	overridden, err := target.insertPipeFilesOverride(transferInfo, pipeFileChannel)
	if overridden {
		return err
	}

	finalCsvChannel := convertPipeFiles(pipeFileChannel, transferInfo, target)

	table := transferInfo.TargetTable

	err = insertFinalCsvs(finalCsvChannel, transferInfo, target, transferInfo.TargetSchema, table)
	if err != nil {
		return fmt.Errorf("error inserting final csvs :: %v", err)
	}

	return nil
}

func scanPipeFilesForPii(pipeFileInfoChannel <-chan PipeFileInfo, transferInfo *data.TransferInfo) <-chan PipeFileInfo {
	scannedPipeFileChannel := make(chan PipeFileInfo)

	go func() {
		defer close(scannedPipeFileChannel)

		for pipeFileInfo := range pipeFileInfoChannel {

			select {
			case <-transferInfo.Context.Done():
				return
			default:
			}

			if !transferInfo.ScannedForPII {

				// Create a new pipe file with column names as the first row
				columnNamesFile, err := os.Create(filepath.Join(transferInfo.PipeFileDir, "column_names.pipe"))
				if err != nil {
					transferInfo.Error = fmt.Sprintf("error creating column names file :: %v", err)
					logger.Error(transferInfo.Error)
					return
				}

				columnNamesWriter := csv.NewWriter(columnNamesFile)
				columnNames := make([]string, len(transferInfo.ColumnInfos))
				for i, colInfo := range transferInfo.ColumnInfos {
					columnNames[i] = colInfo.Name
				}

				err = columnNamesWriter.Write(columnNames)
				if err != nil {
					transferInfo.Error = fmt.Sprintf("error writing column names to file :: %v", err)
					logger.Error(transferInfo.Error)
					return
				}

				// Write the contents of the current pipe file to the columnNamesFile
				pipeFile, err := os.Open(pipeFileInfo.FilePath)
				if err != nil {
					transferInfo.Error = fmt.Sprintf("error opening pipe file :: %v", err)
					logger.Error(transferInfo.Error)
					return
				}

				pipeReader := csv.NewReader(pipeFile)
				for {
					record, err := pipeReader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						transferInfo.Error = fmt.Sprintf("error reading pipe file :: %v", err)
						logger.Error(transferInfo.Error)
						return
					}

					err = columnNamesWriter.Write(record)
					if err != nil {
						transferInfo.Error = fmt.Sprintf("error writing to column names file :: %v", err)
						logger.Error(transferInfo.Error)
						return
					}
				}

				columnNamesWriter.Flush()
				columnNamesFile.Close()
				pipeFile.Close()

				// find python binary location by getting text written at /python_location.txt
				pythonLocationBytes, err := os.ReadFile("/python_location.txt")
				if err != nil {
					transferInfo.Error = fmt.Sprintf("error reading python location :: %v", err)
					logger.Error(transferInfo.Error)
					return
				}

				logger.Info("running pii_scan.py", "python_file", columnNamesFile.Name(), "custom_strategry_threshold", instanceTransfer.CustomStrategyThreshold, "custom_strategy_percentile", instanceTransfer.CustomStrategyPercentile, "num_ros_to_scan_for_pii", instanceTransfer.NumRowsToScannForPII)

				// Command to run the Python script
				cmd := exec.Command(string(pythonLocationBytes), "/pii_scan.py", columnNamesFile.Name(), fmt.Sprintf("%f", instanceTransfer.CustomStrategyThreshold), fmt.Sprintf("%f", instanceTransfer.CustomStrategyPercentile), fmt.Sprintf("%d", instanceTransfer.NumRowsToScannForPII))

				// Capture standard output and error
				output, err := cmd.CombinedOutput()
				if err != nil {
					transferInfo.Error = fmt.Sprintf("error scanning for PII :: %v", string(output))
					logger.Error("error scanning for PII", "output", string(output), "error", err)
					return
				}

				// Parse the JSON output
				var result map[string]interface{}
				if err := json.Unmarshal(output, &result); err != nil {
					transferInfo.Error = fmt.Sprintf("error parsing JSON output :: %v", err)
					logger.Error(transferInfo.Error)
					return
				}

				os.Remove(columnNamesFile.Name())

				// Output the result
				fmt.Printf("Analysis Result: %+v\n", result)

				for columnName := range result {
					columnNode, exists := transferInfo.TableNode.FindChildNodeByName(columnName)
					if !exists {
						logger.Error("column not found in tree", "column", columnName)
						return
					} else {
						// logger.Info("found node", "node", node.Name)
						columnNode.ChangeContainsPII(true, result[columnName].(string))
					}
				}
			}

			transferInfo.ScannedForPII = true

			scannedPipeFileChannel <- pipeFileInfo
		}
	}()

	return scannedPipeFileChannel
}

func convertPipeFiles(
	pipeFileInfoChannel <-chan PipeFileInfo,
	transferInfo *data.TransferInfo,
	target System,
) <-chan FinalCsvInfo {

	finalCsvInfoChannel := make(chan FinalCsvInfo)

	finalCsvInfoChannel, overridden := target.convertPipeFilesOverride(pipeFileInfoChannel, finalCsvInfoChannel, transferInfo)
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
				transferInfo.Error = fmt.Sprintf("error opening pipeFile :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			fileNum, err := getFileNum(pipeFilePath)
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error getting fileNum :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			csvFileName := filepath.Join(transferInfo.FinalCsvDir, fmt.Sprintf("%032v.csv", fileNum))
			csvFile, err := os.Create(csvFileName)
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error creating csv file :: %v", err)
				logger.Error(transferInfo.Error)
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
					transferInfo.Error = fmt.Sprintf("error reading csv row :: %v", err)
					logger.Error(transferInfo.Error)
					return
				}

				for i := range row {
					if row[i] != transferInfo.Null {
						row[i], err = finalCsvFormatters[transferInfo.ColumnInfos[i].PipeType](row[i])
						if err != nil {
							transferInfo.Error = fmt.Sprintf("error formatting final csv :: %v", err)
							logger.Error(transferInfo.Error)
							return
						}
					}
				}

				err = csvWriter.Write(row)
				if err != nil {
					transferInfo.Error = fmt.Sprintf("error writing csv row :: %v", err)
					logger.Error(transferInfo.Error)
					return
				}
			}

			err = pipeFile.Close()
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error closing pipeFile :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			csvWriter.Flush()

			err = csvFile.Close()
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error closing csvFile :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			select {
			case <-transferInfo.Context.Done():
				return
			default:
			}

			finalCsvInfo := FinalCsvInfo{
				FilePath:   csvFile.Name(),
				InsertInfo: csvFile.Name(),
			}

			finalCsvInfoChannel <- finalCsvInfo

			if !transferInfo.KeepFiles {
				err = os.Remove(pipeFilePath)
				if err != nil {
					transferInfo.Error = fmt.Sprintf("error removing pipeFile :: %v", err)
					return
				}
			}
		}

		logger.Info(fmt.Sprintf("transfer %v finished converting pipe files to final csvs", transferInfo.ID))
	}()

	return finalCsvInfoChannel
}

func insertFinalCsvs(
	finalCsvChannel <-chan FinalCsvInfo,
	transferInfo *data.TransferInfo,
	target System,
	schema, table string,
) (
	err error,
) {
	// inserts final csvs into the target system

	for finalCsvinfo := range finalCsvChannel {

		select {
		case <-transferInfo.Context.Done():
			return errors.New("context cancelled")
		default:
		}

		err = target.runInsertCmd(finalCsvinfo, transferInfo, schema, table)
		if err != nil {
			return fmt.Errorf("error inserting final csv :: %v", err)
		}

		if !transferInfo.KeepFiles {
			err = os.Remove(finalCsvinfo.FilePath)
			if err != nil {
				return fmt.Errorf("error removing final csv :: %v", err)
			}
		}
	}

	logger.Info(fmt.Sprintf("transfer %v finished inserting final csvs", transferInfo.ID))

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

func createDbIfNotExists(transferInfo *data.TransferInfo, system System) (err error) {

	database := transferInfo.TargetDatabase

	overridden, err := system.createDbIfNotExistsOverride(database)
	if overridden {
		return err
	}

	query := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %v`, escapeIfNeeded(database, system))
	err = system.exec(query)
	if err != nil {
		return fmt.Errorf("error creating database %v :: %v", database, err)
	}

	logger.Info("database created if not exists", "database", transferInfo.TargetDatabase)

	return nil
}

func createStagingDbIfNotExists(transferInfo *data.TransferInfo, system System) (err error) {

	database := transferInfo.StagingDbName

	overridden, err := system.createDbIfNotExistsOverride(database)
	if overridden {
		return err
	}

	query := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %v`, escapeIfNeeded(database, system))
	err = system.exec(query)
	if err != nil {
		return fmt.Errorf("error creating database %v :: %v", database, err)
	}

	logger.Info("staging database created if not exists", "staging-database", transferInfo.StagingDbName, "database", transferInfo.TargetDatabase)

	return nil
}

func createSchemaIfNotExists(transferInfo *data.TransferInfo, system System) (err error) {
	overridden, err := system.createSchemaIfNotExistsOverride(transferInfo.TargetSchema)
	if overridden {
		return err
	}

	query := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %v`, escapeIfNeeded(transferInfo.TargetSchema, system))

	err = system.exec(query)
	if err != nil {
		return fmt.Errorf("error creating schema %v :: %v", transferInfo.TargetSchema, err)
	}

	logger.Info("schema created if not exists", "database", transferInfo.TargetDatabase, "staging-database", transferInfo.StagingDbName, "schema", transferInfo.TargetSchema)

	return nil
}

func dropTableIfExists(transferInfo *data.TransferInfo, system System) (err error) {
	overridden, err := system.dropTableIfExistsOverride(transferInfo.TargetSchema, transferInfo.TargetTable)
	if overridden {
		return err
	}

	escapedSchemaPeriodTable := getSchemaPeriodTable(transferInfo.TargetSchema, transferInfo.TargetTable, system, true)

	query := fmt.Sprintf("drop table if exists %v", escapedSchemaPeriodTable)
	err = system.exec(query)
	if err != nil {
		return fmt.Errorf("error dropping table %v :: %v", escapedSchemaPeriodTable, err)
	}

	logger.Info("dropped table if exists", "database", transferInfo.TargetDatabase, "staging-database", transferInfo.StagingDbName, "schema", transferInfo.TargetSchema, "table", transferInfo.TargetTable)

	return nil
}

func getTableColumnInfos(transferInfo *data.TransferInfo, system System) (err error) {

	schema, table := transferInfo.TargetSchema, transferInfo.TargetTable

	rows, err := system.getTableColumnInfosRows(schema, table)
	if err != nil {
		return fmt.Errorf("error getting table column infos rows :: %v", err)
	}

	var columnName string
	var columnType string
	var columnPrecision int64
	var columnScale int64
	var columnLength int64
	var columnIsPrimary bool

	for rows.Next() {
		err := rows.Scan(&columnName, &columnType, &columnPrecision, &columnScale, &columnLength, &columnIsPrimary)
		if err != nil {
			return fmt.Errorf("error scanning table column infos rows :: %v", err)
		}

		pipeType, err := system.dbTypeToPipeType(columnType)
		if err != nil {
			return fmt.Errorf("error getting pipe type for column %v :: %v", columnName, err)
		}

		decimalOk := false
		if columnPrecision > 0 || columnScale > 0 {
			decimalOk = true
		}

		lengthOk := false
		if columnLength > 0 {
			lengthOk = true
		}

		columnInfo := &data.ColumnInfo{
			Name:         columnName,
			PipeType:     pipeType,
			DecimalOk:    decimalOk,
			Precision:    columnPrecision,
			Scale:        columnScale,
			LengthOk:     lengthOk,
			Length:       columnLength,
			IsPrimaryKey: columnIsPrimary,
		}

		transferInfo.ColumnInfos = append(transferInfo.ColumnInfos, columnInfo)

		columnTreeID := fmt.Sprintf("%v_%v", transferInfo.TableNode.ID, columnName)

		transferInfo.TableNode.AddChild(columnTreeID, columnName)
	}

	if len(transferInfo.ColumnInfos) == 0 {
		return fmt.Errorf("no columns found for table %v.%v", schema, table)
	}

	return nil
}
