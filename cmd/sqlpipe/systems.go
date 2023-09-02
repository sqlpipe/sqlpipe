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
	getFinalCsvFormatters() map[string]func(string) (string, error)
	runUploadCmd(transfer *Transfer, csvFileName string) error
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
	case "snowflake":
		return newSnowflake(name, connectionString)
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
	out := make(chan string)

	transferErrGroup.Go(func() error {

		defer close(out)

		pipeFileFormatters, err := transfer.Source.getPipeFileFormatters()
		if err != nil {
			return fmt.Errorf("error getting pipe file formatters :: %v", err)
		}

		pipeFileNum := 1

		pipeFile, err := os.Create(filepath.Join(transfer.PipeFileDir, fmt.Sprintf("%b.pipe", pipeFileNum)))
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
				pipeFile, err = os.Create(filepath.Join(transfer.PipeFileDir, fmt.Sprintf("%b.pipe", pipeFileNum)))
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

		infoLog.Printf("pipe files written at %v", transfer.PipeFileDir)
		return nil
	})

	return out, nil
}

func convertPipeFilesCommon(transfer *Transfer, in <-chan string, transferErrGroup *errgroup.Group) (<-chan string, error) {

	out := make(chan string)

	finalCsvFormatters := transfer.Target.getFinalCsvFormatters()

	transferErrGroup.Go(func() error {
		defer close(out)

		for pipeFileName := range in {

			pipeFileName := pipeFileName
			conversionErrGroup := errgroup.Group{}
			conversionErrGroup.SetLimit(1)

			conversionErrGroup.Go(func() error {
				pipeFile, err := os.Open(pipeFileName)
				if err != nil {
					return fmt.Errorf("error opening pipeFile :: %v", err)
				}
				defer pipeFile.Close()

				if !transfer.KeepFiles {
					defer os.Remove(pipeFileName)
				}

				// strip path from pipeFile name, get number
				pipeFileNameClean := filepath.Base(pipeFileName)
				pipeFileNum := strings.Split(pipeFileNameClean, ".")[0]

				psqlCsvFile, err := os.Create(filepath.Join(transfer.FinalCsvDir, fmt.Sprintf("%s.csv", pipeFileNum)))
				if err != nil {
					return fmt.Errorf("error creating final csv file :: %v", err)
				}
				defer psqlCsvFile.Close()

				csvReader := csv.NewReader(pipeFile)
				csvWriter := csv.NewWriter(psqlCsvFile)

				for {
					row, err := csvReader.Read()
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}
						return fmt.Errorf("error reading csv values in %v :: %v", pipeFile.Name(), err)
					}

					for i := range row {
						if row[i] != transfer.Null {
							row[i], err = finalCsvFormatters[transfer.ColumnInfo[i].pipeType](row[i])
							if err != nil {
								return fmt.Errorf("error formatting pipe file to final csv :: %v", err)
							}
						}
					}

					err = csvWriter.Write(row)
					if err != nil {
						return fmt.Errorf("error writing final csv :: %v", err)
					}
				}

				err = pipeFile.Close()
				if err != nil {
					return fmt.Errorf("error closing pipeFile :: %v", err)
				}

				csvWriter.Flush()

				err = psqlCsvFile.Close()
				if err != nil {
					return fmt.Errorf("error closing final csv file :: %v", err)
				}

				out <- psqlCsvFile.Name()

				return nil
			})

			err := conversionErrGroup.Wait()
			if err != nil {
				return fmt.Errorf("error converting pipeFiles :: %v", err)
			}
		}

		infoLog.Printf("converted pipe files to final csvs at %v\n", transfer.FinalCsvDir)
		return nil
	})

	return out, nil
}

func insertFinalCsvCommon(transfer *Transfer, in <-chan string) error {
	insertErrGroup := errgroup.Group{}

	for finalCsvFileName := range in {
		finalCsvFileName := finalCsvFileName

		insertErrGroup.Go(func() error {
			if !transfer.KeepFiles {
				defer os.Remove(finalCsvFileName)
			}

			err := transfer.Target.runUploadCmd(transfer, finalCsvFileName)
			if err != nil {
				return fmt.Errorf("error running upload cmd :: %v", err)
			}

			return nil
		})
		err := insertErrGroup.Wait()
		if err != nil {
			return fmt.Errorf("error inserting final csvs :: %v", err)
		}
	}

	infoLog.Printf("finished inserting final csvs for transfer %v\n", transfer.Id)

	return nil
}
