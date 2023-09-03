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
	dropTable(schema, table string) (dropped string, err error)
	query(query string) (*sql.Rows, error)
	exec(query string) (err error)
	getColumnInfo(rows *sql.Rows) ([]ColumnInfo, error)
	getPipeFileFormatters() (map[string]func(interface{}) (string, error), error)
	dbTypeToPipeType(databaseTypeName string, columnType sql.ColumnType) (pipeType string, err error)
	pipeTypeToCreateType(columnInfo ColumnInfo) (createType string, err error)
	createPipeFiles(transferErrGroup *errgroup.Group, pipeFileDir, null, transferId string, columnInfo []ColumnInfo, rows *sql.Rows) (out <-chan string, err error)
	insertPipeFiles(ctx context.Context, targetSchema, targetTable, csvFileName, transferId, finalCsvDir, null, targetConnectionString string, target System, keepFiles bool, columnInfo []ColumnInfo, in <-chan string, transferErrGroup *errgroup.Group) error
	getFinalCsvFormatters() map[string]func(string) (string, error)
	runUploadCmd(targetSchema, targetTable, csvFileName, null, targetConnectionString string) error
}

func newSystem(name, systemType, connectionString string) (System, error) {
	switch systemType {
	case TypePostgreSQL:
		return newPostgresql(name, connectionString)
	case TypeMSSQL:
		return newMssql(name, connectionString)
	case TypeMySQL:
		return newMysql(name, connectionString)
	case TypeOracle:
		return newOracle(name, connectionString)
	case TypeSnowflake:
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

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	err = db.PingContext(pingCtx)
	if err != nil {
		return nil, fmt.Errorf("error pinging %v :: %v", name, err)
	}

	return db, nil
}

func dropTableIfExistsCommon(schema, table string, system System) (string, error) {

	if schema != "" {
		schema = fmt.Sprintf("%v.", schema)
	}

	toDrop := fmt.Sprintf("%v%v", schema, table)

	err := system.exec(fmt.Sprintf("drop table if exists %v", toDrop))
	if err != nil {
		return "", fmt.Errorf("error dropping %v%v :: %v", schema, table, err)
	}

	return toDrop, nil
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

func getColumnInfoCommon(rows *sql.Rows, source System) ([]ColumnInfo, error) {
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

		scanType, err := getScanType(columnTypes[i])
		if err != nil {
			warningLog.Printf("error getting scantype for column %v :: %v", columnNames[i], err)
		}

		pipeType, err := source.dbTypeToPipeType(columnTypes[i].DatabaseTypeName(), *columnTypes[i])
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

func createTableCommon(targetSchema, targetTable string, columnInfo []ColumnInfo, target System) (string, error) {
	if targetSchema != "" {
		targetSchema = fmt.Sprintf("%v.", targetSchema)
	}

	toCreate := fmt.Sprintf("%v%v", targetSchema, targetTable)

	createSchema := targetSchema

	var queryBuilder = strings.Builder{}

	queryBuilder.WriteString("create table ")
	queryBuilder.WriteString(toCreate)
	queryBuilder.WriteString(" (")

	for i := range columnInfo {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		queryBuilder.WriteString(columnInfo[i].name)
		queryBuilder.WriteString(" ")
		createType, err := target.pipeTypeToCreateType(columnInfo[i])
		if err != nil {
			return "", fmt.Errorf("error getting create type for column %v :: %v", columnInfo[i].name, err)
		}
		queryBuilder.WriteString(createType)
	}
	queryBuilder.WriteString(")")

	err := target.exec(queryBuilder.String())
	if err != nil {
		return "", fmt.Errorf("error running create table %v.%v :: %v", createSchema, targetTable, err)
	}

	return toCreate, nil
}

func createPipeFilesCommon(source System, transferErrGroup *errgroup.Group, pipeFileDir, null, transferId string, columnInfo []ColumnInfo, rows *sql.Rows) (<-chan string, error) {
	out := make(chan string)

	transferErrGroup.Go(func() error {

		defer close(out)

		pipeFileFormatters, err := source.getPipeFileFormatters()
		if err != nil {
			return fmt.Errorf("error getting pipe file formatters :: %v", err)
		}

		pipeFileNum := 1

		pipeFile, err := os.Create(filepath.Join(pipeFileDir, fmt.Sprintf("%b.pipe", pipeFileNum)))
		if err != nil {
			return fmt.Errorf("error creating temp file :: %v", err)
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
				return fmt.Errorf("error scanning row :: %v", err)
			}

			for j := range columnInfo {
				if values[j] == nil {
					csvRow[j] = null
					csvLength += 5
				} else {
					stringVal, err := pipeFileFormatters[columnInfo[j].pipeType](values[j])
					if err != nil {
						return fmt.Errorf("error while formatting pipe type %v :: %v", columnInfo[j].pipeType, err)
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
				pipeFile, err = os.Create(filepath.Join(pipeFileDir, fmt.Sprintf("%b.pipe", pipeFileNum)))
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

		infoLog.Printf("transfer %v finished writing pipe files", transferId)
		return nil
	})

	return out, nil
}

func convertPipeFilesCommon(ctx context.Context, transferId, finalCsvDir, null string, target System, keepFiles bool, columnInfo []ColumnInfo, in <-chan string, transferErrGroup *errgroup.Group) (<-chan string, error) {

	out := make(chan string)

	finalCsvFormatters := target.getFinalCsvFormatters()

	transferErrGroup.Go(func() error {
		defer close(out)

		for pipeFileName := range in {
			select {
			case <-ctx.Done():
				return errors.New("context cancelled")
			default:

				pipeFileName := pipeFileName
				conversionErrGroup := errgroup.Group{}
				conversionErrGroup.SetLimit(1)

				conversionErrGroup.Go(func() error {
					pipeFile, err := os.Open(pipeFileName)
					if err != nil {
						return fmt.Errorf("error opening pipeFile :: %v", err)
					}
					defer pipeFile.Close()

					if !keepFiles {
						defer os.Remove(pipeFileName)
					}

					// strip path from pipeFile name, get number
					pipeFileNameClean := filepath.Base(pipeFileName)
					pipeFileNum := strings.Split(pipeFileNameClean, ".")[0]

					psqlCsvFile, err := os.Create(filepath.Join(finalCsvDir, fmt.Sprintf("%s.csv", pipeFileNum)))
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
							if row[i] != null {
								row[i], err = finalCsvFormatters[columnInfo[i].pipeType](row[i])
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
		}

		infoLog.Printf("transfer %v finished converting pipe files to final csvs", transferId)
		return nil
	})

	return out, nil
}

func insertFinalCsvCommon(ctx context.Context, targetSchema, targetTable, null, targetConnectionString, transferId string, keepFiles bool, target System, in <-chan string) error {

	insertErrGroup := errgroup.Group{}

	for finalCsvFileName := range in {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		default:

			finalCsvFileName := finalCsvFileName

			insertErrGroup.Go(func() error {
				if !keepFiles {
					defer os.Remove(finalCsvFileName)
				}

				err := target.runUploadCmd(targetSchema, targetTable, finalCsvFileName, null, targetConnectionString)
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
	}

	infoLog.Printf("transfer %v finished inserting final csvs", transferId)

	return nil
}
