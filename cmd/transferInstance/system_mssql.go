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
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sqlpipe/sqlpipe/internal/data"
)

type Mssql struct {
	Connection     *sql.DB
	ConnectionInfo ConnectionInfo
}

func newMssql(connectionInfo ConnectionInfo) (mssql Mssql, err error) {

	connectionString := fmt.Sprintf("sqlserver://%v:%v@%v:%v?database=%v", connectionInfo.Username,
		connectionInfo.Password, connectionInfo.Hostname, connectionInfo.Port, connectionInfo.Database)

	db, err := openConnectionPool(connectionString, DriverMSSQL)
	if err != nil {
		return mssql, fmt.Errorf("error opening mssql db :: %v", err)
	}

	mssql.Connection = db
	mssql.ConnectionInfo = connectionInfo

	return mssql, nil
}

func (system Mssql) closeConnectionPool(printError bool) (err error) {
	err = system.Connection.Close()
	if err != nil && printError {
		logger.Error(fmt.Sprintf("error closing connection pool :: %v", err))
	}
	return err
}

func (system Mssql) query(query string) (rows *sql.Rows, err error) {
	rows, err = system.Connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error running dql :: %v :: %v", query, err)
	}
	return rows, nil
}

func (system Mssql) queryRow(query string) (row *sql.Row) {
	row = system.Connection.QueryRow(query)
	return row
}

func (system Mssql) exec(query string) (err error) {
	_, err = system.Connection.Exec(query)
	if err != nil {
		return fmt.Errorf("error running ddl/dml on :: %v :: %v", query, err)
	}
	return nil
}

func (system Mssql) dropTableIfExistsOverride(schema, table string) (overridden bool, err error) {
	return false, nil
}

func (system Mssql) createTableIfNotExistsOverride(schema, table string, transferInfo *data.TransferInfo) (overridden bool, err error) {

	return false, errors.New("not implemented")

	// unescapedSchemaPeriodTable := getSchemaPeriodTable(schema, table, system, true)
	// escapedSchemaPeriodTable := getSchemaPeriodTable(schema, table, system, true)

	// escapedPrimaryKeys := []string{}

	// for i := range columnInfos {
	// 	if columnInfos[i].IsPrimaryKey {
	// 		escapedPrimaryKeys = append(escapedPrimaryKeys, escapeIfNeeded(columnInfos[i].Name, system))
	// 	}
	// }

	// var queryBuilder = strings.Builder{}
	// // IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'YourTableName' AND schema_id = SCHEMA_ID('YourSchemaName'))
	// queryBuilder.WriteString("IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '")
	// queryBuilder.WriteString(table)
	// queryBuilder.WriteString("' AND schema_id = SCHEMA_ID('")
	// queryBuilder.WriteString(schema)
	// queryBuilder.WriteString("')) BEGIN CREATE TABLE ")
	// queryBuilder.WriteString(escapedSchemaPeriodTable)
	// queryBuilder.WriteString(" (")

	// for i := range columnInfos {
	// 	if i > 0 {
	// 		queryBuilder.WriteString(", ")
	// 	}

	// 	escapedName := escapeIfNeeded(columnInfos[i].Name, system)

	// 	queryBuilder.WriteString(escapedName)
	// 	queryBuilder.WriteString(" ")

	// 	createType, err := system.pipeTypeToCreateType(columnInfos[i])
	// 	if err != nil {
	// 		return false, fmt.Errorf("error getting create type for column %v :: %v", columnInfos[i].Name, err)
	// 	}

	// 	queryBuilder.WriteString(createType)
	// }

	// if incremental && len(escapedPrimaryKeys) > 0 {
	// 	queryBuilder.WriteString(", primary key (")
	// 	queryBuilder.WriteString(strings.Join(escapedPrimaryKeys, ","))
	// 	queryBuilder.WriteString(")")
	// }

	// queryBuilder.WriteString(") END")

	// err = system.exec(queryBuilder.String())
	// if err != nil {
	// 	return false, fmt.Errorf("error running create table %v :: %v", unescapedSchemaPeriodTable, err)
	// }

	// logger.Info(fmt.Sprintf("created table %v if not exists", unescapedSchemaPeriodTable))

	// return true, nil
}

func (system Mssql) driverTypeToPipeType(
	columnType *sql.ColumnType,
	databaseTypeName string,
) (
	pipeType string,
	err error,
) {
	switch databaseTypeName {
	case "NVARCHAR":
		return "nvarchar", nil
	case "NCHAR":
		return "nvarchar", nil
	case "VARCHAR":
		return "varchar", nil
	case "CHAR":
		return "varchar", nil
	case "NTEXT":
		return "ntext", nil
	case "TEXT":
		return "text", nil
	case "BIGINT":
		return "int64", nil
	case "INT":
		return "int32", nil
	case "SMALLINT":
		return "int16", nil
	case "TINYINT":
		return "int16", nil
	case "FLOAT":
		return "float64", nil
	case "REAL":
		return "float32", nil
	case "DECIMAL":
		return "decimal", nil
	case "NUMERIC":
		return "decimal", nil
	case "MONEY":
		return "money", nil
	case "SMALLMONEY":
		return "money", nil
	case "DATETIME2":
		return "datetime", nil
	case "DATETIME":
		return "datetime", nil
	case "SMALLDATETIME":
		return "datetime", nil
	case "DATETIMEOFFSET":
		return "datetimetz", nil
	case "DATE":
		return "date", nil
	case "TIME":
		return "time", nil
	case "BINARY":
		return "varbinary", nil
	case "IMAGE":
		return "blob", nil
	case "VARBINARY":
		return "blob", nil
	case "UNIQUEIDENTIFIER":
		return "uuid", nil
	case "BIT":
		return "bool", nil
	case "XML":
		return "xml", nil
	default:
		return "", fmt.Errorf(
			"unsupported database type for mssql: %v", columnType.DatabaseTypeName())
	}
}

func (system Mssql) dbTypeToPipeType(
	databaseTypeName string,
) (
	pipeType string,
	err error,
) {
	switch databaseTypeName {
	case "bigint":
		return "int64", nil
	case "int":
		return "int32", nil
	case "smallint":
		return "int16", nil
	case "tinyint":
		return "int16", nil
	case "float":
		return "float64", nil
	case "real":
		return "float32", nil
	case "decimal":
		return "decimal", nil
	case "numeric":
		return "decimal", nil
	case "money":
		return "money", nil
	case "smallmoney":
		return "money", nil
	case "datetime2":
		return "datetime", nil
	case "datetime":
		return "datetime", nil
	case "smalldatetime":
		return "datetime", nil
	case "datetimeoffset":
		return "datetimetz", nil
	case "date":
		return "date", nil
	case "time":
		return "time", nil
	case "binary":
		return "varbinary", nil
	case "timestamp":
		return "varbinary", nil
	case "bit":
		return "bool", nil
	case "xml":
		return "xml", nil
	case "nvarchar":
		return "nvarchar", nil
	case "varchar":
		return "varchar", nil
	case "nchar":
		return "nvarchar", nil
	case "char":
		return "varchar", nil
	case "text":
		return "text", nil
	case "ntext":
		return "ntext", nil
	case "varbinary":
		return "varbinary", nil
	case "image":
		return "blob", nil
	case "uniqueidentifier":
		return "uuid", nil
	default:
		return "", fmt.Errorf("unsupported database type for mssql: %v", databaseTypeName)
	}
}

func (system Mssql) pipeTypeToCreateType(columnInfo *data.ColumnInfo) (createType string, err error) {

	columnInfo.Length = columnInfo.Length * 2

	switch columnInfo.PipeType {
	case "nvarchar":
		if columnInfo.LengthOk {
			if columnInfo.Length <= 0 {
				return "nvarchar(max)", nil
			} else if columnInfo.Length <= 4000 {
				return fmt.Sprintf("nvarchar(%v)", columnInfo.Length), nil
			} else {
				return "nvarchar(max)", nil
			}
		} else {
			return "nvarchar(4000)", nil
		}
	case "varchar":
		if columnInfo.LengthOk {
			if columnInfo.Length <= 0 {
				return "varchar(max)", nil
			} else if columnInfo.Length <= 8000 {
				return fmt.Sprintf("varchar(%v)", columnInfo.Length), nil
			} else {
				return "varchar(max)", nil
			}
		} else {
			return "varchar(8000)", nil
		}
	case "ntext":
		return "nvarchar(max)", nil
	case "text":
		return "varchar(max)", nil
	case "int64":
		return "bigint", nil
	case "int32":
		return "integer", nil
	case "int16":
		return "smallint", nil
	case "float64":
		return "float", nil
	case "float32":
		return "real", nil
	case "decimal":
		scaleOk := false
		precisionOk := false

		if columnInfo.DecimalOk {
			if columnInfo.Scale > 0 && columnInfo.Scale <= 38 {
				scaleOk = true
			}

			if columnInfo.Precision > 0 &&
				columnInfo.Precision <= 38 &&
				columnInfo.Precision > columnInfo.Scale {
				precisionOk = true
			}
		}

		if scaleOk && precisionOk {
			return fmt.Sprintf("decimal(%v,%v)", columnInfo.Precision, columnInfo.Scale), nil
		} else {
			return "float", nil
		}
	case "money":
		return "money", nil
	case "datetime":
		return "datetime2", nil
	case "datetimetz":
		return "datetime2", nil
	case "date":
		return "date", nil
	case "time":
		return "time", nil
	case "varbinary":
		if columnInfo.LengthOk {
			if columnInfo.Length <= 0 {
				return "varbinary(max)", nil
			} else if columnInfo.Length <= 8000 {
				return fmt.Sprintf("varbinary(%v)", columnInfo.Length), nil
			} else {
				return "varbinary(max)", nil
			}
		} else {
			return "varbinary(8000)", nil
		}
	case "blob":
		return "varbinary(max)", nil
	case "uuid":
		return "uniqueidentifier", nil
	case "bool":
		return "bit", nil
	case "json":
		if columnInfo.LengthOk {
			if columnInfo.Length <= 0 {
				return "nvarchar(max)", nil
			} else if columnInfo.Length <= 4000 {
				return fmt.Sprintf("nvarchar(%v)", columnInfo.Length), nil
			} else {
				return "nvarchar(max)", nil
			}
		} else {
			return "nvarchar(4000)", nil
		}
	case "xml":
		return "xml", nil
	case "varbit":
		if columnInfo.LengthOk {
			if columnInfo.Length <= 0 {
				return "varchar(max)", nil
			} else if columnInfo.Length <= 8000 {
				return fmt.Sprintf("varchar(%v)", columnInfo.Length), nil
			} else {
				return "varchar(max)", nil
			}
		} else {
			return "varchar(8000)", nil
		}
	default:
		return "", fmt.Errorf("unsupported pipeType for mssql: %v", columnInfo.PipeType)
	}
}

func (system Mssql) createPipeFilesOverride(pipeFileChannelIn chan PipeFileInfo, transferInfo *data.TransferInfo, rows *sql.Rows,
) (pipeFileInfoChannel chan PipeFileInfo, overridden bool) {
	return pipeFileChannelIn, false
}

func (system Mssql) getPipeFileFormatters() (
	pipeFileFormatters map[string]func(interface{}) (pipeFileValue string, err error),
) {
	return map[string]func(interface{}) (pipeFileValue string, err error){
		"nvarchar": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"varchar": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"ntext": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"text": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"int64": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%d", v), nil
		},
		"int32": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%d", v), nil
		},
		"int16": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%d", v), nil
		},
		"float64": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%f", v), nil
		},
		"float32": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%f", v), nil
		},
		"decimal": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"money": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"datetime": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to datetime mssqlPipeFileFormatters",
				)
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"datetimetz": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to datetimetz mssqlPipeFileFormatters",
				)
			}
			return valTime.UTC().Format(time.RFC3339Nano), nil
		},
		"date": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to date mssqlPipeFileFormatters",
				)
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"time": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to time mssqlPipeFileFormatters",
				)
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"varbinary": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%x", v), nil
		},
		"blob": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%x", v), nil
		},
		"uuid": func(v interface{}) (pipeFileValue string, err error) {
			val, ok := v.([]uint8)
			if !ok {
				return "", errors.New(
					"non byte array value passed to uuid mssqlPipeFileFormatters",
				)
			}
			return fmt.Sprintf("%02X%02X%02X%02X-%02X%02X-%02X%02X-%02X%02X-%02X",
				val[3],
				val[2],
				val[1],
				val[0],
				val[5],
				val[4],
				val[7],
				val[6],
				val[8],
				val[9],
				val[10:],
			), nil
		},
		"bool": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%t", v), nil
		},
		"json": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"xml": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
	}
}

func (system Mssql) insertPipeFilesOverride(transferInfo *data.TransferInfo, pipeFileInfoChannel <-chan PipeFileInfo) (overridden bool, err error) {
	return false, nil
}

func (system Mssql) convertPipeFilesOverride(
	pipeFileInfoChannel <-chan PipeFileInfo,
	finalCsvInfoChannel chan FinalCsvInfo,
	transferInfo *data.TransferInfo,
) (chan FinalCsvInfo, bool) {
	// converts a pipe file to a csv that can be uploaded by bcp

	finalCsvChannel := make(chan FinalCsvInfo)

	finalCsvFormatters := system.getFinalCsvFormatters()

	go func() {

		defer close(finalCsvChannel)

		for pipeFileInfo := range pipeFileInfoChannel {

			pipeFile, err := os.Open(pipeFileInfo.FilePath)
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error opening pipeFile :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			fileNum, err := getFileNum(pipeFileInfo.FilePath)
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error getting file num :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			finalCsvFile, err := os.Create(
				filepath.Join(transferInfo.FinalCsvDir, fmt.Sprintf("%032b.csv", fileNum)),
			)
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error creating final csv file :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			csvReader := csv.NewReader(pipeFile)
			csvBuilder := strings.Builder{}

			var value string

			for {
				row, err := csvReader.Read()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					transferInfo.Error = fmt.Sprintf("error reading pipe file :: %v", err)
					logger.Error(transferInfo.Error)
					return
				}

				for i := range row {
					if i != 0 {
						csvBuilder.WriteString(transferInfo.Delimiter)
					}
					if row[i] == transferInfo.Null {
						csvBuilder.WriteString("")
					} else {
						value, err = finalCsvFormatters[transferInfo.ColumnInfos[i].PipeType](row[i])
						if err != nil {
							transferInfo.Error = fmt.Sprintf(
								"error formatting value for final csv :: %v", err)
							logger.Error(transferInfo.Error)
							return
						}
						csvBuilder.WriteString(value)
					}
				}
				csvBuilder.WriteString(transferInfo.Newline)
			}

			_, err = finalCsvFile.WriteString(csvBuilder.String())
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error writing to final csv file :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			err = finalCsvFile.Close()
			if err != nil {
				transferInfo.Error = fmt.Sprintf("error closing final csv file :: %v", err)
				logger.Error(transferInfo.Error)
				return
			}

			err = pipeFile.Close()
			if err != nil {
				if !strings.Contains(err.Error(), "file already closed") {
					transferInfo.Error = fmt.Sprintf("error closing pipe file :: %v", err)
					logger.Error(transferInfo.Error)
					return
				}
			}

			select {
			case <-transferInfo.Context.Done():
				return
			default:
				finalCsvInfo := FinalCsvInfo{
					FilePath:   finalCsvFile.Name(),
					InsertInfo: finalCsvFile.Name(),
				}

				finalCsvChannel <- finalCsvInfo
			}

		}
		logger.Info("finished converting pipe files")
	}()

	return finalCsvChannel, true
}

func (system Mssql) insertFinalCsvsOverride(transferInfo *data.TransferInfo) (overridden bool, err error) {
	return false, nil
}

func (system Mssql) getFinalCsvFormatters() map[string]func(string) (string, error) {
	return map[string]func(string) (string, error){
		"nvarchar": func(v string) (string, error) {
			if v == "" {
				return "\x00", nil
			}
			return v, nil
		},
		"varchar": func(v string) (string, error) {
			if v == "" {
				return "\x00", nil
			}
			return v, nil
		},
		"ntext": func(v string) (string, error) {
			if v == "" {
				return "\x00", nil
			}
			return v, nil
		},
		"text": func(v string) (string, error) {
			if v == "" {
				return "\x00", nil
			}
			return v, nil
		},
		"int64": func(v string) (string, error) {
			return v, nil
		},
		"int32": func(v string) (string, error) {
			return v, nil
		},
		"int16": func(v string) (string, error) {
			return v, nil
		},
		"float64": func(v string) (string, error) {
			return v, nil
		},
		"float32": func(v string) (string, error) {
			return v, nil
		},
		"decimal": func(v string) (string, error) {
			return v, nil
		},
		"money": func(v string) (string, error) {
			return v, nil
		},
		"datetime": func(v string) (string, error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to bcp csv :: %v", err)
			}
			return valTime.Format("2006-01-02 15:04:05.9999999"), nil
		},
		"datetimetz": func(v string) (string, error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf(
					"error parsing datetimetz value in mssql datetimetz mssql formatter :: %v",
					err)
			}

			return valTime.UTC().Format("2006-01-02 15:04:05.9999999"), nil
		},
		"date": func(v string) (string, error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to bcp csv :: %v", err)
			}
			return valTime.Format("2006-01-02"), nil
		},
		"time": func(v string) (string, error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing time value to bcp csv :: %v", err)
			}

			return valTime.Format("15:04:05.999999"), nil
		},
		"varbinary": func(v string) (string, error) {
			return v, nil
		},
		"blob": func(v string) (string, error) {
			return v, nil
		},
		"uuid": func(v string) (string, error) {
			return v, nil
		},
		"bool": func(v string) (string, error) {
			switch v {
			case "true":
				return "1", nil
			case "false":
				return "0", nil
			default:
				return "", fmt.Errorf("error writing bool value to bcp csv :: %v", v)
			}
		},
		"json": func(v string) (string, error) {
			return v, nil
		},
		"xml": func(v string) (string, error) {
			return v, nil
		},
		"varbit": func(v string) (string, error) {
			return v, nil
		},
	}
}

func (system Mssql) runInsertCmd(
	finalCsvInfo FinalCsvInfo,
	transferInfo *data.TransferInfo,
	schema, table string,
) (
	err error,
) {

	// fileNum, err := getFileNum(finalCsvInfo.FilePath)
	// if err != nil {
	// 	return fmt.Errorf("error getting file num :: %v", err)
	// }

	// escapedSchemaPeriodtable := getSchemaPeriodTable(schema, table, system, true)

	// cmd := exec.CommandContext(
	// 	transferInfo.Context,
	// 	"bcp",
	// 	fmt.Sprintf("%v.%v",
	// 		transferInfo.TargetDatabase, escapedSchemaPeriodtable,
	// 	),
	// 	"in",
	// 	finalCsvInfo.FilePath,
	// 	"-c",
	// 	"-S", transferInfo.TargetHostname,
	// 	"-U", transferInfo.TargetUsername,
	// 	"-P", transferInfo.TargetPassword,
	// 	"-t", transferInfo.Delimiter,
	// 	"-r", transferInfo.Newline,
	// 	"-e", filepath.Join(transferInfo.FinalCsvDir, fmt.Sprintf("%032b.err", fileNum)),
	// )

	// result, err := cmd.CombinedOutput()
	// if err != nil {
	// 	return fmt.Errorf(
	// 		"failed to upload csv to mssql :: stderr %v :: stdout %s", err, string(result),
	// 	)
	// }

	// return nil

	return errors.New("not implemented")
}

func (system Mssql) schemaRequired() bool {
	return true
}

var mssqlReservedKeywords = map[string]bool{
	"ADD":                            true,
	"ALL":                            true,
	"ALTER":                          true,
	"AND":                            true,
	"ANY":                            true,
	"AS":                             true,
	"ASC":                            true,
	"AUTHORIZATION":                  true,
	"BACKUP":                         true,
	"BEGIN":                          true,
	"BETWEEN":                        true,
	"BREAK":                          true,
	"BROWSE":                         true,
	"BULK":                           true,
	"BY":                             true,
	"CASCADE":                        true,
	"CASE":                           true,
	"CHECK":                          true,
	"CHECKPOINT":                     true,
	"CLOSE":                          true,
	"CLUSTERED":                      true,
	"COALESCE":                       true,
	"COLLATE":                        true,
	"COLUMN":                         true,
	"COMMIT":                         true,
	"COMPUTE":                        true,
	"CONSTRAINT":                     true,
	"CONTAINS":                       true,
	"CONTAINSTABLE":                  true,
	"CONTINUE":                       true,
	"CONVERT":                        true,
	"CREATE":                         true,
	"CROSS":                          true,
	"CURRENT":                        true,
	"CURRENT_DATE":                   true,
	"CURRENT_TIME":                   true,
	"CURRENT_TIMESTAMP":              true,
	"CURRENT_USER":                   true,
	"CURSOR":                         true,
	"DATABASE":                       true,
	"DBCC":                           true,
	"DEALLOCATE":                     true,
	"DECLARE":                        true,
	"DEFAULT":                        true,
	"DELETE":                         true,
	"DENY":                           true,
	"DESC":                           true,
	"DISK":                           true,
	"DISTINCT":                       true,
	"DISTRIBUTED":                    true,
	"DOUBLE":                         true,
	"DROP":                           true,
	"DUMP":                           true,
	"ELSE":                           true,
	"END":                            true,
	"ERRLVL":                         true,
	"ESCAPE":                         true,
	"EXCEPT":                         true,
	"EXEC":                           true,
	"EXECUTE":                        true,
	"EXISTS":                         true,
	"EXIT":                           true,
	"EXTERNAL":                       true,
	"FETCH":                          true,
	"FILE":                           true,
	"FILLFACTOR":                     true,
	"FOR":                            true,
	"FOREIGN":                        true,
	"FREETEXT":                       true,
	"FREETEXTTABLE":                  true,
	"FROM":                           true,
	"FULL":                           true,
	"FUNCTION":                       true,
	"GOTO":                           true,
	"GRANT":                          true,
	"GROUP":                          true,
	"HAVING":                         true,
	"HOLDLOCK":                       true,
	"IDENTITY":                       true,
	"IDENTITY_INSERT":                true,
	"IDENTITYCOL":                    true,
	"IF":                             true,
	"IN":                             true,
	"INDEX":                          true,
	"INNER":                          true,
	"INSERT":                         true,
	"INTERSECT":                      true,
	"INTO":                           true,
	"IS":                             true,
	"JOIN":                           true,
	"KEY":                            true,
	"KILL":                           true,
	"LEFT":                           true,
	"LIKE":                           true,
	"LINENO":                         true,
	"LOAD":                           true,
	"MERGE":                          true,
	"NATIONAL":                       true,
	"NOCHECK":                        true,
	"NONCLUSTERED":                   true,
	"NOT":                            true,
	"NULL":                           true,
	"NULLIF":                         true,
	"OF":                             true,
	"OFF":                            true,
	"OFFSETS":                        true,
	"ON":                             true,
	"OPEN":                           true,
	"OPENDATASOURCE":                 true,
	"OPENQUERY":                      true,
	"OPENROWSET":                     true,
	"OPENXML":                        true,
	"OPTION":                         true,
	"OR":                             true,
	"ORDER":                          true,
	"OUTER":                          true,
	"OVER":                           true,
	"PERCENT":                        true,
	"PIVOT":                          true,
	"PLAN":                           true,
	"PRECISION":                      true,
	"PRIMARY":                        true,
	"PRINT":                          true,
	"PROC":                           true,
	"PROCEDURE":                      true,
	"PUBLIC":                         true,
	"RAISEERROR":                     true,
	"RANGE":                          true,
	"READ":                           true,
	"READTEXT":                       true,
	"RECONFIGURE":                    true,
	"REFERENCES":                     true,
	"REPLICATION":                    true,
	"RESTORE":                        true,
	"RESTRICT":                       true,
	"RETURN":                         true,
	"REVERT":                         true,
	"REVOKE":                         true,
	"RIGHT":                          true,
	"ROLLBACK":                       true,
	"ROWCOUNT":                       true,
	"ROWGUIDCOL":                     true,
	"RULE":                           true,
	"SAVE":                           true,
	"SCHEMA":                         true,
	"SECURITYAUDIT":                  true,
	"SELECT":                         true,
	"SEMANTICKEYPHRASETABLE":         true,
	"SEMANTICSIMILARITYDETAILSTABLE": true,
	"SEMANTICSIMILARITYTABLE":        true,
	"SESSION_USER":                   true,
	"SET":                            true,
	"SETUSER":                        true,
	"SHUTDOWN":                       true,
	"SOME":                           true,
	"STATISTICS":                     true,
	"SYSTEM_USER":                    true,
	"TABLE":                          true,
	"TABLESAMPLE":                    true,
	"TEXTSIZE":                       true,
	"THEN":                           true,
	"TO":                             true,
	"TOP":                            true,
	"TRAN":                           true,
	"TRANSACTION":                    true,
	"TRIGGER":                        true,
	"TRUNCATE":                       true,
	"TRY_CONVERT":                    true,
	"TSEQUAL":                        true,
	"UNION":                          true,
	"UNIQUE":                         true,
	"UNPIVOT":                        true,
	"UPDATE":                         true,
	"UPDATETEXT":                     true,
	"USE":                            true,
	"USER":                           true,
	"VALUES":                         true,
	"VARYING":                        true,
	"VIEW":                           true,
	"WAITFOR":                        true,
	"WHEN":                           true,
	"WHERE":                          true,
	"WHILE":                          true,
	"WITH":                           true,
	"WITHIN GROUP":                   true,
	"WRITETEXT":                      true,
}

func (system Mssql) isReservedKeyword(objectName string) bool {

	if _, ok := mssqlReservedKeywords[strings.ToUpper(objectName)]; ok {
		return true
	}
	return false
}

func (system Mssql) escape(objectName string) (escaped string) {
	return fmt.Sprintf("[%v]", objectName)
}

func (system Mssql) createSchemaIfNotExistsOverride(schema string) (overridden bool, err error) {

	escapedSchema := escapeIfNeeded(schema, system)

	query := fmt.Sprintf(`
	IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '%v')
	BEGIN
		EXEC('CREATE SCHEMA %v')
	END`,
		schema,
		escapedSchema,
	)

	err = system.exec(query)
	if err != nil {
		return false, fmt.Errorf("error creating schema %v :: %v", schema, err)
	}

	return true, nil
}

func (system Mssql) getIncrementalTimeOverride(schema, table, incrementalColumn string, initialLoad bool) (time.Time, bool, bool, error) {
	return time.Time{}, false, initialLoad, nil
}

func (system Mssql) IsTableNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "Invalid object name")
}

func (system Mssql) getPrimaryKeysRows(schema, table string) (rows *sql.Rows, err error) {

	unescapedSchemaPeriodTable := getSchemaPeriodTable(schema, table, system, false)

	query := fmt.Sprintf(`
		SELECT
			col.name AS column_name
		FROM
			sys.indexes idx
		JOIN
			sys.index_columns idx_col ON idx.object_id = idx_col.object_id AND idx.index_id = idx_col.index_id
		JOIN
			sys.columns col ON idx.object_id = col.object_id AND idx_col.column_id = col.column_id
		WHERE
			idx.is_primary_key = 1
			AND idx.object_id = OBJECT_ID('%v')`,
		unescapedSchemaPeriodTable)

	rows, err = system.query(query)
	if err != nil {
		return nil, fmt.Errorf("error getting primary keys rows :: %v", err)
	}

	return rows, nil
}

var mssqlDatetimeFormat = "2006-01-02 15:04:05.9999999"
var mssqlDateFormat = "2006-01-02"
var mssqlTimeFormat = "15:04:05.9999999"

func (system Mssql) getSqlFormatters() (
	pipeFileFormatters map[string]func(string) (pipeFileValue string, err error),
) {
	return map[string]func(string) (pipeFileValue string, err error){
		"nvarchar": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"varchar": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"ntext": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"text": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"int64": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"int32": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"int16": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"float64": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"float32": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"decimal": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"money": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"datetime": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to mssql csv :: %v", err)
			}
			return fmt.Sprintf("'%v'", valTime.Format(mssqlDatetimeFormat)), nil
		},
		"datetimetz": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to mssql csv :: %v", err)
			}
			return fmt.Sprintf("'%v'", valTime.Format(mssqlDatetimeFormat)), nil
		},
		"date": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to mssql csv :: %v", err)
			}
			return fmt.Sprintf("'%v'", valTime.Format(mssqlDateFormat)), nil
		},
		"time": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to mssql csv :: %v", err)
			}

			return fmt.Sprintf("'%v'", valTime.Format(mssqlTimeFormat)), nil
		},
		"varbinary": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf(`0x%s`, v), nil
		},
		"blob": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf(`0x%s`, v), nil
		},
		"uuid": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"bool": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"json": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"xml": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"varbit": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
	}
}

func (system Mssql) getTableColumnInfosRows(schema, table string) (rows *sql.Rows, err error) {
	query := fmt.Sprintf(`
		WITH PrimaryKeys AS (
			SELECT
				c.COLUMN_NAME
			FROM
				INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
				JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS c
					ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
					AND tc.TABLE_NAME = c.TABLE_NAME
			WHERE
				tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
				AND tc.TABLE_SCHEMA = '%v'
				AND tc.TABLE_NAME = '%v'
		)

		SELECT
			columns.COLUMN_NAME AS col_name,
			columns.DATA_TYPE AS col_type,
			coalesce(columns.NUMERIC_PRECISION, -1) AS col_precision,
			coalesce(columns.NUMERIC_SCALE, -1) AS col_scale,
			coalesce(columns.CHARACTER_MAXIMUM_LENGTH, -1) AS col_length,
			CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS col_is_primary
		FROM
			INFORMATION_SCHEMA.COLUMNS AS columns
			LEFT JOIN PrimaryKeys pk ON columns.COLUMN_NAME = pk.COLUMN_NAME
		WHERE columns.TABLE_SCHEMA = '%v'
			AND columns.TABLE_NAME = '%v'
		ORDER BY
			columns.ORDINAL_POSITION;`, schema, table, schema, table)

	rows, err = system.query(query)
	if err != nil {
		return nil, fmt.Errorf("error getting table column infos rows :: %v", err)
	}

	return rows, nil
}

func (system Mssql) createDbIfNotExistsOverride(database string) (overridden bool, err error) {
	return false, errors.New("not implemented")
}

func (system Mssql) discoverStructure(instanceTransfer *data.InstanceTransfer) (*data.InstanceTransfer, error) {

	// Create an ID for the instance tree node based on the instance properties.
	treeNodeInstanceID := fmt.Sprintf("%v_%v_%v_%v",
		instanceTransfer.SourceInstance.CloudProvider,
		instanceTransfer.SourceInstance.CloudAccountID,
		instanceTransfer.SourceInstance.Region,
		instanceTransfer.SourceInstance.ID,
	)

	instanceTransfer.SchemaTree = &data.SafeTreeNode{
		ID:          treeNodeInstanceID,
		Name:        instanceTransfer.SourceInstance.ID,
		ContainsPII: false,
		Mu:          &sync.Mutex{},
	}

	// List all databases on the SQL Server instance.
	databases, err := system.listAllDatabases()
	if err != nil {
		return nil, fmt.Errorf("error listing all databases :: %v", err)
	}

	// print databases
	for _, database := range databases {
		fmt.Println(database)
	}

	dbConnInfo := system.ConnectionInfo

	for _, database := range databases {

		// Skip SQL Server system databases.
		if database == "master" || database == "tempdb" || database == "model" || database == "msdb" || database == "rdsadmin" {
			continue
		}

		treeNodeDBID := fmt.Sprintf("%v_%v", treeNodeInstanceID, database)
		dbNode := instanceTransfer.SchemaTree.AddChild(treeNodeDBID, database)

		// Set the target database in the connection info.
		dbConnInfo.Database = database

		// Create a new SQL Server connection using the SQLServer-specific function.
		dbSystem, err := newMssql(dbConnInfo)
		if err != nil {
			return nil, fmt.Errorf("error creating db system :: %v", err)
		}

		// List all schemas in the current database.
		schemas, err := dbSystem.listAllSchemasInDatabase()
		if err != nil {
			return nil, fmt.Errorf("error listing all schemas in database %v :: %v", database, err)
		}

		placeholders := map[string]string{
			"cloud_provider":   instanceTransfer.SourceInstance.CloudProvider,
			"cloud_account_id": instanceTransfer.SourceInstance.CloudAccountID,
			"cloud_region":     instanceTransfer.SourceInstance.Region,
			"instance_id":      instanceTransfer.SourceInstance.ID,
		}

		targetDbTemplate := instanceTransfer.NamingConvention.DatabaseNameInSnowflake
		targetSchemaTemplate := instanceTransfer.NamingConvention.SchemaNameInSnowflake
		targetTableTemplate := instanceTransfer.NamingConvention.TableNameInSnowflake

		for _, schema := range schemas {
			treeNodeSchemaID := fmt.Sprintf("%v_%v", treeNodeDBID, schema)
			schemaNode := dbNode.AddChild(treeNodeSchemaID, schema)

			// List all tables in the schema.
			tables, err := dbSystem.listAllTablesInSchema(schema)
			if err != nil {
				return nil, fmt.Errorf("error listing all tables in schema %v :: %v", schema, err)
			}
			for _, table := range tables {
				treeNodeTableID := fmt.Sprintf("%v_%v", treeNodeSchemaID, table)
				tableNode := schemaNode.AddChild(treeNodeTableID, table)

				id := uuid.New().String()
				ctx, cancel := context.WithCancel(context.Background())

				placeholders["database_name"] = database
				placeholders["schema_name"] = schema
				placeholders["table_name"] = table

				pattern := regexp.MustCompile(`\[([^\]]+)\]`)

				targetDbName := pattern.ReplaceAllStringFunc(targetDbTemplate, func(m string) string {
					key := m[1 : len(m)-1]
					if val, found := placeholders[key]; found {
						return val
					}
					return m
				})
				targetDbName = dashToUnderscoreReplacer.Replace(targetDbName)

				targetSchemaName := pattern.ReplaceAllStringFunc(targetSchemaTemplate, func(m string) string {
					key := m[1 : len(m)-1]
					if val, found := placeholders[key]; found {
						return val
					}
					return m
				})
				targetSchemaName = dashToUnderscoreReplacer.Replace(targetSchemaName)
				if targetSchemaName == "" {
					targetSchemaName = instanceTransfer.NamingConvention.SchemaFallbackInSnowflake
				}

				targetTableName := pattern.ReplaceAllStringFunc(targetTableTemplate, func(m string) string {
					key := m[1 : len(m)-1]
					if val, found := placeholders[key]; found {
						return val
					}
					return m
				})
				targetTableName = dashToUnderscoreReplacer.Replace(targetTableName)

				// Replace dashes with underscores in the RestoredInstanceID.
				stagingDbNameSuffix := strings.ReplaceAll(instanceTransfer.RestoredInstanceID, "-", "_")
				stagingDbName := fmt.Sprintf("%v_%v", stagingDbNameSuffix, database)

				transferInfo := &data.TransferInfo{
					ID:      id,
					Context: ctx,
					Cancel:  cancel,
					SourceInstance: data.Instance{
						ID:       instanceTransfer.SourceInstance.ID,
						Type:     instanceTransfer.SourceInstance.Type,
						Host:     instanceTransfer.SourceInstance.Host,
						Port:     instanceTransfer.SourceInstance.Port,
						Username: instanceTransfer.SourceInstance.Username,
						Password: instanceTransfer.SourceInstance.Password,
					},
					SourceDatabase:                database,
					SourceSchema:                  schema,
					SourceTable:                   table,
					TargetType:                    instanceTransfer.TargetType,
					TargetHost:                    instanceTransfer.TargetHost,
					TargetUsername:                instanceTransfer.TargetUsername,
					TargetPassword:                instanceTransfer.TargetPassword,
					TargetDatabase:                targetDbName,
					TargetSchema:                  targetSchemaName,
					TargetTable:                   targetTableName,
					DropTargetTableIfExists:       true,
					CreateTargetSchemaIfNotExists: true,
					CreateTargetTableIfNotExists:  true,
					Delimiter:                     instanceTransfer.Delimiter,
					Newline:                       instanceTransfer.Newline,
					Null:                          instanceTransfer.Null,
					PsqlAvailable:                 instanceTransfer.PsqlAvailable,
					BcpAvailable:                  instanceTransfer.BcpAvailable,
					SqlLdrAvailable:               instanceTransfer.SqlLdrAvailable,
					StagingDbName:                 stagingDbName,
					TableNode:                     tableNode,
					ScanForPII:                    instanceTransfer.ScanForPII,
				}
				instanceTransfer.TransferInfos = append(instanceTransfer.TransferInfos, transferInfo)
			}
		}
	}

	return instanceTransfer, nil
}

func (system Mssql) listAllDatabases() (databases []string, err error) {
	rows, err := system.query("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb');")
	if err != nil {
		return nil, fmt.Errorf("error listing all databases :: %v", err)
	}

	for rows.Next() {
		var database string
		err = rows.Scan(&database)
		if err != nil {
			return nil, fmt.Errorf("error scanning database :: %v", err)
		}
		databases = append(databases, database)
	}

	return databases, nil
}

func (system Mssql) listAllSchemasInDatabase() (schemas []string, err error) {
	rows, err := system.query(`SELECT
	name
FROM
	sys.schemas
WHERE
	name NOT IN ('guest', 'sys', 'INFORMATION_SCHEMA', 'db_accessadmin', 'db_backupoperator',
'db_datareader',
'db_datawriter',
'db_ddladmin',
'db_denydatareader',
'db_denydatawriter',
'db_owner',
'db_securityadmin')`)
	if err != nil {
		return nil, fmt.Errorf("error listing all schemas in database :: %v", err)
	}

	for rows.Next() {
		var schema string
		err = rows.Scan(&schema)
		if err != nil {
			return nil, fmt.Errorf("error scanning schema :: %v", err)
		}

		schemas = append(schemas, schema)
	}

	return schemas, nil
}

func (system Mssql) listAllTablesInSchema(schema string) (tables []string, err error) {
	query := fmt.Sprintf(`SELECT t.name
		FROM sys.tables t
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = '%v'`, schema)
	rows, err := system.query(query)
	if err != nil {
		return nil, fmt.Errorf("error listing all tables in schema :: %v", err)
	}

	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, fmt.Errorf("error scanning table :: %v", err)
		}

		tables = append(tables, table)
	}

	return tables, nil
}
