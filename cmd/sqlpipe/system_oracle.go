package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type Oracle struct {
	name             string
	connectionString string
	connection       *sql.DB
}

func newOracle(name, connectionString string) (oracle Oracle, err error) {
	if name == "" {
		name = TypeOracle
	}
	db, err := openDbCommon(name, connectionString, DriverOracle)
	if err != nil {
		return oracle, fmt.Errorf("error opening oracle db :: %v", err)
	}
	oracle.connection = db
	oracle.name = name
	oracle.connectionString = connectionString
	return oracle, nil
}

func (system Oracle) query(query string) (rows *sql.Rows, err error) {
	rows, err = system.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error running dql on %v :: %v :: %v", system.name, query, err)
	}
	return rows, nil
}

func (system Oracle) exec(query string) (err error) {
	_, err = system.connection.Exec(query)
	if err != nil {
		return fmt.Errorf("error running ddl/dml on %v :: %v :: %v", system.name, query, err)
	}
	return nil
}

func (system Oracle) dropTableIfExists(transfer Transfer) (dropped string, err error) {

	dropped = fmt.Sprintf("%v.%v", transfer.TargetSchema, transfer.TargetTable)

	query := fmt.Sprintf("drop table %v", dropped)
	err = system.exec(query)
	if err != nil {
		if !strings.Contains(err.Error(), "ORA-00942") {
			return "", fmt.Errorf("error dropping %v :: %v", dropped, err)
		}
	}

	return dropped, nil
}

func (system Oracle) getColumnInfo(rows *sql.Rows) (columnInfo []ColumnInfo, err error) {
	return getColumnInfoCommon(rows, system)
}

func (system Oracle) createTable(
	columnInfo []ColumnInfo,
	transfer Transfer,
) (
	created string,
	err error,
) {
	return createTableCommon(columnInfo, transfer, system)
}

func (system Oracle) dbTypeToPipeType(
	columnType *sql.ColumnType,
	databaseTypeName string,
) (
	pipeType string,
	err error,
) {
	switch databaseTypeName {
	case "NCHAR":
		pipeType = "nvarchar"
	case "CHAR":
		pipeType = "nvarchar"
	case "OCIClobLocator":
		pipeType = "ntext"
	case "LONG":
		pipeType = "ntext"
	case "IBDouble":
		pipeType = "float64"
	case "IBFloat":
		pipeType = "float32"
	case "NUMBER":
		pipeType = "decimal"
	case "TimeStampDTY":
		pipeType = "datetime"
	case "TimeStampTZ_DTY":
		pipeType = "datetimetz"
	case "TimeStampLTZ_DTY":
		pipeType = "datetimetz"
	case "DATE":
		pipeType = "date"
	case "RAW":
		pipeType = "varbinary"
	case "OCIBlobLocator":
		pipeType = "blob"
	case "OCIFileLocator":
		pipeType = "blob"
	case "ROWID":
		pipeType = "nvarchar"
	case "UROWID":
		pipeType = "nvarchar"
	case "IntervalYM_DTY":
		pipeType = "nvarchar"
	case "IntervalDS_DTY":
		pipeType = "nvarchar"
	default:
		return "", fmt.Errorf(
			"unsupported database type for oracle: %v", columnType.DatabaseTypeName())
	}

	return pipeType, nil
}

func (system Oracle) pipeTypeToCreateType(columnInfo ColumnInfo) (createType string, err error) {
	switch columnInfo.pipeType {
	case "nvarchar", "varchar":
		if columnInfo.lengthOk {
			if columnInfo.length <= 0 {
				createType = "varchar2(4000)"
			} else if columnInfo.length <= 4000 {
				createType = fmt.Sprintf("varchar2(%v)", columnInfo.length)
			} else {
				createType = "clob"
			}
		} else {
			createType = "varchar2(4000)"
		}
	case "ntext", "text":
		createType = "clob"
	case "int64":
		createType = "number(19)"
	case "int32":
		createType = "number(10)"
	case "int16":
		createType = "number(5)"
	case "float64":
		createType = "BINARY_DOUBLE"
	case "float32":
		createType = "BINARY_FLOAT"
	case "decimal":

		precisionOk := false
		scaleOk := false

		if columnInfo.decimalOk {
			if columnInfo.precision >= 0 && columnInfo.precision <= 38 {
				precisionOk = true
			}
			if columnInfo.scale >= 0 && columnInfo.scale <= 38 {
				scaleOk = true
			}
		}

		if precisionOk && scaleOk {
			createType = fmt.Sprintf("decimal(%v, %v)", columnInfo.precision, columnInfo.scale)
		} else {
			createType = "varchar2(64)"
		}

	case "money":
		if columnInfo.decimalOk {
			createType = fmt.Sprintf("decimal(%v, %v)", columnInfo.scale, columnInfo.precision)
		} else {
			createType = fmt.Sprintf("decimal(%v, %v)", 38, 4)
		}
	case "datetime":
		createType = "timestamp"
	case "datetimetz":
		createType = "timestamp"
	case "date":
		createType = "date"
	case "time":
		createType = "varchar2(256)"
	case "varbinary":
		if columnInfo.lengthOk {
			if columnInfo.length <= 0 {
				createType = "raw(2000)"
			} else if columnInfo.length <= 2000 {
				createType = fmt.Sprintf("raw(%v)", columnInfo.length)
			} else {
				createType = "blob"
			}
		} else {
			createType = "raw(2000)"
		}
	case "blob":
		createType = "blob"
	case "uuid":
		createType = "raw(16)"
	case "bool":
		createType = "number(1)"
	case "json":
		createType = "clob"
	case "xml":
		if columnInfo.lengthOk {
			if columnInfo.length <= 0 {
				createType = "varchar2(4000)"
			} else if columnInfo.length <= 4000 {
				createType = fmt.Sprintf("varchar2(%v)", columnInfo.length)
			} else {
				createType = "clob"
			}
		} else {
			createType = "varchar2(4000)"
		}
	case "varbit":
		if columnInfo.lengthOk {
			if columnInfo.length <= 0 {
				createType = "varchar2(4000)"
			} else if columnInfo.length <= 4000 {
				createType = fmt.Sprintf("varchar2(%v)", columnInfo.length)
			} else {
				createType = "clob"
			}
		} else {
			createType = "varchar2(4000)"
		}
	default:
		return "", fmt.Errorf("unsupported pipe type for oracle: %v", columnInfo.pipeType)
	}

	return createType, nil
}

func (system Oracle) createPipeFiles(
	ctx context.Context,
	errorChannel chan<- error,
	columnInfo []ColumnInfo,
	transfer Transfer,
	rows *sql.Rows,
) (
	pipeFileChannel <-chan string,
) {
	return createPipeFilesCommon(ctx, errorChannel, columnInfo, transfer, rows, system)
}

func (system Oracle) getPipeFileFormatters() (
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
					"non time.Time value passed to datetime oraclePipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"datetimetz": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to datetimetz oraclePipeFileFormatters")
			}
			return valTime.UTC().Format(time.RFC3339Nano), nil
		},
		"date": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to date oraclePipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"time": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"varbinary": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%x", v), nil
		},
		"blob": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%x", v), nil
		},
		"uuid": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
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

func (system Oracle) insertPipeFiles(
	ctx context.Context,
	pipeFileChannel <-chan string,
	errorChannel chan<- error,
	columnInfo []ColumnInfo,
	transfer Transfer,
) (
	err error,
) {
	finalCsvChannel := system.convertPipeFiles(ctx, pipeFileChannel, errorChannel,
		columnInfo, transfer)

	err = system.insertFinalCsvs(ctx, finalCsvChannel, transfer)
	if err != nil {
		return fmt.Errorf("error inserting final csvs :: %v", err)
	}

	return nil
}

func (system Oracle) convertPipeFiles(
	ctx context.Context,
	pipeFileChannelIn <-chan string,
	errorChannel chan<- error,
	columnInfo []ColumnInfo,
	transfer Transfer,
) <-chan string {

	convertedFinalCsvChannel := convertPipeFilesCommon(ctx, pipeFileChannelIn, errorChannel, columnInfo, transfer, system)

	finalCsvChannelOut := make(chan string)
	go func() {
		defer close(finalCsvChannelOut)

		for finalCsvFileName := range convertedFinalCsvChannel {
			// strip path from pipeFile name, get number
			fileNum, err := getFileNum(finalCsvFileName)
			if err != nil {
				errorChannel <- fmt.Errorf("error getting file number :: %v", err)
				return
			}

			finalCsvFile, err := os.Open(finalCsvFileName)
			if err != nil {
				errorChannel <- fmt.Errorf("error opening final csv :: %v", err)
				return
			}

			defer func() {
				finalCsvFile.Close()
			}()

			oracleCtlFile, err := os.Create(filepath.Join(transfer.FinalCsvDir,
				fmt.Sprintf("%v.ctl", fileNum)))
			if err != nil {
				errorChannel <- fmt.Errorf("error creating oracle file :: %v", err)
				return
			}
			defer oracleCtlFile.Close()

			controlFileBuilder := strings.Builder{}

			controlFileBuilder.WriteString(`LOAD DATA CHARACTERSET 'AL32UTF8' infile '`)
			controlFileBuilder.WriteString(filepath.Join(transfer.FinalCsvDir,
				fmt.Sprintf("%v.csv", fileNum)))
			controlFileBuilder.WriteString(`' append into table `)
			controlFileBuilder.WriteString(transfer.TargetSchema)
			controlFileBuilder.WriteString(".")
			controlFileBuilder.WriteString(transfer.TargetTable)
			controlFileBuilder.WriteString(
				` fields csv with embedded terminated by ',' optionally enclosed by '"' (`)

			firstCol := true

			for i, column := range columnInfo {

				if !firstCol {
					controlFileBuilder.WriteString(" ,")
				}

				controlFileBuilder.WriteString(column.name)

				switch column.pipeType {
				case "date":
					controlFileBuilder.WriteString(" date 'YYYY-MM-DD'")
				case "datetime":
					controlFileBuilder.WriteString(" timestamp 'YYYY-MM-DD HH24:MI:SS.FF'")
				case "datetimetz":
					controlFileBuilder.WriteString(
						" timestamp with time zone 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'")
				default:
					maxLen, err := maxColumnByteLength(finalCsvFileName, transfer.Null, i)
					if err != nil {
						errorChannel <- fmt.Errorf("error getting max column length :: %v", err)
						return
					}
					controlFileBuilder.WriteString(" char(")
					controlFileBuilder.WriteString(fmt.Sprint(maxLen))
					controlFileBuilder.WriteString(") PRESERVE BLANKS")
				}

				controlFileBuilder.WriteString(" nullif ")
				controlFileBuilder.WriteString(column.name)
				controlFileBuilder.WriteString("='")
				controlFileBuilder.WriteString(transfer.Null)
				controlFileBuilder.WriteString("'")

				firstCol = false
			}

			controlFileBuilder.WriteString(")")

			// write frontmatter to oracle file
			_, err = oracleCtlFile.WriteString(controlFileBuilder.String())
			if err != nil {
				errorChannel <- fmt.Errorf("error writing oracle ctl file :: %v", err)
				return
			}

			err = oracleCtlFile.Close()
			if err != nil {
				errorChannel <- fmt.Errorf("error closing oracle ctl file :: %v", err)
				return
			}

			finalCsvChannelOut <- finalCsvFileName
		}

		infoLog.Printf("transfer %v finished creating sqllder ctl files", transfer.Id)
	}()

	return finalCsvChannelOut
}

func (system Oracle) insertFinalCsvs(
	ctx context.Context,
	finalCsvChannel <-chan string,
	transfer Transfer,
) (
	err error,
) {
	return insertFinalCsvsCommon(ctx, finalCsvChannel, transfer, system)
}

func (system Oracle) getFinalCsvFormatters() map[string]func(string) (string, error) {
	return map[string]func(string) (string, error){
		"nvarchar": func(v string) (string, error) {
			return v, nil
		},
		"varchar": func(v string) (string, error) {
			return v, nil
		},
		"ntext": func(v string) (string, error) {
			return v, nil
		},
		"text": func(v string) (string, error) {
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
				return "", fmt.Errorf("error writing datetime value to oracle csv :: %v", err)
			}
			return valTime.Format("2006-01-02 15:04:05.999999"), nil
		},
		"datetimetz": func(v string) (string, error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing datetime value to oracle csv :: %v", err)
			}
			return valTime.UTC().Format("2006-01-02 15:04:05.999999 -07:00"), nil
		},
		"date": func(v string) (string, error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to oracle csv :: %v", err)
			}
			return valTime.Format("2006-01-02"), nil
		},
		"time": func(v string) (string, error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing time value to oracle csv :: %v", err)
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
			return strings.Replace(v, "-", "", -1), nil
		},
		"bool": func(v string) (string, error) {
			if v == "1" {
				return "1", nil
			}
			return "0", nil
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

func (system Oracle) runInsertCmd(
	ctx context.Context,
	finalCsvLocation string,
	transfer Transfer,
) (
	err error,
) {

	fileNum, err := getFileNum(finalCsvLocation)
	if err != nil {
		return fmt.Errorf("error getting file number :: %v", err)
	}

	ctlFileName := filepath.Join(transfer.FinalCsvDir, fmt.Sprintf("%v.ctl", fileNum))
	logFileName := filepath.Join(transfer.FinalCsvDir, fmt.Sprintf("%v.log", fileNum))
	badFileName := filepath.Join(transfer.FinalCsvDir, fmt.Sprintf("%v.bad", fileNum))
	discardFileName := filepath.Join(transfer.FinalCsvDir, fmt.Sprintf("%v.discard", fileNum))

	if !transfer.KeepFiles {
		defer os.Remove(logFileName)
		defer os.Remove(badFileName)
		defer os.Remove(discardFileName)
		defer os.Remove(ctlFileName)
	}

	cmd := exec.CommandContext(
		ctx,
		"sqlldr",
		fmt.Sprintf("%s/%s@%s:%d/%s", transfer.TargetUsername, transfer.TargetPassword,
			transfer.TargetHostname, transfer.TargetPort, transfer.TargetDatabase),
		fmt.Sprintf("control=%s", ctlFileName),
		fmt.Sprintf("LOG=%s", logFileName),
		fmt.Sprintf("BAD=%s", badFileName),
		fmt.Sprintf("DISCARD=%s", discardFileName),
	)

	result, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf(
			"failed to upload csv to oracle :: stderr %v :: stdout %s", err, string(result))
	}

	return nil
}
