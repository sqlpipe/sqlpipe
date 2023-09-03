package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

type Mssql struct {
	name             string
	connectionString string
	connection       *sql.DB
}

func newMssql(name, connectionString string) (mssql Mssql, err error) {
	if name == "" {
		name = TypeMSSQL
	}
	db, err := openDbCommon(name, connectionString, DriverMSSQL)
	if err != nil {
		return mssql, fmt.Errorf("error opening mssql db :: %v", err)
	}
	mssql.connection = db
	mssql.name = name
	mssql.connectionString = connectionString
	return mssql, nil
}

func (system Mssql) query(query string) (*sql.Rows, error) {
	rows, err := system.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error running dql on %v :: %v :: %v", system.name, query, err)
	}
	return rows, nil
}

func (system Mssql) exec(query string) error {
	_, err := system.connection.Exec(query)
	if err != nil {
		return fmt.Errorf("error running ddl/dml on %v :: %v :: %v", system.name, query, err)
	}
	return nil
}

func (system Mssql) dropTable(schema, table string) (string, error) {
	return dropTableIfExistsCommon(schema, table, system)
}

func (system Mssql) getColumnInfo(transfer *Transfer) ([]ColumnInfo, error) {
	return getColumnInfoCommon(transfer)
}

func (system Mssql) createPipeFiles(transfer *Transfer, transferErrGroup *errgroup.Group) (<-chan string, error) {
	return createPipeFilesCommon(transfer, transferErrGroup)
}

func (system Mssql) dbTypeToPipeType(databaseType string, columnType sql.ColumnType, transfer *Transfer) (pipeType string, err error) {
	switch columnType.DatabaseTypeName() {
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
		return "", fmt.Errorf("unsupported database type for mssql: %v", columnType.DatabaseTypeName())
	}
}

func (system Mssql) pipeTypeToCreateType(columnInfo ColumnInfo) (createType string, err error) {
	switch columnInfo.pipeType {
	case "nvarchar":
		if columnInfo.lengthOk {
			if columnInfo.length <= 0 {
				return "nvarchar(max)", nil
			} else if columnInfo.length <= 4000 {
				return fmt.Sprintf("nvarchar(%v)", columnInfo.length), nil
			} else {
				return "nvarchar(max)", nil
			}
		} else {
			return "nvarchar(4000)", nil
		}
	case "varchar":
		if columnInfo.lengthOk {
			if columnInfo.length <= 0 {
				return "varchar(max)", nil
			} else if columnInfo.length <= 8000 {
				return fmt.Sprintf("varchar(%v)", columnInfo.length), nil
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

		if columnInfo.decimalOk {
			if columnInfo.scale > 0 && columnInfo.scale <= 38 {
				scaleOk = true
			}

			if columnInfo.precision > 0 && columnInfo.precision <= 38 && columnInfo.precision > columnInfo.scale {
				precisionOk = true
			}
		}

		if scaleOk && precisionOk {
			return fmt.Sprintf("decimal(%v,%v)", columnInfo.precision, columnInfo.scale), nil
		} else {
			return "varchar(64)", nil
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
		if columnInfo.lengthOk {
			if columnInfo.length <= 0 {
				return "varbinary(max)", nil
			} else if columnInfo.length <= 8000 {
				return fmt.Sprintf("varbinary(%v)", columnInfo.length), nil
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
		if columnInfo.lengthOk {
			if columnInfo.length <= 0 {
				return "nvarchar(max)", nil
			} else if columnInfo.length <= 4000 {
				return fmt.Sprintf("nvarchar(%v)", columnInfo.length), nil
			} else {
				return "nvarchar(max)", nil
			}
		} else {
			return "nvarchar(4000)", nil
		}
	case "xml":
		return "xml", nil
	case "varbit":
		if columnInfo.lengthOk {
			if columnInfo.length <= 0 {
				return "varchar(max)", nil
			} else if columnInfo.length <= 8000 {
				return fmt.Sprintf("varchar(%v)", columnInfo.length), nil
			} else {
				return "varchar(max)", nil
			}
		} else {
			return "varchar(8000)", nil
		}
	default:
		return "", fmt.Errorf("unsupported pipeType for mssql: %v", columnInfo.pipeType)
	}
}

func (system Mssql) insertPipeFiles(transfer *Transfer, in <-chan string, transferErrGroup *errgroup.Group) error {
	finalCsvs, err := mssqlConvertPipeFiles(transfer, in, transferErrGroup)
	if err != nil {
		return fmt.Errorf("error converting pipe files :: %v", err)
	}

	err = insertFinalCsvCommon(transfer, finalCsvs)
	if err != nil {
		return fmt.Errorf("error inserting final csvs :: %v", err)
	}

	return nil
}

func mssqlConvertPipeFiles(transfer *Transfer, in <-chan string, transferErrGroup *errgroup.Group) (<-chan string, error) {

	out := make(chan string)

	finalCsvFormatters := transfer.Target.getFinalCsvFormatters()

	transferErrGroup.Go(func() error {
		defer close(out)

		for pipeFileName := range in {

			pipeFileName := pipeFileName
			conversionErrGroup := errgroup.Group{}

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

				bcpCsvFile, err := os.Create(filepath.Join(transfer.FinalCsvDir, fmt.Sprintf("%s.csv", pipeFileNum)))
				if err != nil {
					return fmt.Errorf("error creating bcp csv file :: %v", err)
				}
				defer bcpCsvFile.Close()

				csvReader := csv.NewReader(pipeFile)
				csvBuilder := strings.Builder{}

				var value string

				for {
					row, err := csvReader.Read()
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}
						return fmt.Errorf("error reading csv values in %v :: %v", pipeFile.Name(), err)
					}

					for i := range row {
						if i != 0 {
							csvBuilder.WriteString(transfer.Delimiter)
						}
						if row[i] == transfer.Null {
							csvBuilder.WriteString("")
						} else {
							value, err = finalCsvFormatters[transfer.ColumnInfo[i].pipeType](row[i])
							if err != nil {
								return fmt.Errorf("error formatting pipe file to bcp csv :: %v", err)
							}
							csvBuilder.WriteString(value)
						}
					}
					csvBuilder.WriteString(transfer.Newline)
				}

				err = pipeFile.Close()
				if err != nil {
					return fmt.Errorf("error closing pipe file :: %v", err)
				}

				_, err = bcpCsvFile.WriteString(csvBuilder.String())
				if err != nil {
					return fmt.Errorf("error writing to bcp csv file :: %v", err)
				}

				err = bcpCsvFile.Close()
				if err != nil {
					return fmt.Errorf("error closing bcp csv file :: %v", err)
				}

				out <- bcpCsvFile.Name()

				return nil
			})

			err := conversionErrGroup.Wait()
			if err != nil {
				return fmt.Errorf("error converting pipeFiles :: %v", err)
			}
		}

		infoLog.Printf("transfer %v finished converting pipe files", transfer.Id)
		return nil
	})

	return out, nil
}

func (system Mssql) runUploadCmd(transfer *Transfer, csvFileName string) error {
	hostName := transfer.TargetHostname

	cmd := exec.Command(
		"bcp",
		fmt.Sprintf("%s.%s.%s", transfer.TargetDatabase, transfer.TargetSchema, transfer.TargetTable),
		"in",
		csvFileName,
		"-c",
		"-S", fmt.Sprintf("%s,%d", hostName, transfer.TargetPort),
		"-U", transfer.TargetUsername,
		"-P", transfer.TargetPassword,
		"-t", transfer.Delimiter,
		"-r", transfer.Newline,
		"-e", "/tmp/errors.txt",
	)
	result, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to upload csv to mssql :: stderr %v :: stdout %s", err, string(result))
	}

	return nil
}

func (system Mssql) getPipeFileFormatters() (map[string]func(interface{}) (string, error), error) {
	return map[string]func(interface{}) (string, error){
		"nvarchar": func(v interface{}) (string, error) {
			return fmt.Sprintf("%s", v), nil
		},
		"varchar": func(v interface{}) (string, error) {
			return fmt.Sprintf("%s", v), nil
		},
		"ntext": func(v interface{}) (string, error) {
			return fmt.Sprintf("%s", v), nil
		},
		"text": func(v interface{}) (string, error) {
			return fmt.Sprintf("%s", v), nil
		},
		"int64": func(v interface{}) (string, error) {
			return fmt.Sprintf("%d", v), nil
		},
		"int32": func(v interface{}) (string, error) {
			return fmt.Sprintf("%d", v), nil
		},
		"int16": func(v interface{}) (string, error) {
			return fmt.Sprintf("%d", v), nil
		},
		"float64": func(v interface{}) (string, error) {
			return fmt.Sprintf("%f", v), nil
		},
		"float32": func(v interface{}) (string, error) {
			return fmt.Sprintf("%f", v), nil
		},
		"decimal": func(v interface{}) (string, error) {
			return fmt.Sprintf("%s", v), nil
		},
		"money": func(v interface{}) (string, error) {
			return fmt.Sprintf("%s", v), nil
		},
		"datetime": func(v interface{}) (string, error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New("non time.Time value passed to datetime mssqlPipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"datetimetz": func(v interface{}) (string, error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New("non time.Time value passed to datetimetz mssqlPipeFileFormatters")
			}
			return valTime.UTC().Format(time.RFC3339Nano), nil
		},
		"date": func(v interface{}) (string, error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New("non time.Time value passed to date mssqlPipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"time": func(v interface{}) (string, error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New("non time.Time value passed to time mssqlPipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"varbinary": func(v interface{}) (string, error) {
			return fmt.Sprintf("%x", v), nil
		},
		"blob": func(v interface{}) (string, error) {
			return fmt.Sprintf("%x", v), nil
		},
		"uuid": func(v interface{}) (string, error) {
			val, ok := v.([]uint8)
			if !ok {
				return "", errors.New("non byte array value passed to uuid mssqlPipeFileFormatters")
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
		"bool": func(v interface{}) (string, error) {
			return fmt.Sprintf("%t", v), nil
		},
		"json": func(v interface{}) (string, error) {
			return fmt.Sprintf("%s", v), nil
		},
		"xml": func(v interface{}) (string, error) {
			return fmt.Sprintf("%s", v), nil
		},
	}, nil
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
				return "", fmt.Errorf("error parsing datetimetz value in mssql datetimetz psql formatter :: %v", err)
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
