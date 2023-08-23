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

type Oracle struct {
	name             string
	connectionString string
	connection       *sql.DB
}

func newOracle(name, connectionString string) (oracle Oracle, err error) {
	if name == "" {
		name = "oracle"
	}
	db, err := openDbCommon(name, connectionString, "oracle")
	if err != nil {
		return oracle, fmt.Errorf("error opening oracle db :: %v", err)
	}
	oracle.connection = db
	oracle.name = name
	oracle.connectionString = connectionString
	return oracle, nil
}

func (system Oracle) query(query string) (*sql.Rows, error) {
	rows, err := system.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error running dql on %v :: %v :: %v", system.name, query, err)
	}
	return rows, nil
}

func (system Oracle) exec(query string) error {
	_, err := system.connection.Exec(query)
	if err != nil {
		return fmt.Errorf("error running ddl/dml on %v :: %v :: %v", system.name, query, err)
	}
	return nil
}

func (system Oracle) dropTable(schema, table string) error {

	dropped := true

	query := fmt.Sprintf("drop table %v.%v", schema, table)
	err := system.exec(query)
	if err != nil {
		if !strings.Contains(err.Error(), "ORA-00942") {
			return fmt.Errorf("error dropping %v.%v :: %v", schema, table, err)
		}
		dropped = false
		infoLog.Printf("table %v.%v does not exist in oracle", schema, table)
	}

	if dropped {
		infoLog.Printf("dropped %v.%v", schema, table)
	}

	return nil
}

func (system Oracle) createTable(transfer *Transfer) error {
	return createTableCommon(transfer)
}

func (system Oracle) getColumnInfo(transfer *Transfer) ([]ColumnInfo, error) {
	return getColumnInfoCommon(transfer)
}

func (system Oracle) createPipeFiles(transfer *Transfer, transferErrGroup *errgroup.Group) (<-chan string, error) {
	return createPipeFilesCommon(transfer, transferErrGroup)
}

func (system Oracle) dbTypeToPipeType(databaseType string, columnType sql.ColumnType, transfer *Transfer) (pipeType string, err error) {
	switch columnType.DatabaseTypeName() {
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
		return "", fmt.Errorf("unsupported database type for oracle: %v", columnType.DatabaseTypeName())
	}

	return pipeType, nil
}

func (system Oracle) pipeTypeToCreateType(columnInfo ColumnInfo) (createType string, err error) {
	switch columnInfo.pipeType {
	case "nvarchar":
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
	case "varchar":
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
	case "ntext":
		createType = "clob"
	case "text":
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

func (system Oracle) insertPipeFiles(transfer *Transfer, in <-chan string, transferErrGroup *errgroup.Group) error {

	oracleFiles, err := oracleConvertPipeFiles(transfer, in, transferErrGroup)
	if err != nil {
		return fmt.Errorf("error converting pipe files :: %v", err)
	}

	err = oracleInsertFiles(transfer, oracleFiles)
	if err != nil {
		return fmt.Errorf("error inserting oracle files :: %v", err)
	}
	return nil
}

func oracleConvertPipeFiles(transfer *Transfer, in <-chan string, transferErrGroup *errgroup.Group) (<-chan string, error) {
	out := make(chan string)

	oracleFileDirPath := filepath.Join(transfer.TmpDir, "oracle")
	err := os.Mkdir(oracleFileDirPath, os.ModePerm)
	if err != nil {
		return out, fmt.Errorf("error creating oracle directory :: %v", err)
	}

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

				oracleCtlFile, err := os.Create(filepath.Join(oracleFileDirPath, fmt.Sprintf("%s.ctl", pipeFileNum)))
				if err != nil {
					return fmt.Errorf("error creating oracle file :: %v", err)
				}
				defer oracleCtlFile.Close()

				controlFileBuilder := strings.Builder{}

				controlFileBuilder.WriteString("LOAD DATA infile '")
				controlFileBuilder.WriteString(filepath.Join(oracleFileDirPath, fmt.Sprintf("%s.csv", pipeFileNum)))
				controlFileBuilder.WriteString(`' append into table `)
				controlFileBuilder.WriteString(transfer.TargetSchema)
				controlFileBuilder.WriteString(".")
				controlFileBuilder.WriteString(transfer.TargetTable)
				controlFileBuilder.WriteString(` fields csv with embedded terminated by ',' optionally enclosed by '"' trailing nullcols (`)

				firstCol := true
				for _, column := range transfer.ColumnInfo {

					if !firstCol {
						controlFileBuilder.WriteString(",")
					}

					controlFileBuilder.WriteString(column.name)

					switch column.pipeType {
					case "date":
						controlFileBuilder.WriteString(" date 'YYYY-MM-DD'")
					case "datetime":
						controlFileBuilder.WriteString(" timestamp 'YYYY-MM-DD HH24:MI:SS.FF'")
					case "datetimetz":
						controlFileBuilder.WriteString(" timestamp with time zone 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'")
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
					return fmt.Errorf("error writing oracle ctl file :: %v", err)
				}

				err = oracleCtlFile.Close()
				if err != nil {
					return fmt.Errorf("error closing oracle ctl file :: %v", err)
				}

				oracleCsvFile, err := os.Create(filepath.Join(oracleFileDirPath, fmt.Sprintf("%s.csv", pipeFileNum)))
				if err != nil {
					return fmt.Errorf("error creating oracle csv file :: %v", err)
				}
				defer oracleCtlFile.Close()

				csvReader := csv.NewReader(pipeFile)
				csvWriter := csv.NewWriter(oracleCsvFile)

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
							row[i], err = oraclePipeFileToCsvFormatters[transfer.ColumnInfo[i].pipeType](row[i])
							if err != nil {
								return fmt.Errorf("error formatting pipe file to oracle csv :: %v", err)
							}
						}
					}

					err = csvWriter.Write(row)
					if err != nil {
						return fmt.Errorf("error writing oracle csv :: %v", err)
					}
				}

				err = pipeFile.Close()
				if err != nil {
					return fmt.Errorf("error closing pipeFile :: %v", err)
				}

				csvWriter.Flush()

				err = oracleCsvFile.Close()
				if err != nil {
					return fmt.Errorf("error closing oracle csv file :: %v", err)
				}

				out <- oracleCsvFile.Name()

				return nil
			})

			err = conversionErrGroup.Wait()
			if err != nil {
				return fmt.Errorf("error converting pipeFiles :: %v", err)
			}

		}

		infoLog.Printf("converted pipe files to oracle csvs at %v\n", oracleFileDirPath)
		return nil
	})

	return out, nil
}

func oracleInsertFiles(transfer *Transfer, in <-chan string) error {
	insertErrGroup := errgroup.Group{}

	oracleFileDirPath := filepath.Join(transfer.TmpDir, "oracle")

	for oracleCsvFileName := range in {
		oracleCsvFileName := oracleCsvFileName

		insertErrGroup.Go(func() error {

			csvFileNameClean := filepath.Base(oracleCsvFileName)
			csvFileNum := strings.Split(csvFileNameClean, ".")[0]

			ctlFileName := filepath.Join(oracleFileDirPath, fmt.Sprintf("%s.ctl", csvFileNum))
			logFileName := filepath.Join(oracleFileDirPath, fmt.Sprintf("%s.log", csvFileNum))
			badFileName := filepath.Join(oracleFileDirPath, fmt.Sprintf("%s.bad", csvFileNum))
			discardFileName := filepath.Join(oracleFileDirPath, fmt.Sprintf("%s.discard", csvFileNum))

			if !transfer.KeepFiles {
				defer os.Remove(oracleCsvFileName)
				defer os.Remove(ctlFileName)
			}

			cmd := exec.Command(
				"sqlldr",
				fmt.Sprintf("%s/%s@%s:%d/%s", transfer.TargetUsername, transfer.TargetPassword, transfer.TargetHostname, transfer.TargetPort, transfer.TargetDatabase),
				fmt.Sprintf("control=%s", ctlFileName),
				fmt.Sprintf("LOG=%s", logFileName),
				fmt.Sprintf("BAD=%s", badFileName),
				fmt.Sprintf("DISCARD=%s", discardFileName),
			)

			result, err := cmd.CombinedOutput()
			if err != nil {
				return fmt.Errorf("failed to upload csv to oracle :: stderr %v :: stdout %s", err, string(result))
			}

			if !transfer.KeepFiles {
				defer os.Remove(logFileName)
				defer os.Remove(badFileName)
				defer os.Remove(discardFileName)
			}

			return nil
		})
		err := insertErrGroup.Wait()
		if err != nil {
			return fmt.Errorf("error inserting oracle csvs :: %v", err)
		}
	}

	infoLog.Printf("finished inserting oracle csvs for transfer %v\n", transfer.Id)

	return nil
}

func (system Oracle) getPipeFileFormatters() (map[string]func(interface{}) (string, error), error) {
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
				return "", errors.New("non time.Time value passed to datetime oraclePipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"datetimetz": func(v interface{}) (string, error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New("non time.Time value passed to datetimetz oraclePipeFileFormatters")
			}
			return valTime.UTC().Format(time.RFC3339Nano), nil
		},
		"date": func(v interface{}) (string, error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New("non time.Time value passed to date oraclePipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"time": func(v interface{}) (string, error) {
			return fmt.Sprintf("%s", v), nil
		},
		"varbinary": func(v interface{}) (string, error) {
			return fmt.Sprintf("%x", v), nil
		},
		"blob": func(v interface{}) (string, error) {
			return fmt.Sprintf("%x", v), nil
		},
		"uuid": func(v interface{}) (string, error) {
			return fmt.Sprintf("%s", v), nil
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

var oraclePipeFileToCsvFormatters = map[string]func(string) (string, error){
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
