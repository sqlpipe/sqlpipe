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

type Postgresql struct {
	name             string
	connectionString string
	connection       *sql.DB
}

func newPostgresql(name, connectionString string) (postgresql Postgresql, err error) {
	if name == "" {
		name = "postgresql"
	}
	db, err := openDbCommon(name, connectionString, "pgx")
	if err != nil {
		return postgresql, fmt.Errorf("error opening postgresql db :: %v", err)
	}
	postgresql.connection = db
	postgresql.name = name
	postgresql.connectionString = connectionString
	return postgresql, nil
}

func (system Postgresql) dropTable(schema, table string) error {
	return dropTableIfExistsCommon(schema, table, system)
}

func (system Postgresql) createTable(schema, table string, columnInfo []ColumnInfo) error {
	return createTableCommon(schema, table, columnInfo, system)
}

func (system Postgresql) query(query string) (*sql.Rows, error) {
	rows, err := system.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error running dql on %v :: %v :: %v", system.name, query, err)
	}
	return rows, nil
}

func (system Postgresql) exec(query string) error {
	_, err := system.connection.Exec(query)
	if err != nil {
		return fmt.Errorf("error running ddl/dml on %v :: %v :: %v", system.name, query, err)
	}
	return nil
}

func (system Postgresql) getColumnInfo(rows *sql.Rows) ([]ColumnInfo, error) {
	return getColumnInfoCommon(rows, system)
}

func (system Postgresql) createPipeFiles(transfer *Transfer, transferErrGroup *errgroup.Group) (out <-chan string, err error) {
	return createPipeFilesCommon(transfer, transferErrGroup)
}

func (system Postgresql) dbTypeToPipeType(databaseType string, columnType sql.ColumnType) (pipeType string, err error) {
	switch columnType.DatabaseTypeName() {
	case "VARCHAR":
		return "nvarchar", nil
	case "BPCHAR":
		return "nvarchar", nil
	case "TEXT":
		return "ntext", nil
	case "INT8":
		return "int64", nil
	case "INT4":
		return "int32", nil
	case "INT2":
		return "int16", nil
	case "FLOAT8":
		return "float64", nil
	case "FLOAT4":
		return "float32", nil
	case "NUMERIC":
		return "decimal", nil
	case "TIMESTAMP":
		return "datetime", nil
	case "TIMESTAMPTZ":
		return "datetimetz", nil
		// return "datetime", nil
	case "DATE":
		return "date", nil
	case "INTERVAL":
		return "nvarchar", nil
	case "TIME":
		return "time", nil
	case "BYTEA":
		return "blob", nil
	case "UUID":
		return "uuid", nil
	case "BOOL":
		return "bool", nil
	case "JSON":
		return "json", nil
	case "JSONB":
		return "json", nil
	case "142":
		return "xml", nil
	case "BIT":
		return "varbit", nil
	case "VARBIT":
		return "varbit", nil
	case "BOX":
		return "nvarchar", nil
	case "CIRCLE":
		return "nvarchar", nil
	case "LINE":
		return "nvarchar", nil
	case "PATH":
		return "nvarchar", nil
	case "POINT":
		return "nvarchar", nil
	case "POLYGON":
		return "nvarchar", nil
	case "LSEG":
		return "nvarchar", nil
	case "INET":
		return "nvarchar", nil
	case "MACADDR":
		return "nvarchar", nil
	case "1266":
		return "nvarchar", nil
	case "774":
		return "nvarchar", nil
	case "CIDR":
		return "nvarchar", nil
	case "3220":
		return "nvarchar", nil
	case "5038":
		return "nvarchar", nil
	case "3615":
		return "nvarchar", nil
	case "3614":
		return "nvarchar", nil
	case "2970":
		return "nvarchar", nil
	default:
		return "", fmt.Errorf("unsupported database type for postgresql: %v", columnType.DatabaseTypeName())
	}
}

func (system Postgresql) pipeTypeToCreateType(columnInfo ColumnInfo) (createType string, err error) {
	switch columnInfo.pipeType {
	case "nvarchar":
		return "text", nil
	case "varchar":
		return "text", nil
	case "ntext":
		return "text", nil
	case "text":
		return "text", nil
	case "int64":
		return "bigint", nil
	case "int32":
		return "integer", nil
	case "int16":
		return "smallint", nil
	case "int8":
		return "smallint", nil
	case "float64":
		return "double precision", nil
	case "float32":
		return "float", nil
	case "decimal":
		if columnInfo.decimalOk {
			if columnInfo.precision > 1000 {
				return "", fmt.Errorf("precision on column %v is greater than 1000", columnInfo.name)
			}
			return fmt.Sprintf("decimal(%v,%v)", columnInfo.precision, columnInfo.scale), nil
		}
		return "decimal", nil
	case "money":
		return "money", nil
	case "datetime":
		return "timestamp", nil
	case "datetimetz":
		return "timestamp", nil
	case "date":
		return "date", nil
	case "time":
		return "time", nil
	case "varbinary":
		return "bytea", nil
	case "blob":
		return "bytea", nil
	case "uuid":
		return "uuid", nil
	case "bool":
		return "boolean", nil
	case "json":
		return "jsonb", nil
	case "xml":
		return "xml", nil
	case "varbit":
		return "varbit", nil
	default:
		return "", fmt.Errorf("unsupported pipeType for postgresql: %v", columnInfo.pipeType)
	}
}

func (system Postgresql) getPipeFileFormatters() (map[string]func(interface{}) (string, error), error) {
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
		"int8": func(v interface{}) (string, error) {
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
				return "", errors.New("non time.Time value passed to datetime postgresqlPipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"datetimetz": func(v interface{}) (string, error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New("non time.Time value passed to datetimetz postgresqlPipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"date": func(v interface{}) (string, error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New("non time.Time value passed to date postgresqlPipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"time": func(v interface{}) (string, error) {
			timeString, ok := v.(string)
			if !ok {
				return "", errors.New("unable to cast value to string in postgresqlPipeFileFormatters")
			}

			timeVal, err := time.Parse("15:04:05.000000", timeString)
			if err != nil {
				return "", errors.New("error parsing time value in postgresqlPipeFileFormatters")
			}

			return timeVal.Format(time.RFC3339Nano), nil
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
		"varbit": func(v interface{}) (string, error) {
			return fmt.Sprintf("%s", v), nil
		},
	}, nil
}

func (system Postgresql) insertPipeFiles(transfer *Transfer, in <-chan string, transferErrGroup *errgroup.Group) error {
	psqlFiles, err := postgresqlConvertPipeFiles(transfer, in, transferErrGroup)
	if err != nil {
		return fmt.Errorf("error converting pipeFiles :: %v", err)
	}

	err = postgresqlInsertPsqlCsvs(transfer, psqlFiles)
	if err != nil {
		return fmt.Errorf("error inserting pipeFiles :: %v", err)
	}

	return nil
}

func postgresqlConvertPipeFiles(transfer *Transfer, in <-chan string, transferErrGroup *errgroup.Group) (<-chan string, error) {

	out := make(chan string)

	psqlCsvDirPath := filepath.Join(transfer.TmpDir, "psql-csv")
	err := os.Mkdir(psqlCsvDirPath, os.ModePerm)
	if err != nil {
		return out, fmt.Errorf("error creating psql-csv directory :: %v", err)
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

				// strip path from pipeFile name, get number
				pipeFileNameClean := filepath.Base(pipeFileName)
				pipeFileNum := strings.Split(pipeFileNameClean, ".")[0]

				psqlCsvFile, err := os.Create(filepath.Join(psqlCsvDirPath, fmt.Sprintf("%s.csv", pipeFileNum)))
				if err != nil {
					return fmt.Errorf("error creating psql csv file :: %v", err)
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
							row[i], err = postgresqlPipeFileToPsqlCsvFormatters[transfer.ColumnInfo[i].pipeType](row[i])
							if err != nil {
								return fmt.Errorf("error formatting pipe file to psql csv :: %v", err)
							}
						}
					}

					err = csvWriter.Write(row)
					if err != nil {
						return fmt.Errorf("error writing psql csv :: %v", err)
					}
				}

				err = pipeFile.Close()
				if err != nil {
					return fmt.Errorf("error closing pipeFile :: %v", err)
				}

				// conversionErrGroup.Go(func() error {
				// 	err := os.Remove(pipeFileName)
				// 	if err != nil {
				// 		return fmt.Errorf("error removing pipeFile :: %v", err)
				// 	}
				// 	return nil
				// })

				csvWriter.Flush()

				err = psqlCsvFile.Close()
				if err != nil {
					return fmt.Errorf("error closing psql csv file :: %v", err)
				}

				out <- psqlCsvFile.Name()

				return nil
			})

			err = conversionErrGroup.Wait()
			if err != nil {
				return fmt.Errorf("error converting pipeFiles :: %v", err)
			}
		}

		infoLog.Printf("converted pipe files to psql csvs at %v\n", psqlCsvDirPath)
		return nil
	})

	return out, nil
}

func postgresqlInsertPsqlCsvs(transfer *Transfer, in <-chan string) error {

	insertErrGroup := errgroup.Group{}

	for psqlCsvFileName := range in {
		copyCmd := fmt.Sprintf(`\copy %s.%s FROM '%s' WITH (FORMAT csv, HEADER false, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL '%v', ENCODING 'UTF8')`, transfer.TargetSchema, transfer.TargetTable, psqlCsvFileName, transfer.Null)

		cmd := exec.Command(psqlTmpFile.Name(), transfer.TargetConnectionString, "-c", copyCmd)

		result, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to upload csv to postgresql :: stderr %v :: stdout %s", err, string(result))
		}

		// psqlCsvFileName := psqlCsvFileName
		// insertErrGroup.Go(func() error {
		// 	err = os.Remove(psqlCsvFileName)
		// 	if err != nil {
		// 		return fmt.Errorf("error removing psql csv :: %v", err)
		// 	}
		// 	return nil
		// })
	}

	infoLog.Printf("finished inserting psql csvs into for transfer %v\n", transfer.Id)

	err := insertErrGroup.Wait()
	if err != nil {
		return fmt.Errorf("error inserting psql csvs :: %v", err)
	}

	return nil
}

var postgresqlPipeFileToPsqlCsvFormatters = map[string]func(string) (string, error){
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
	"int8": func(v string) (string, error) {
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
		return v, nil
	},
	"datetimetz": func(v string) (string, error) {
		valTime, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return "", fmt.Errorf("error parsing datetimetz value in postgresql datetimetz psql formatter :: %v", err)
		}

		return valTime.UTC().Format(time.RFC3339Nano), nil
	},
	"date": func(v string) (string, error) {
		valTime, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return "", fmt.Errorf("error writing date value to psql csv :: %v", err)
		}
		return valTime.Format("2006-01-02"), nil
	},
	"time": func(v string) (string, error) {
		valTime, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return "", fmt.Errorf("error writing time value to psql csv :: %v", err)
		}

		return valTime.Format("15:04:05.999999"), nil
	},
	"varbinary": func(v string) (string, error) {
		return fmt.Sprintf(`\x%s`, v), nil
	},
	"blob": func(v string) (string, error) {
		return fmt.Sprintf(`\x%s`, v), nil
	},
	"uuid": func(v string) (string, error) {
		return v, nil
	},
	"bool": func(v string) (string, error) {
		return v, nil
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
