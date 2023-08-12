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
)

type Mssql struct {
	name             string
	connectionString string
	connection       *sql.DB
}

func newMssql(name, connectionString string) (mssql Mssql, err error) {
	if name == "" {
		name = "mssql"
	}
	db, err := openDbCommon(name, connectionString, "sqlserver")
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

func (system Mssql) dropTable(schema, table string) error {
	return dropTableIfExistsCommon(schema, table, system)
}

func (system Mssql) createTable(schema, table string, columnInfo []ColumnInfo) error {
	return createTableCommon(schema, table, columnInfo, system)
}

func (system Mssql) getColumnInfo(rows *sql.Rows) ([]ColumnInfo, error) {
	return getColumnInfoCommon(rows, system)
}

func (system Mssql) createPipeFiles(transfer Transfer) (pipeFilesDir string, err error) {
	return createPipeFilesCommon(transfer.Rows, transfer.ColumnInfo, transfer.Id, system, transfer.Null)
}

func (system Mssql) dbTypeToPipeType(databaseType string, columnType sql.ColumnType) (pipeType string, err error) {
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
		return "int8", nil
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
	case "int8":
		return "tinyint", nil
	case "float64":
		return "float", nil
	case "float32":
		return "real", nil
	case "decimal":
		if columnInfo.decimalOk {
			if columnInfo.precision > 38 {
				return "", fmt.Errorf("precision on column %v is greater than 38", columnInfo.name)
			}
			return fmt.Sprintf("decimal(%v,%v)", columnInfo.precision, columnInfo.scale), nil
		}
		return "float", nil
	case "money":
		return "money", nil
	case "datetime":
		return "datetime2", nil
	case "datetimetz":
		return "datetimeoffset", nil
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
	default:
		return "", fmt.Errorf("unsupported pipeType for mssql: %v", columnInfo.pipeType)
	}
}

func (system Mssql) insertPipeFiles(transfer Transfer) error {
	pipeFilesDir := filepath.Join(transfer.TmpDir, "pipe-files")

	bcpCsvDirPath := filepath.Join(transfer.TmpDir, "bcp-csv")
	err := os.Mkdir(bcpCsvDirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error creating bcp-csv directory :: %v", err)
	}

	pipeFileEntries, err := os.ReadDir(pipeFilesDir)
	if err != nil {
		return fmt.Errorf("unable to read pipe files dir :: %v", err)
	}

	for _, pipeFileEntry := range pipeFileEntries {
		pipeFile, err := os.Open(filepath.Join(pipeFilesDir, pipeFileEntry.Name()))
		if err != nil {
			return fmt.Errorf("unable to read file %v :: %v", pipeFileEntry.Name(), err)
		}
		defer pipeFile.Close()

		pipeFileNum := strings.Split(pipeFileEntry.Name(), ".")[0]

		bcpCsvFile, err := os.Create(filepath.Join(bcpCsvDirPath, fmt.Sprintf("%s.csv", pipeFileNum)))
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
				return fmt.Errorf("error reading csv values in %v :: %v", pipeFileEntry.Name(), err)
			}

			for i := range row {
				if row[i] == transfer.Null {
					csvBuilder.WriteString("")
					csvBuilder.WriteString(transfer.Delimiter)
				} else {
					value, err = mssqlPipeFileToBcpCsvFormatters[transfer.ColumnInfo[i].pipeType](row[i])
					if err != nil {
						return fmt.Errorf("error formatting pipe file to bcp csv :: %v", err)
					}
					csvBuilder.WriteString(value)
					csvBuilder.WriteString(transfer.Delimiter)
				}
			}

			csvBuilder.WriteString(transfer.Newline)
		}

		_, err = bcpCsvFile.WriteString(csvBuilder.String())
		if err != nil {
			return fmt.Errorf("error writing to bcp csv file :: %v", err)
		}

		err = bcpCsvFile.Close()
		if err != nil {
			return fmt.Errorf("error closing bcp csv file :: %v", err)
		}

		err = pipeFile.Close()
		if err != nil {
			return fmt.Errorf("error closing pipe file :: %v", err)
		}

	}

	infoLog.Printf("wrote bcp csvs to %v", bcpCsvDirPath)

	bcpCsvs, err := os.ReadDir(bcpCsvDirPath)
	if err != nil {
		return fmt.Errorf("unable to read bcp csvs dir :: %v", err)
	}

	for _, f := range bcpCsvs {

		// mycommand := fmt.Sprintf(`\copy %s.%s FROM '%s' WITH (FORMAT csv, HEADER false, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL %v, ENCODING 'UTF8')`, schema, table, filepath.Join(bcpCsvDirPath, f.Name()), null)

		// mycommand := fmt.Sprintf(`bcp %s.%s.%s in %s -c -t"%s" -r"%s" -S %s -U %s -P %s`, transfer.BcpDatabase, transfer.TargetSchema, transfer.TargetTable, filepath.Join(bcpCsvDirPath, f.Name()), transfer.Delimiter, transfer.Newline, transfer.BcpServer, transfer.BcpUsername, transfer.BcpPass)

		cmd := exec.Command(bcpTmpFile.Name(), "in", filepath.Join(bcpCsvDirPath, f.Name()), "-c", "-S", transfer.BcpServer, "-U", transfer.BcpUsername, "-P", transfer.BcpPass, "-d", transfer.BcpDatabase, "-c", "-t", transfer.Delimiter, "-r", transfer.Newline)
		// cmd := exec.Command(mycommand)
		result, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to upload csv to mssql :: stderr %v :: stdout %s", err, string(result))
		}
	}

	infoLog.Printf("inserted bcp csvs into %s.%s", transfer.TargetSchema, transfer.TargetTable)

	return nil
}

func (system Mssql) getPipeFileFormatters() map[string]func(interface{}) (string, error) {
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
				return "", errors.New("non time.Time value passed to datetime mssqlPipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"datetimetz": func(v interface{}) (string, error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New("non time.Time value passed to datetimetz mssqlPipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
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
			return fmt.Sprintf("%x", v), nil
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
	}
}

var mssqlPipeFileToBcpCsvFormatters = map[string]func(string) (string, error){
	"nvarchar": func(v string) (string, error) {
		if v == "" {
			return `""`, nil
		}
		return v, nil
	},
	"varchar": func(v string) (string, error) {
		if v == "" {
			return `""`, nil
		}
		return v, nil
	},
	"ntext": func(v string) (string, error) {
		if v == "" {
			return `""`, nil
		}
		return v, nil
	},
	"text": func(v string) (string, error) {
		if v == "" {
			return `""`, nil
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
		return v, nil
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
}
