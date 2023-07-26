package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"
)

type Mssql struct {
	name       string
	connection *sql.DB
}

func newMssql(name, connectionString string) (Mssql, error) {
	mssql := Mssql{
		name: "mssql",
	}

	db, err := sql.Open("sqlserver", connectionString)
	if err != nil {
		return mssql, fmt.Errorf("error opening connection to %v :: %v", name, err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pingCancel()
	err = db.PingContext(pingCtx)
	if err != nil {
		return mssql, fmt.Errorf("error pinging mssql :: %v", err)
	}

	mssql.connection = db

	return mssql, nil
}

func (system Mssql) dropTable(schema, table string) error {
	query := fmt.Sprintf("drop table if exists %v.%v", schema, table)
	_, err := system.connection.Exec(query)
	if err != nil {
		err = fmt.Errorf("error dropping %v.%v in %v :: %v", schema, table, system.name, err)
	}
	return err
}

func (system Mssql) createTable(schema, table string, columnInfo []ColumnInfo) error {
	var queryBuilder = strings.Builder{}

	queryBuilder.WriteString("create table ")
	queryBuilder.WriteString(schema)
	queryBuilder.WriteString(".")
	queryBuilder.WriteString(table)
	queryBuilder.WriteString(" (")

	for i := range columnInfo {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		queryBuilder.WriteString(columnInfo[i].name)
		queryBuilder.WriteString(" ")

		switch columnInfo[i].databaseType {
		case "nvarchar":
			if columnInfo[i].lengthOk {
				if columnInfo[i].length <= 0 {
					queryBuilder.WriteString("nvarchar(max)")
				} else if columnInfo[i].length <= 4000 {
					queryBuilder.WriteString(fmt.Sprintf("nvarchar(%v)", columnInfo[i].length))
				} else {
					queryBuilder.WriteString("nvarchar(max)")
				}
			} else {
				queryBuilder.WriteString("nvarchar(4000)")
			}
		case "varchar":
			if columnInfo[i].lengthOk {
				if columnInfo[i].length <= 0 {
					queryBuilder.WriteString("varchar(max)")
				} else if columnInfo[i].length <= 8000 {
					queryBuilder.WriteString(fmt.Sprintf("varchar(%v)", columnInfo[i].length))
				} else {
					queryBuilder.WriteString("varchar(max)")
				}
			} else {
				queryBuilder.WriteString("varchar(8000)")
			}
		case "ntext":
			queryBuilder.WriteString("nvarchar(max)")
		case "text":
			queryBuilder.WriteString("varchar(max)")
		case "int64":
			queryBuilder.WriteString("bigint")
		case "int32":
			queryBuilder.WriteString("integer")
		case "int16":
			queryBuilder.WriteString("smallint")
		case "int8":
			queryBuilder.WriteString("tinyint")
		case "float64":
			queryBuilder.WriteString("float")
		case "float32":
			queryBuilder.WriteString("real")
		case "decimal":
			queryBuilder.WriteString("decimal")
			if columnInfo[i].decimalOk {
				if columnInfo[i].precision > 38 {
					return fmt.Errorf("error creating %v.%v in %v :: precision on column %v is greater than 38", schema, table, system.name, columnInfo[i].name)
				}
				queryBuilder.WriteString("(")
				queryBuilder.WriteString(fmt.Sprintf("%v", columnInfo[i].precision))
				queryBuilder.WriteString(", ")
				queryBuilder.WriteString(fmt.Sprintf("%v", columnInfo[i].scale))
				queryBuilder.WriteString(")")
			}
		case "money":
			queryBuilder.WriteString("money")
		case "datetime":
			queryBuilder.WriteString("datetime2")
		case "datetimetz":
			queryBuilder.WriteString("datetimeoffset")
		case "date":
			queryBuilder.WriteString("date")
		case "time":
			queryBuilder.WriteString("time")
		case "varbinary":
			if columnInfo[i].lengthOk {
				if columnInfo[i].length <= 0 {
					queryBuilder.WriteString("varbinary(max)")
				} else if columnInfo[i].length <= 8000 {
					queryBuilder.WriteString(fmt.Sprintf("varbinary(%v)", columnInfo[i].length))
				} else {
					queryBuilder.WriteString("varbinary(max)")
				}
			} else {
				queryBuilder.WriteString("varbinary(8000)")
			}
		case "blob":
			queryBuilder.WriteString("varbinary(max)")
		case "uuid":
			queryBuilder.WriteString("uniqueidentifier")
		case "bool":
			queryBuilder.WriteString("bit")
		case "json":
			if columnInfo[i].lengthOk {
				if columnInfo[i].length <= 0 {
					queryBuilder.WriteString("nvarchar(max)")
				} else if columnInfo[i].length <= 4000 {
					queryBuilder.WriteString(fmt.Sprintf("nvarchar(%v)", columnInfo[i].length))
				} else {
					queryBuilder.WriteString("nvarchar(max)")
				}
			} else {
				queryBuilder.WriteString("nvarchar(4000)")
			}
		case "xml":
			queryBuilder.WriteString("xml")
		default:
			return fmt.Errorf("unsupported pipeType for mssql: %v", columnInfo[i].databaseType)
		}
	}
	queryBuilder.WriteString(")")

	_, err := system.connection.Exec(queryBuilder.String())
	if err != nil {
		return fmt.Errorf("error creating %v.%v in %v :: %v", schema, table, system.name, err)
	}
	return nil
}

func (system Mssql) query(query string) (*sql.Rows, error) {
	rows, err := system.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying %v :: %v", system.name, err)
	}
	return rows, nil
}

func (system Mssql) getColumnInfo(rows *sql.Rows) ([]ColumnInfo, error) {
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
		scanType := getScanType(columnTypes[i])

		var databaseType string

		switch columnTypes[i].DatabaseTypeName() {
		case "NVARCHAR":
			databaseType = "nvarchar"
		case "NCHAR":
			databaseType = "nvarchar"
		case "VARCHAR":
			databaseType = "varchar"
		case "CHAR":
			databaseType = "varchar"
		case "NTEXT":
			databaseType = "ntext"
		case "TEXT":
			databaseType = "text"
		case "BIGINT":
			databaseType = "int64"
		case "INT":
			databaseType = "int32"
		case "SMALLINT":
			databaseType = "int16"
		case "TINYINT":
			databaseType = "int8"
		case "FLOAT":
			databaseType = "float64"
		case "REAL":
			databaseType = "float32"
		case "DECIMAL":
			databaseType = "decimal"
		case "MONEY":
			databaseType = "money"
		case "SMALLMONEY":
			databaseType = "money"
		case "DATETIME2":
			databaseType = "datetime"
		case "DATETIME":
			databaseType = "datetime"
		case "SMALLDATETIME":
			databaseType = "datetime"
		case "DATETIMEOFFSET":
			databaseType = "datetimetz"
		case "DATE":
			databaseType = "date"
		case "TIME":
			databaseType = "time"
		case "BINARY":
			databaseType = "varbinary"
		case "IMAGE":
			databaseType = "blob"
		case "VARBINARY":
			databaseType = "blob"
		case "UNIQUEIDENTIFIER":
			databaseType = "uuid"
		case "BIT":
			databaseType = "bool"
		case "XML":
			databaseType = "xml"
		default:
			return columnInfo, fmt.Errorf("unsupported database type for mssql: %v", columnTypes[i].DatabaseTypeName())
		}

		columnInfo = append(columnInfo, ColumnInfo{
			name:         columnNames[i],
			databaseType: databaseType,
			scanType:     scanType,
			decimalOk:    decimalOk,
			precision:    precision,
			scale:        scale,
			lengthOk:     lengthOk,
			length:       length,
			nullableOk:   nullableOk,
			nullable:     nullable,
		})
	}

	return columnInfo, nil
}

func (system Mssql) writeCsv(rows *sql.Rows, columnInfo []ColumnInfo, transferId string) (tmpDir string, err error) {
	if bcpAvailable {
		rows.Close()
		return system.writeCsvWithBcp(rows, columnInfo)
	}

	tmpDir, err = os.MkdirTemp("", transferId)
	if err != nil {
		return "", fmt.Errorf("error creating temp dir :: %v", err)
	}

	tmpFile, err := os.CreateTemp(tmpDir, "")
	if err != nil {
		return "", fmt.Errorf("error creating temp file :: %v", err)
	}

	csvWriter := csv.NewWriter(tmpFile)

	values := make([]interface{}, len(columnInfo))
	valuePtrs := make([]interface{}, len(columnInfo))
	for i := range columnInfo {
		valuePtrs[i] = &values[i]
	}

	for i := 0; rows.Next(); i++ {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return "", fmt.Errorf("error scanning row :: %v", err)
		}

		row := make([]string, len(columnInfo))
		for j := range columnInfo {
			if values[j] == nil {
				row[j] = "\u0000"
			} else {
				row[j] = fmt.Sprint(values[j])
			}
		}

		if i%999 == 0 {
			csvWriter.Flush()
			err = csvWriter.Error()
			if err != nil {
				return "", fmt.Errorf("error flushing csv :: %v", err)
			}

			tmpFile, err = os.CreateTemp(tmpDir, "")
			if err != nil {
				return "", fmt.Errorf("error creating temp file :: %v", err)
			}
			csvWriter = csv.NewWriter(tmpFile)
		}

		err = csvWriter.Write(row)
		if err != nil {
			return "", fmt.Errorf("error writing row :: %v", err)
		}
	}

	csvWriter.Flush()
	err = csvWriter.Error()
	if err != nil {
		return "", fmt.Errorf("error flushing csv :: %v", err)
	}

	infoLog.Println(tmpDir)

	return tmpDir, nil
}

var mssqlFormatters = map[string]func(interface{}) string{
	"nvarchar": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"varchar": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"ntext": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"text": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"int64": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"int32": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"int16": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"int8": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"float64": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"float32": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"decimal": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"money": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"datetime": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"datetimetz": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"date": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"time": func(v interface{}) string {
		val, ok := v.(time.Time)
		if !ok {
			panic("non-time.Time value passed to mssql time writer")
		}
		return val.Format("15:04:05.9999999")
	},
	"varbinary": func(v interface{}) string {
		val, ok := v.([]byte)
		if !ok {
			panic("non-[]byte value passed to mssql varbinary writer")
		}
		return fmt.Sprintf("%v", val)
	},
	"blob": func(v interface{}) string {
		val, ok := v.([]byte)
		if !ok {
			panic("non-[]byte value passed to mssql blob writer")
		}
		return fmt.Sprintf("%v", val)
	},
	"uuid": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"bool": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"json": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
	"xml": func(v interface{}) string {
		return fmt.Sprintf("%v", v)
	},
}

func (system Mssql) writeCsvWithBcp(rows *sql.Rows, columnInfo []ColumnInfo) (tmpDir string, err error) {
	return "", nil
}
