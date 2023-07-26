package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type Postgresql struct {
	name       string
	connection *sql.DB
}

func newPostgresql(name, connectionString string) (Postgresql, error) {

	postgresql := Postgresql{
		name: name,
	}

	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		return postgresql, fmt.Errorf("error opening connection to %v :: %v", name, err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pingCancel()
	err = db.PingContext(pingCtx)
	if err != nil {
		return postgresql, fmt.Errorf("error pinging %v :: %v", name, err)
	}

	postgresql.connection = db

	return postgresql, nil
}

func (system Postgresql) dropTable(schema, table string) error {
	query := fmt.Sprintf("drop table if exists %v.%v", schema, table)
	_, err := system.connection.Exec(query)
	if err != nil {
		err = fmt.Errorf("error dropping %v.%v in %v :: %v", schema, table, system.name, err)
	}
	return err
}

func (system Postgresql) createTable(schema, table string, columnInfo []ColumnInfo) error {
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
			queryBuilder.WriteString("text")
		case "varchar":
			queryBuilder.WriteString("text")
		case "ntext":
			queryBuilder.WriteString("text")
		case "text":
			queryBuilder.WriteString("text")
		case "int64":
			queryBuilder.WriteString("bigint")
		case "int32":
			queryBuilder.WriteString("integer")
		case "int16":
			queryBuilder.WriteString("smallint")
		case "int8":
			queryBuilder.WriteString("smallint")
		case "float64":
			queryBuilder.WriteString("double precision")
		case "float32":
			queryBuilder.WriteString("float")
		case "decimal":
			queryBuilder.WriteString("decimal")
			if columnInfo[i].decimalOk {
				if columnInfo[i].precision > 1000 {
					return fmt.Errorf("error creating %v.%v in %v :: precision on column %v is greater than 1000", schema, table, system.name, columnInfo[i].name)
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
			queryBuilder.WriteString("timestamp")
		case "datetimetz":
			queryBuilder.WriteString("timestamptz")
		case "date":
			queryBuilder.WriteString("date")
		case "time":
			queryBuilder.WriteString("time")
		case "varbinary":
			queryBuilder.WriteString("bytea")
		case "blob":
			queryBuilder.WriteString("bytea")
		case "uuid":
			queryBuilder.WriteString("uuid")
		case "bool":
			queryBuilder.WriteString("boolean")
		case "json":
			queryBuilder.WriteString("jsonb")
		case "xml":
			queryBuilder.WriteString("xml")
		default:
			return fmt.Errorf("unsupported pipeType for postgresql: %v", columnInfo[i].databaseType)
		}
	}
	queryBuilder.WriteString(")")

	_, err := system.connection.Exec(queryBuilder.String())
	if err != nil {
		return fmt.Errorf("error creating %v.%v in %v :: %v", schema, table, system.name, err)
	}
	return nil
}

func (system Postgresql) query(query string) (*sql.Rows, error) {
	rows, err := system.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying %v :: %v", system.name, err)
	}
	return rows, nil
}

func (system Postgresql) getColumnInfo(rows *sql.Rows) ([]ColumnInfo, error) {
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
		case "VARCHAR":
			databaseType = "nvarchar"
		case "BPCHAR":
			databaseType = "nvarchar"
		case "TEXT":
			databaseType = "ntext"
		case "INT8":
			databaseType = "int64"
		case "INT4":
			databaseType = "int32"
		case "INT2":
			databaseType = "int16"
		case "FLOAT8":
			databaseType = "float64"
		case "FLOAT4":
			databaseType = "float32"
		case "NUMERIC":
			databaseType = "decimal"
		case "TIMESTAMP":
			databaseType = "datetime"
		case "TIMESTAMPTZ":
			databaseType = "datetimetz"
		case "DATE":
			databaseType = "date"
		case "INTERVAL":
			databaseType = "nvarchar"
		case "TIME":
			databaseType = "time"
		case "BYTEA":
			databaseType = "blob"
		case "UUID":
			databaseType = "uuid"
		case "BOOL":
			databaseType = "bool"
		case "JSON":
			databaseType = "json"
		case "JSONB":
			databaseType = "json"
		case "142":
			databaseType = "xml"
		case "BIT":
			databaseType = "nvarchar"
		case "VARBIT":
			databaseType = "nvarchar"
		case "BOX":
			databaseType = "nvarchar"
		case "CIRCLE":
			databaseType = "nvarchar"
		case "LINE":
			databaseType = "nvarchar"
		case "PATH":
			databaseType = "nvarchar"
		case "POINT":
			databaseType = "nvarchar"
		case "POLYGON":
			databaseType = "nvarchar"
		case "LSEG":
			databaseType = "nvarchar"
		case "INET":
			databaseType = "nvarchar"
		case "MACADDR":
			databaseType = "nvarchar"
		case "1266":
			databaseType = "nvarchar"
		case "774":
			databaseType = "nvarchar"
		case "CIDR":
			databaseType = "nvarchar"
		case "3220":
			databaseType = "nvarchar"
		case "5038":
			databaseType = "nvarchar"
		case "3615":
			databaseType = "nvarchar"
		case "3614":
			databaseType = "nvarchar"
		case "2970":
			databaseType = "nvarchar"
		default:
			return columnInfo, fmt.Errorf("unsupported database type for postgresql: %v", columnTypes[i].DatabaseTypeName())
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

func (system Postgresql) writeCsv(rows *sql.Rows, columnInfo []ColumnInfo, transferId string) (tmpDir string, err error) {
	return "", nil
}
