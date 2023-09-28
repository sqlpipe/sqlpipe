package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

type Postgresql struct {
	name             string
	connectionString string
	connection       *sql.DB
}

func newPostgresql(name, connectionString string) (postgresql Postgresql, err error) {
	if name == "" {
		name = TypePostgreSQL
	}
	db, err := openDbCommon(name, connectionString, DriverPostgreSQL)
	if err != nil {
		return postgresql, fmt.Errorf("error opening postgresql db :: %v", err)
	}
	postgresql.connection = db
	postgresql.name = name
	postgresql.connectionString = connectionString
	return postgresql, nil
}

func (system Postgresql) query(query string) (rows *sql.Rows, err error) {
	rows, err = system.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error running dql on %v :: %v :: %v", system.name, query, err)
	}
	return rows, nil
}

func (system Postgresql) exec(query string) (err error) {
	_, err = system.connection.Exec(query)
	if err != nil {
		return fmt.Errorf("error running ddl/dml on %v :: %v :: %v", system.name, query, err)
	}
	return nil
}

func (system Postgresql) dropTableIfExists(transfer Transfer) (dropped string, err error) {
	return dropTableIfExistsCommon(transfer, system)
}

func (system Postgresql) getColumnInfo(rows *sql.Rows) (columnInfo []ColumnInfo, err error) {
	return getColumnInfoCommon(rows, system)
}

func (system Postgresql) createTable(
	columnInfo []ColumnInfo,
	transfer Transfer,
) (
	created string,
	err error,
) {
	return createTableCommon(columnInfo, transfer, system)
}

func (system Postgresql) dbTypeToPipeType(
	columnType *sql.ColumnType,
	databaseTypeName string,
) (
	pipeType string,
	err error,
) {
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

func (system Postgresql) pipeTypeToCreateType(
	columnInfo ColumnInfo,
	transfer Transfer,
) (
	createType string,
	err error,
) {
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
	case "float64":
		return "double precision", nil
	case "float32":
		return "float", nil
	case "decimal":
		scaleOk := false
		precisionOk := false

		if columnInfo.decimalOk {
			if columnInfo.scale > 0 && columnInfo.scale <= 1000 {
				scaleOk = true
			}

			if columnInfo.precision > 0 && columnInfo.precision <= 1000 && columnInfo.precision > columnInfo.scale {
				precisionOk = true
			}
		}

		if scaleOk && precisionOk {
			return fmt.Sprintf("decimal(%v,%v)", columnInfo.precision, columnInfo.scale), nil
		} else {
			return "decimal", nil
		}
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

func (system Postgresql) createPipeFiles(
	ctx context.Context,
	errorChannel chan<- error,
	columnInfo []ColumnInfo,
	transfer Transfer,
	rows *sql.Rows,
) <-chan string {
	return createPipeFilesCommon(ctx, errorChannel, columnInfo, transfer, rows, system)
}

func (system Postgresql) getPipeFileFormatters() (
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
				return "", errors.New("non time.Time value passed to datetime postgresqlPipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"datetimetz": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New("non time.Time value passed to datetimetz postgresqlPipeFileFormatters")
			}
			return valTime.UTC().Format(time.RFC3339Nano), nil
		},
		"date": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New("non time.Time value passed to date postgresqlPipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"time": func(v interface{}) (pipeFileValue string, err error) {
			timeString, ok := v.(string)
			if !ok {
				return "", errors.New("unable to cast value to string in postgresqlPipeFileFormatters")
			}

			timeVal, err := time.Parse("15:04:05.999999", timeString)
			if err != nil {
				return "", fmt.Errorf("error parsing time value %v in postgresqlPipeFileFormatters", timeString)
			}

			return timeVal.Format(time.RFC3339Nano), nil
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
		"varbit": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
	}
}

func (system Postgresql) insertPipeFiles(
	ctx context.Context,
	pipeFileChannel <-chan string,
	errorChannel chan<- error,
	columnInfo []ColumnInfo,
	transfer Transfer,
) (
	err error,
) {
	finalCsvChannel := system.convertPipeFiles(
		ctx, pipeFileChannel, errorChannel, columnInfo, transfer)

	err = system.insertFinalCsvs(ctx, finalCsvChannel, transfer)
	if err != nil {
		return fmt.Errorf("error inserting final csvs :: %v", err)
	}

	return nil
}

func (system Postgresql) convertPipeFiles(
	ctx context.Context,
	pipeFileChannel <-chan string,
	errorChannel chan<- error,
	columnInfo []ColumnInfo,
	transfer Transfer,
) <-chan string {
	return convertPipeFilesCommon(ctx, pipeFileChannel, errorChannel, columnInfo, transfer, system)
}

func (system Postgresql) getFinalCsvFormatters() (
	finalCsvFormatters map[string]func(string) (finalCsvValue string, err error),
) {
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
			return v, nil
		},
		"datetimetz": func(v string) (string, error) {
			return v, nil
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
}

func (system Postgresql) insertFinalCsvs(
	ctx context.Context,
	finalCsvChannel <-chan string,
	transfer Transfer,
) (
	err error,
) {
	return insertFinalCsvsCommon(ctx, finalCsvChannel, transfer, system)
}

func (system Postgresql) runInsertCmd(
	ctx context.Context,
	finalCsvLocation string,
	transfer Transfer,
) (
	err error,
) {
	copyCmd := fmt.Sprintf(`\copy %s.%s FROM '%s' WITH 
	(FORMAT csv, HEADER false, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL '%v', ENCODING 'UTF8')`,
		transfer.TargetSchema, transfer.TargetTable, finalCsvLocation, transfer.Null)

	cmd := exec.CommandContext(ctx, "psql", transfer.TargetConnectionString, "-c", copyCmd)

	result, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to upload csv to postgresql :: stderr %v :: stdout %s",
			err, string(result))
	}

	return nil
}
