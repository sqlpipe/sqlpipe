package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type Mysql struct {
	name             string
	connectionString string
	connection       *sql.DB
}

func newMysql(name, connectionString string) (mysql Mysql, err error) {
	if name == "" {
		name = TypeMySQL
	}
	db, err := openDbCommon(name, connectionString, DriverMySQL)
	if err != nil {
		return mysql, fmt.Errorf("error opening mysql db :: %v", err)
	}
	mysql.connection = db
	mysql.name = name
	mysql.connectionString = connectionString
	return mysql, nil
}

func (system Mysql) query(query string) (rows *sql.Rows, err error) {
	rows, err = system.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error running dql on %v :: %v :: %v", system.name, query, err)
	}
	return rows, nil
}

func (system Mysql) exec(query string) (err error) {
	_, err = system.connection.Exec(query)
	if err != nil {
		return fmt.Errorf("error running ddl/dml on %v :: %v :: %v", system.name, query, err)
	}
	return nil
}

func (system Mysql) dropTableIfExists(transfer Transfer) (dropped string, err error) {
	return dropTableIfExistsCommon(transfer, system)
}

func (system Mysql) getColumnInfo(rows *sql.Rows) ([]ColumnInfo, error) {
	return getColumnInfoCommon(rows, system)
}

func (system Mysql) createTable(
	columnInfo []ColumnInfo,
	transfer Transfer,
) (
	created string,
	err error,
) {
	return createTableCommon(columnInfo, transfer, system)
}

func (system Mysql) dbTypeToPipeType(
	columnType *sql.ColumnType,
	databaseTypeName string,
) (
	pipeType string,
	err error,
) {
	switch databaseTypeName {
	case "VARCHAR":
		return "nvarchar", nil
	case "CHAR":
		return "nvarchar", nil
	case "TEXT":
		return "ntext", nil

	case "UNSIGNED BIGINT":
		return "int64", nil
	case "BIGINT":
		return "int64", nil
	case "UNSIGNED INT":
		return "int64", nil
	case "INT":
		return "int32", nil
	case "MEDIUMINT":
		return "int32", nil
	case "UNSIGNED SMALLINT":
		return "int32", nil
	case "YEAR":
		return "int16", nil
	case "SMALLINT":
		return "int16", nil
	case "UNSIGNED TINYINT":
		return "int16", nil
	case "TINYINT":
		return "int16", nil

	case "DOUBLE":
		return "float64", nil
	case "FLOAT":
		return "float32", nil

	case "DECIMAL":
		return "decimal", nil

	case "DATETIME":
		return "datetime", nil
	case "TIMESTAMP":
		return "datetimetz", nil
	case "DATE":
		return "date", nil
	case "TIME":
		return "time", nil

	case "BINARY":
		return "blob", nil
	case "VARBINARY":
		return "blob", nil
	case "BLOB":
		return "blob", nil

	case "JSON":
		return "json", nil

	case "BIT":
		return "varbit", nil

	case "GEOMETRY":
		return "blob", nil

	default:
		return "", fmt.Errorf("unsupported database type for mysql: %v", databaseTypeName)
	}
}

func (system Mysql) pipeTypeToCreateType(columnInfo ColumnInfo, transfer Transfer) (createType string, err error) {
	switch columnInfo.pipeType {
	case "nvarchar":
		return "longtext", nil
	case "varchar":
		return "longtext", nil
	case "ntext":
		return "longtext", nil
	case "text":
		return "longtext", nil
	case "int64":
		return "bigint", nil
	case "int32":
		return "integer", nil
	case "int16":
		return "smallint", nil
	case "float64":
		return "double", nil
	case "float32":
		return "float", nil
	case "decimal":

		scaleOk := false
		precisionOk := false

		if columnInfo.decimalOk {
			if columnInfo.scale > 0 && columnInfo.scale <= 65 {
				scaleOk = true
			}

			if columnInfo.precision > 0 &&
				columnInfo.precision <= 65 &&
				columnInfo.precision > columnInfo.scale {
				precisionOk = true
			}
		}

		if scaleOk && precisionOk {
			return fmt.Sprintf("decimal(%v,%v)", columnInfo.precision, columnInfo.scale), nil
		} else {
			if transfer.CastBadDecimalToVarchar {
				warningLog.Printf(
					"transfer %v: invalid decimal scale or precision, using varchar for column %v",
					transfer.Id,
					columnInfo.name,
				)
				return "longtext", nil
			} else {
				warningLog.Printf(
					"transfer %v: invalid decimal scale or precision, using double float for column %v",
					transfer.Id,
					columnInfo.name,
				)
				return "double", nil
			}
		}

	case "money":

		if columnInfo.decimalOk {
			if columnInfo.precision > 65 {
				return "", fmt.Errorf("precision on column %v is greater than 65", columnInfo.name)
			}
			if columnInfo.scale > 30 {
				return "", fmt.Errorf("scale on column %v is greater than 30", columnInfo.name)
			}
			return fmt.Sprintf("decimal(%v,%v)", columnInfo.precision, columnInfo.scale), nil
		}
		return "float", nil

	case "datetime":
		return "datetime", nil
	case "datetimetz":
		return "datetime", nil
	case "date":
		return "date", nil
	case "time":
		return "time", nil
	case "varbinary":
		return "blob", nil
	case "blob":
		return "blob", nil
	case "uuid":
		return "binary(16)", nil
	case "bool":
		return "tinyint(1)", nil
	case "json":
		return "json", nil
	case "xml":
		return "longtext", nil
	case "varbit":
		return "longtext", nil
	default:
		return "", fmt.Errorf("unsupported pipeType for mysql: %v", columnInfo.pipeType)
	}
}

func (system Mysql) createPipeFiles(
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

func (system Mysql) getPipeFileFormatters() (
	pipeFileFormatters map[string]func(interface{}) (pipeFileValue string, err error),
) {
	pipeFileFormatters = map[string]func(interface{}) (pipeFileValue string, err error){
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
			valBytes, ok := v.([]byte)
			if !ok {
				return "", errors.New("non []uint8 value passed to int64 mysqlPipeFileFormatter")
			}
			return string(valBytes), nil
		},
		"int32": func(v interface{}) (pipeFileValue string, err error) {
			valBytes, ok := v.([]byte)
			if !ok {
				return "", errors.New("non []uint8 value passed to int32 mysqlPipeFileFormatter")
			}
			return string(valBytes), nil
		},
		"int16": func(v interface{}) (pipeFileValue string, err error) {
			valBytes, ok := v.([]byte)
			if !ok {
				return "", errors.New("non []uint8 value passed to int16 mysqlPipeFileFormatter")
			}
			return string(valBytes), nil
		},
		"float64": func(v interface{}) (pipeFileValue string, err error) {
			valBytes, ok := v.([]byte)
			if !ok {
				return "", errors.New("non []uint8 value passed to float64 mysqlPipeFileFormatter")
			}
			return string(valBytes), nil
		},
		"float32": func(v interface{}) (pipeFileValue string, err error) {
			valBytes, ok := v.([]byte)
			if !ok {
				return "", errors.New("non []uint8 value passed to float32 mysqlPipeFileFormatter")
			}
			return string(valBytes), nil
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
					"non time.Time value passed to datetime mysqlPipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"datetimetz": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to datetime mysqlPipeFileFormatters")
			}
			return valTime.UTC().Format(time.RFC3339Nano), nil
		},
		"date": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to datetime mysqlPipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"time": func(v interface{}) (pipeFileValue string, err error) {
			valBytes, ok := v.([]byte)
			if !ok {
				return "", errors.New("non []uint8 value passed to time mysqlPipeFileFormatter")
			}

			valTime, err := time.Parse("15:04:05.999999", string(valBytes))
			if err != nil {
				return "", fmt.Errorf(
					"error parsing time value in mysqlPipeFileFormatter :: %v", err)
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

			varbitBuilder := strings.Builder{}

			valBytes, ok := v.([]byte)
			if !ok {
				return "", errors.New("non []uint8 value passed to varbit mysqlPipeFileFormatter")
			}

			for _, b := range valBytes {
				varbitBuilder.WriteString(fmt.Sprintf("%b", b))
			}

			return strings.TrimLeft(varbitBuilder.String(), "0"), nil
		},
	}

	return pipeFileFormatters
}

func (system Mysql) insertPipeFiles(
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

func (system Mysql) convertPipeFiles(
	ctx context.Context,
	pipeFileChannel <-chan string,
	errorChannel chan<- error,
	columnInfo []ColumnInfo,
	transfer Transfer,
) (
	finalCsvChannel <-chan string,
) {
	return convertPipeFilesCommon(ctx, pipeFileChannel, errorChannel, columnInfo, transfer, system)
}

func (system Mysql) getFinalCsvFormatters() (
	finalCsvFormatters map[string]func(string) (finalCsvValue string, err error)) {
	return map[string]func(string) (finalCsvValue string, err error){
		"nvarchar": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"varchar": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"ntext": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"text": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"int64": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"int32": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"int16": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"float64": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"float32": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"decimal": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"money": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"datetime": func(v string) (finalCsvValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf(
					"error parsing datetimetz value in mysql datetime mysql formatter :: %v", err)
			}

			return valTime.Format("2006-01-02 15:04:05.999999"), nil
		},
		"datetimetz": func(v string) (finalCsvValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf(
					"error parsing datetimetz value in mysql datetimetz mysql formatter :: %v",
					err)
			}

			return valTime.Format("2006-01-02 15:04:05.999999"), nil
		},
		"date": func(v string) (finalCsvValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to mysql csv :: %v", err)
			}
			return valTime.Format("2006-01-02"), nil
		},
		"time": func(v string) (finalCsvValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing time value to mysql csv :: %v", err)
			}

			return valTime.Format("15:04:05.999999"), nil
		},
		"varbinary": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"blob": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"uuid": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"bool": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"json": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"xml": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
		"varbit": func(v string) (finalCsvValue string, err error) {
			return v, nil
		},
	}
}

func (system Mysql) insertFinalCsvs(
	ctx context.Context,
	finalCsvChannel <-chan string,
	transfer Transfer,
) (
	err error,
) {
	return insertFinalCsvsCommon(ctx, finalCsvChannel, transfer, system)
}

func (system Mysql) runInsertCmd(
	ctx context.Context,
	finalCsvLocation string,
	transfer Transfer,
) (
	err error,
) {
	mysql.RegisterLocalFile(finalCsvLocation)
	defer mysql.DeregisterLocalFile(finalCsvLocation)

	copyQuery := fmt.Sprintf(
		`load data local infile '%v' into table %v fields escaped by '' terminated by ','
		optionally enclosed by '"' lines terminated by '\n';`, finalCsvLocation,
		transfer.TargetTable)

	err = system.exec(copyQuery)
	if err != nil {
		return fmt.Errorf("error inserting csv into mysql :: %v", err)
	}

	return nil
}
