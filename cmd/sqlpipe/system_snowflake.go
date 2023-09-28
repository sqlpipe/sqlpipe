package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Snowflake struct {
	name             string
	connectionString string
	connection       *sql.DB
}

func newSnowflake(name, connectionString string) (snowflake Snowflake, err error) {
	if name == "" {
		name = TypeSnowflake
	}
	db, err := openDbCommon(name, connectionString, DriverSnowflake)
	if err != nil {
		return snowflake, fmt.Errorf("error opening snowflake db :: %v", err)
	}
	snowflake.connection = db
	snowflake.name = name
	snowflake.connectionString = connectionString
	return snowflake, nil
}

func (system Snowflake) query(query string) (rows *sql.Rows, err error) {
	rows, err = system.connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error running dql on %v :: %v :: %v", system.name, query, err)
	}
	return rows, nil
}

func (system Snowflake) exec(query string) (err error) {
	_, err = system.connection.Exec(query)
	if err != nil {
		return fmt.Errorf("error running ddl/dml on %v :: %v :: %v", system.name, query, err)
	}
	return nil
}

func (system Snowflake) dropTableIfExists(transfer Transfer) (dropped string, err error) {
	return dropTableIfExistsCommon(transfer, system)
}

func (system Snowflake) getColumnInfo(rows *sql.Rows) (columnInfo []ColumnInfo, err error) {
	return getColumnInfoCommon(rows, system)
}

func (system Snowflake) createTable(
	columnInfo []ColumnInfo,
	transfer Transfer,
) (
	created string,
	err error,
) {
	return createTableCommon(columnInfo, transfer, system)
}

func (system Snowflake) dbTypeToPipeType(
	columnType *sql.ColumnType,
	databaseTypeName string,
) (
	pipeType string,
	err error,
) {
	switch columnType.DatabaseTypeName() {
	case "TEXT":
		return "nvarchar", nil
	case "REAL":
		return "float64", nil
	case "FIXED":
		return "decimal", nil
	case "TIMESTAMP_NTZ":
		return "datetime", nil
	case "TIMESTAMP_LTZ":
		return "datetimetz", nil
	case "TIMESTAMP_TZ":
		return "datetimetz", nil
	case "DATE":
		return "date", nil
	case "TIME":
		return "time", nil
	case "BINARY":
		return "varbinary", nil
	case "BOOLEAN":
		return "bool", nil
	case "VARIANT":
		return "nvarchar", nil
	case "OBJECT":
		return "nvarchar", nil
	case "ARRAY":
		return "nvarchar", nil
	default:
		return "", fmt.Errorf("unsupported database type for snowflake: %v", columnType.DatabaseTypeName())
	}
}

func (system Snowflake) pipeTypeToCreateType(
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
		return "number", nil
	case "int32":
		return "number", nil
	case "int16":
		return "number", nil
	case "float64":
		return "real", nil
	case "float32":
		return "real", nil
	case "decimal":
		return "number", nil
	case "money":
		return "number", nil
	case "datetime":
		return "datetime", nil
	case "datetimetz":
		return "timestamp_tz", nil
	case "date":
		return "date", nil
	case "time":
		return "time", nil
	case "varbinary":
		return "varbinary", nil
	case "blob":
		return "varbinary", nil
	case "uuid":
		return "varbinary", nil
	case "bool":
		return "boolean", nil
	case "json":
		return "variant", nil
	case "xml":
		return "text", nil
	case "varbit":
		return "text", nil
	default:
		return "", fmt.Errorf("unsupported pipeType for snowflake: %v", columnInfo.pipeType)
	}
}

func (system Snowflake) createPipeFiles(
	ctx context.Context,
	errorChannel chan<- error,
	columnInfo []ColumnInfo,
	transfer Transfer,
	rows *sql.Rows,
) <-chan string {
	return createPipeFilesCommon(ctx, errorChannel, columnInfo, transfer, rows, system)
}

func (system Snowflake) getPipeFileFormatters() (
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
					"non time.Time value passed to datetime snowflakePipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"datetimetz": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to datetimetz snowflakePipeFileFormatters")
			}
			return valTime.UTC().Format(time.RFC3339Nano), nil
		},
		"date": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to date snowflakePipeFileFormatters")
			}
			return valTime.Format(time.RFC3339Nano), nil
		},
		"time": func(v interface{}) (pipeFileValue string, err error) {
			valTime, ok := v.(time.Time)
			if !ok {
				return "", errors.New(
					"non time.Time value passed to time snowflakePipeFileFormatters")
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
			return fmt.Sprintf("%s", v), nil
		},
	}
}

func (system Snowflake) insertPipeFiles(
	ctx context.Context,
	pipeFileChannel <-chan string,
	errorChannel chan<- error,
	columnInfo []ColumnInfo,
	transfer Transfer,
) (
	err error,
) {

	fileFormatName := fmt.Sprintf(`"sqlpipe-%s"`, transfer.Id)

	fileFormatQuery := fmt.Sprintf(
		`CREATE FILE FORMAT %s.%s type = CSV ESCAPE_UNENCLOSED_FIELD = 'NONE' 
		FIELD_OPTIONALLY_ENCLOSED_BY = '\"' NULL_IF = ('%s');;`,
		transfer.TargetSchema, fileFormatName, transfer.Null)
	err = system.exec(fileFormatQuery)
	if err != nil {
		return fmt.Errorf("error creating snowflake file format :: %v", err)
	}
	defer func() {
		err := system.exec(
			fmt.Sprintf(`DROP FILE FORMAT %s."sqlpipe-%s"`, transfer.TargetSchema, transfer.Id))
		if err != nil {
			warningLog.Printf(
				"error dropping snowflake file format %s :: %v", fileFormatName, err)
		}
	}()

	infoLog.Printf("created file format %v in snowflake", fileFormatName)

	finalCsvChannel := system.convertPipeFiles(ctx, pipeFileChannel, errorChannel,
		columnInfo, transfer)
	if err != nil {
		return fmt.Errorf("error converting pipeFiles :: %v", err)
	}

	err = system.insertFinalCsvs(ctx, finalCsvChannel, transfer)
	if err != nil {
		return fmt.Errorf("error inserting final csvs :: %v", err)
	}

	return nil
}

func (system Snowflake) convertPipeFiles(
	ctx context.Context,
	pipeFileChannel <-chan string,
	errorChannel chan<- error,
	columnInfo []ColumnInfo,
	transfer Transfer,
) <-chan string {
	finalCsvChannelIn := convertPipeFilesCommon(ctx, pipeFileChannel, errorChannel,
		columnInfo, transfer, system)

	finalCsvChannelOut := make(chan string)

	go func() {

		defer close(finalCsvChannelOut)

		for snowflakeFileName := range finalCsvChannelIn {

			if !transfer.KeepFiles {
				defer os.Remove(snowflakeFileName)
			}

			stageName := fmt.Sprintf("sqlpipe-%s", uuid.New().String())
			err := system.exec(
				fmt.Sprintf(`CREATE STAGE %s."%s"`, transfer.TargetSchema, stageName))
			if err != nil {
				errorChannel <- fmt.Errorf("error creating snowflake stage :: %v", err)
				return
			}

			err = system.exec(fmt.Sprintf(
				`PUT 'file://%s' @%s."%s"`, snowflakeFileName, transfer.TargetSchema, stageName))
			if err != nil {
				errorChannel <- fmt.Errorf("error putting snowflake csv :: %v", err)
				return
			}

			finalCsvChannelOut <- stageName
		}

		infoLog.Printf("transfer %v finished uploading snowflake csvs", transfer.Id)
	}()

	return finalCsvChannelOut
}

func (system Snowflake) getFinalCsvFormatters() (
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
			return v, nil
		},
		"blob": func(v string) (string, error) {
			return v, nil
		},
		"uuid": func(v string) (string, error) {
			return strings.Replace(v, "-", "", -1), nil
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

func (system Snowflake) insertFinalCsvs(
	ctx context.Context,
	finalCsvChannel <-chan string,
	transfer Transfer,
) (
	err error,
) {
	return insertFinalCsvsCommon(ctx, finalCsvChannel, transfer, system)
}

func (system Snowflake) runInsertCmd(
	ctx context.Context,
	finalCsvLocation string,
	transfer Transfer,
) (
	err error,
) {
	defer func() {
		err := system.exec(fmt.Sprintf(`DROP STAGE %s."%s"`,
			transfer.TargetSchema, finalCsvLocation))
		if err != nil {
			warningLog.Printf("error dropping snowflake stage :: %v", err)
		}
	}()

	err = system.exec(
		fmt.Sprintf(`COPY INTO %s.%s FROM @%s."%s" 
		file_format = (format_name = %s."sqlpipe-%s")`,
			transfer.TargetSchema, transfer.TargetTable, transfer.TargetSchema,
			finalCsvLocation, transfer.TargetSchema, transfer.Id))
	if err != nil {
		return fmt.Errorf("error copying csv into snowflake :: %v", err)
	}

	return nil
}
