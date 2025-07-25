package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Snowflake struct {
	Name       string
	Connection *sql.DB
}

func (system Snowflake) getSystemName() (name string) {
	return system.Name
}

func newSnowflake(connectionInfo ConnectionInfo) (snowflake Snowflake, err error) {
	db, err := openConnectionPool(connectionInfo.Name, connectionInfo.ConnectionString, DriverSnowflake)
	if err != nil {
		return snowflake, fmt.Errorf("error opening snowflake db :: %v", err)
	}
	snowflake.Connection = db
	snowflake.Name = connectionInfo.Name
	return snowflake, nil
}

func (system Snowflake) closeConnectionPool(printError bool) (err error) {
	err = system.Connection.Close()
	if err != nil && printError {
		errorLog.Printf("error closing %v connection pool :: %v", system.Name, err)
	}
	return err
}

func (system Snowflake) query(query string) (rows *sql.Rows, err error) {
	rows, err = system.Connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error running dql on %v :: %v :: %v", system.Name, query, err)
	}
	return rows, nil
}

func (system Snowflake) queryRow(query string) (row *sql.Row) {
	row = system.Connection.QueryRow(query)
	return row
}

func (system Snowflake) exec(query string) (err error) {
	_, err = system.Connection.Exec(query)
	if err != nil {
		return fmt.Errorf("error running ddl/dml on %v :: %v :: %v", system.Name, query, err)
	}
	return nil
}

func (system Snowflake) dropTableIfExistsOverride(schema, table string) (overridden bool, err error) {
	return false, nil
}

func (system Snowflake) escape(objectName string) (escaped string) {
	return fmt.Sprintf(`"%v"`, objectName)
}

func (system Snowflake) isReservedKeyword(keyword string) bool {
	if _, ok := snowflakeReservedKeywords[keyword]; ok {
		return true
	}

	return false
}

func (system Snowflake) createTableIfNotExistsOverride(schema, table string, columnInfos []ColumnInfo, incremental bool) (overridden bool, err error) {
	return false, nil
}

func (system Snowflake) driverTypeToPipeType(
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
) (
	createType string,
	err error,
) {
	switch columnInfo.PipeType {
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
		return "real", nil
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
		return "", fmt.Errorf("unsupported pipeType for snowflake: %v", columnInfo.PipeType)
	}
}

func (system Snowflake) createPipeFilesOverride(pipeFileChannelIn chan PipeFileInfo, columnInfo []ColumnInfo, transfer Transfer, rows *sql.Rows,
) (pipeFileInfoChannel chan PipeFileInfo, overridden bool) {
	return pipeFileChannelIn, false
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

func (system Snowflake) insertPipeFilesOverride(columnInfos []ColumnInfo, transfer Transfer, pipeFileInfoChannel <-chan PipeFileInfo, vacuumTable string) (overridden bool, err error) {

	table := transfer.TargetTable

	escapedSchemaName := escapeIfNeeded(transfer.TargetSchema, system)

	fileFormatQuery := fmt.Sprintf(
		`CREATE FILE FORMAT if not exists %s.sqlpipe_csv type = CSV ESCAPE_UNENCLOSED_FIELD = 'NONE'
		FIELD_OPTIONALLY_ENCLOSED_BY = '\"' NULL_IF = ('%s');`,
		escapedSchemaName, transfer.Null)
	err = system.exec(fileFormatQuery)
	if err != nil {
		return true, fmt.Errorf("error creating snowflake file format :: %v", err)
	}
	infoLog.Printf("created %v.sqlpipe_csv file format if not exists in snowflake", escapedSchemaName)

	createStageQuery := fmt.Sprintf(
		`CREATE STAGE if not exists %v.sqlpipe_stage;`,
		escapedSchemaName)
	err = system.exec(createStageQuery)
	if err != nil {
		return true, fmt.Errorf("error creating sqlpipe_stage in snowflake :: %v", err)
	}
	infoLog.Printf("created %v.sqlpipe_stage if not exists in snowflake", escapedSchemaName)

	finalCsvChannel := convertPipeFiles(pipeFileInfoChannel, columnInfos, transfer, system)

	putCsvsChannel := system.putCsvs(finalCsvChannel, columnInfos, transfer)

	err = insertFinalCsvs(putCsvsChannel, transfer, system, transfer.TargetSchema, table)
	if err != nil {
		return true, fmt.Errorf("error inserting final csvs :: %v", err)
	}

	return true, nil
}

func (system Snowflake) insertFinalCsvsOverride(transfer Transfer) (overridden bool, err error) {
	return false, nil
}

func (system Snowflake) convertPipeFilesOverride(pipeFilePath <-chan PipeFileInfo, finalCsvInfoChannelIn chan FinalCsvInfo, transfer Transfer, columnInfos []ColumnInfo,
) (finalCsvInfoChannel chan FinalCsvInfo, overridden bool) {
	return finalCsvInfoChannelIn, false
}

func (system Snowflake) putCsvs(
	finalCsvChannelIn <-chan FinalCsvInfo,
	columnInfo []ColumnInfo,
	transfer Transfer,
) <-chan FinalCsvInfo {

	finalCsvChannelOut := make(chan FinalCsvInfo)

	go func() {

		defer close(finalCsvChannelOut)

		for finalCsvInfo := range finalCsvChannelIn {

			escapedSchema := escapeIfNeeded(transfer.TargetSchema, system)

			finalCsvInfo.InsertInfo = fmt.Sprintf("%v.csv", uuid.New().String())

			putQuery := fmt.Sprintf(`PUT file://%v @%v.sqlpipe_stage/%v`, finalCsvInfo.FilePath, escapedSchema, finalCsvInfo.InsertInfo)

			err := system.exec(putQuery)
			if err != nil {
				transfer.Error = fmt.Sprintf("error putting csv into snowflake :: %v", err)
				transferMap.CancelAndSetStatus(transfer.Id, transfer, StatusError)
				errorLog.Println(transfer.Error)
				return
			}

			finalCsvChannelOut <- finalCsvInfo
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
				return "", fmt.Errorf("error writing date value to snowflake csv :: %v", err)
			}
			return valTime.Format("2006-01-02"), nil
		},
		"time": func(v string) (string, error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing time value to snowflake csv :: %v", err)
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

func (system Snowflake) runInsertCmd(
	finalCsvInfo FinalCsvInfo,
	transfer Transfer,
	schema, table string,
) (
	err error,
) {

	escapedSchemaPeriodtable := getSchemaPeriodTable(schema, table, system, true)
	escapedSchema := escapeIfNeeded(schema, system)

	copyQuery := fmt.Sprintf(`	
		COPY INTO %v
		FROM @%v.sqlpipe_stage/%v
		FILE_FORMAT = (FORMAT_NAME = %v.sqlpipe_csv)`,
		escapedSchemaPeriodtable,
		escapedSchema,
		finalCsvInfo.InsertInfo,
		escapedSchema,
	)

	err = system.exec(copyQuery)
	if err != nil {
		return fmt.Errorf("error copying csv into snowflake :: %v", err)
	}

	return nil
}

func (system Snowflake) schemaRequired() bool {
	return true
}

func (system Snowflake) createSchemaIfNotExistsOverride(schema string) (overridden bool, err error) {
	return false, nil
}

var snowflakeReservedKeywords = map[string]bool{
	"alter":             true,
	"and":               true,
	"as":                true,
	"between":           true,
	"by":                true,
	"case":              true,
	"cast":              true,
	"check":             true,
	"collate":           true,
	"column":            true,
	"create":            true,
	"current_date":      true,
	"current_time":      true,
	"current_timestamp": true,
	"delete":            true,
	"desc":              true,
	"distinct":          true,
	"drop":              true,
	"else":              true,
	"exists":            true,
	"false":             true,
	"for":               true,
	"from":              true,
	"full":              true,
	"grant":             true,
	"group":             true,
	"having":            true,
	"in":                true,
	"inner":             true,
	"insert":            true,
	"intersect":         true,
	"into":              true,
	"is":                true,
	"join":              true,
	"left":              true,
	"like":              true,
	"not":               true,
	"null":              true,
	"on":                true,
	"or":                true,
	"order":             true,
	"outer":             true,
	"revoke":            true,
	"right":             true,
	"select":            true,
	"set":               true,
	"table":             true,
	"then":              true,
	"true":              true,
	"union":             true,
	"update":            true,
	"using":             true,
	"values":            true,
	"when":              true,
	"where":             true,
	"with":              true,
}

func (system Snowflake) IsTableNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "does not exist")
}

func (system Snowflake) dbTypeToPipeType(
	databaseTypeName string,
) (
	pipeType string,
	err error,
) {
	switch databaseTypeName {
	case "NUMBER":
		return "decimal", nil
	case "FLOAT":
		return "float64", nil
	case "TEXT":
		return "nvarchar", nil
	case "BINARY":
		return "varbinary", nil
	case "BOOLEAN":
		return "bool", nil
	case "DATE":
		return "date", nil
	case "TIME":
		return "time", nil
	case "TIMESTAMP_LTZ":
		return "datetimetz", nil
	case "TIMESTAMP_NTZ":
		return "datetime", nil
	case "TIMESTAMP_TZ":
		return "datetimetz", nil
	case "VARIANT":
		return "nvarchar", nil
	case "OBJECT":
		return "varbinary", nil
	case "ARRAY":
		return "nvarchar", nil
	case "GEOGRAPHY":
		return "varbinary", nil
	case "GEOMETRY":
		return "varbinary", nil
	default:
		return "", fmt.Errorf("unsupported database type for snowflake: %v", databaseTypeName)
	}
}

func (system Snowflake) getIncrementalTimeOverride(schema, table, incrementalColumn string, initialLoad bool) (time.Time, bool, bool, error) {
	return time.Time{}, false, initialLoad, nil
}

func (system Snowflake) getPrimaryKeysRows(schema, table string) (rows *sql.Rows, err error) {
	return nil, errors.New("snowflake does not enforce primary keys")
}

func (system Snowflake) getTableColumnInfosRows(schema, table string) (rows *sql.Rows, err error) {
	query := fmt.Sprintf(`
		SELECT
			columns.COLUMN_NAME AS col_name,
			columns.DATA_TYPE AS col_type,
			coalesce(columns.NUMERIC_PRECISION, -1) AS col_precision,
			coalesce(columns.NUMERIC_SCALE, -1) AS col_scale,
			coalesce(columns.CHARACTER_MAXIMUM_LENGTH, -1) AS col_length,
			false as is_primary_key
		FROM
			INFORMATION_SCHEMA.COLUMNS columns
		WHERE 
			upper(columns.TABLE_SCHEMA) = upper('%v')
			AND upper(columns.TABLE_NAME) = upper('%v')
		ORDER BY
			columns.ORDINAL_POSITION;`,
		schema, table)

	rows, err = system.query(query)
	if err != nil {
		return nil, fmt.Errorf("error getting table column infos rows :: %v", err)
	}

	return rows, nil
}

var snowflakeDatetimeFormatter = "2006-01-02 15:04:05.999999999"
var snowflakeDateFormatter = "2006-01-02"
var snowflakeTimeFormatter = "15:04:05.999999999"

func (system Snowflake) getSqlFormatters() (
	pipeFileFormatters map[string]func(string) (pipeFileValue string, err error),
) {
	return map[string]func(string) (pipeFileValue string, err error){
		"nvarchar": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"varchar": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"ntext": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"text": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"int64": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"int32": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"int16": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"float64": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"float32": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"decimal": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"money": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"datetime": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to snowflake csv :: %v", err)
			}
			return fmt.Sprintf("'%v'", valTime.Format(snowflakeDatetimeFormatter)), nil
		},
		"datetimetz": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to snowflake csv :: %v", err)
			}
			return fmt.Sprintf("'%v'", valTime.Format(snowflakeDatetimeFormatter)), nil
		},
		"date": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to snowflake csv :: %v", err)
			}
			return fmt.Sprintf("'%v'", valTime.Format(snowflakeDateFormatter)), nil
		},
		"time": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to snowflake csv :: %v", err)
			}

			return fmt.Sprintf("'%v'", valTime.Format(snowflakeTimeFormatter)), nil
		},
		"varbinary": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf(`0x%s`, v), nil
		},
		"blob": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf(`0x%s`, v), nil
		},
		"uuid": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"bool": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
		"json": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"xml": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf("'%v'", singleQuoteReplacer.Replace(v)), nil
		},
		"varbit": func(v string) (pipeFileValue string, err error) {
			return v, nil
		},
	}
}
