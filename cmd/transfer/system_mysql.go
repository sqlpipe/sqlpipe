package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/sqlpipe/sqlpipe/internal/data"
)

type Mysql struct {
	Name       string
	Connection *sql.DB
}

func (system Mysql) getSystemName() (name string) {
	return system.Name
}

func newMysql(connectionInfo ConnectionInfo) (mysql Mysql, err error) {
	db, err := openConnectionPool(connectionInfo.Name, connectionInfo.ConnectionString, DriverMySQL)
	if err != nil {
		return mysql, fmt.Errorf("error opening mysql db :: %v", err)
	}
	mysql.Connection = db
	mysql.Name = connectionInfo.Name
	return mysql, nil
}

func (system Mysql) closeConnectionPool(printError bool) (err error) {
	err = system.Connection.Close()
	if err != nil && printError {
		logger.Error(fmt.Sprintf("error closing %v connection pool :: %v", system.Name, err))
	}
	return err
}

func (system Mysql) query(query string) (rows *sql.Rows, err error) {
	rows, err = system.Connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error running dql on %v :: %v :: %v", system.Name, query, err)
	}
	return rows, nil
}

func (system Mysql) queryRow(query string) (row *sql.Row) {
	row = system.Connection.QueryRow(query)
	return row
}

func (system Mysql) exec(query string) (err error) {
	_, err = system.Connection.Exec(query)
	if err != nil {
		return fmt.Errorf("error running ddl/dml on %v :: %v :: %v", system.Name, query, err)
	}
	return nil
}

func (system Mysql) dropTableIfExistsOverride(schema, table string) (overridden bool, err error) {
	return false, nil
}

func (system Mysql) createTableIfNotExistsOverride(schema, table string, columnInfos []ColumnInfo, incremental bool) (overridden bool, err error) {
	return false, nil
}

func (system Mysql) driverTypeToPipeType(
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

func (system Mysql) pipeTypeToCreateType(columnInfo ColumnInfo) (createType string, err error) {
	switch columnInfo.PipeType {
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

		if columnInfo.DecimalOk {
			if columnInfo.Scale > 0 && columnInfo.Scale <= 65 {
				scaleOk = true
			}

			if columnInfo.Precision > 0 &&
				columnInfo.Precision <= 65 &&
				columnInfo.Precision > columnInfo.Scale {
				precisionOk = true
			}
		}

		if scaleOk && precisionOk {
			return fmt.Sprintf("decimal(%v,%v)", columnInfo.Precision, columnInfo.Scale), nil
		} else {
			return "double", nil
		}

	case "money":

		if columnInfo.DecimalOk {
			if columnInfo.Precision > 65 {
				return "", fmt.Errorf("precision on column %v is greater than 65", columnInfo.Name)
			}
			if columnInfo.Scale > 30 {
				return "", fmt.Errorf("scale on column %v is greater than 30", columnInfo.Name)
			}
			return fmt.Sprintf("decimal(%v,%v)", columnInfo.Precision, columnInfo.Scale), nil
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
		return "", fmt.Errorf("unsupported pipeType for mysql: %v", columnInfo.PipeType)
	}
}

func (system Mysql) createPipeFilesOverride(pipeFileChannelIn chan PipeFileInfo, columnInfo []ColumnInfo, transferInfo data.TransferInfo, rows *sql.Rows,
) (pipeFileInfoChannel chan PipeFileInfo, overridden bool) {
	return pipeFileChannelIn, false
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

func (system Mysql) insertPipeFilesOverride(columnInfo []ColumnInfo, transferInfo data.TransferInfo, pipeFileInfoChannel <-chan PipeFileInfo, vacuumTable string) (overridden bool, err error) {
	return false, nil
}

func (system Mysql) convertPipeFilesOverride(pipeFilePath <-chan PipeFileInfo, finalCsvInfoChannelIn chan FinalCsvInfo, transferInfo data.TransferInfo, columnInfos []ColumnInfo,
) (finalCsvInfoChannel chan FinalCsvInfo, overridden bool) {
	return finalCsvInfoChannelIn, false
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

func (system Mysql) insertFinalCsvsOverride(transferInfo data.TransferInfo) (overridden bool, err error) {
	return false, nil
}

func (system Mysql) runInsertCmd(
	finalCsvInfo FinalCsvInfo,
	transferInfo data.TransferInfo,
	schema, table string,
) (
	err error,
) {
	mysql.RegisterLocalFile(finalCsvInfo.FilePath)
	defer mysql.DeregisterLocalFile(finalCsvInfo.FilePath)

	escapedTable := system.escape(table)

	copyQuery := fmt.Sprintf(
		`load data local infile '%v' into table %v fields escaped by '' terminated by ','
		optionally enclosed by '"' lines terminated by '\n';`, finalCsvInfo.FilePath, escapedTable)

	err = system.exec(copyQuery)
	if err != nil {
		return fmt.Errorf("error inserting csv into mysql :: %v", err)
	}

	return nil
}

func (system Mysql) escape(objectName string) (escaped string) {
	return fmt.Sprintf("`%v`", objectName)
}

func (system Mysql) isReservedKeyword(objectName string) bool {
	return false
}

func (system Mysql) schemaRequired() bool {
	return false
}

// func (system Mysql) getPkQuery(table Table) (query string) {
// 	return fmt.Sprintf(`
// 		SELECT
// 			COLUMN_NAME AS column_name
// 		FROM
// 			INFORMATION_SCHEMA.KEY_COLUMN_USAGE
// 		WHERE
// 			CONSTRAINT_NAME = 'PRIMARY'
// 			AND TABLE_SCHEMA = '%v'
// 			AND TABLE_NAME = '%v'
// 		ORDER BY
// 			ORDINAL_POSITION ASC;
// 	`, table.UnescapedSourceSchema, table.EscapedName)
// }

func (system Mysql) createSchemaIfNotExistsOverride(schema string) (overridden bool, err error) {
	err = system.exec(fmt.Sprintf("create database if not exists %v", schema))
	if err != nil {
		return false, fmt.Errorf("error creating schema :: %v", err)
	}

	return true, nil
}

// func (system Mysql) getColumnNamesQuery(schema, table string) string {
// 	return fmt.Sprintf(`
// 		SELECT
// 			COLUMN_NAME
// 		FROM
// 			INFORMATION_SCHEMA.COLUMNS
// 		WHERE
// 			TABLE_SCHEMA = '%v'
// 			AND TABLE_NAME = '%v'
// 		ORDER BY
//     		ORDINAL_POSITION;
// 	`, table.UnescapedSourceSchema, table.EscapedName)
// }

func (system Mysql) dbTypeToPipeType(
	databaseTypeName string,
) (
	pipeType string,
	err error,
) {
	switch databaseTypeName {
	case "bigint":
		return "int64", nil
	case "char":
		return "nvarchar", nil
	case "varchar":
		return "nvarchar", nil
	case "longtext":
		return "ntext", nil
	case "mediumtext":
		return "ntext", nil
	case "text":
		return "ntext", nil
	case "tinytext":
		return "ntext", nil
	case "enum":
		return "nvarchar", nil
	case "int":
		return "int32", nil
	case "mediumint":
		return "int32", nil
	case "smallint":
		return "int16", nil
	case "tinyint":
		return "int16", nil
	case "double":
		return "float64", nil
	case "float":
		return "float32", nil
	case "decimal":
		return "decimal", nil
	case "datetime":
		return "datetime", nil
	case "timestamp":
		return "datetime", nil
	case "date":
		return "date", nil
	case "time":
		return "time", nil
	case "year":
		return "varchar", nil
	case "binary":
		return "varbinary", nil
	case "varbinary":
		return "varbinary", nil
	case "longblob":
		return "blob", nil
	case "mediumblob":
		return "blob", nil
	case "blob":
		return "blob", nil
	case "tinyblob":
		return "blob", nil
	case "geometry":
		return "blob", nil
	case "bit":
		return "varbit", nil
	case "json":
		return "json", nil
	case "set":
		return "nvarchar", nil

	default:
		return "", fmt.Errorf("unsupported database type for mysql: %v", databaseTypeName)
	}
}

func (system Mysql) IsTableNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "doesn't exist")
}

func (system Mysql) getIncrementalTimeOverride(schema, table, incrementalColumn string, initialLoad bool) (time.Time, bool, bool, error) {
	return time.Time{}, false, initialLoad, nil
}

func (system Mysql) getPrimaryKeysRows(schema, table string) (rows *sql.Rows, err error) {

	unescapedSchemaPeriodTable := getSchemaPeriodTable(schema, table, system, false)

	query := fmt.Sprintf(`
		SELECT 
			COLUMN_NAME 
		FROM 
			information_schema.KEY_COLUMN_USAGE 
		WHERE lower(TABLE_NAME) = lower('%v')
			AND CONSTRAINT_NAME = 'PRIMARY';

		`,
		unescapedSchemaPeriodTable)

	rows, err = system.query(query)
	if err != nil {
		return nil, fmt.Errorf("error getting primary keys rows :: %v", err)
	}

	return rows, nil
}

var mysqlDatetimeFormat = "2006-01-02 15:04:05.999999"
var mysqlDateFormat = "2006-01-02"
var mysqlTimeFormat = "15:04:05.999999"

func (system Mysql) getSqlFormatters() (
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
				return "", fmt.Errorf("error writing date value to mysql csv :: %v", err)
			}
			return fmt.Sprintf("'%v'", valTime.Format(mysqlDatetimeFormat)), nil
		},
		"datetimetz": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to mysql csv :: %v", err)
			}
			return fmt.Sprintf("'%v'", valTime.Format(mysqlDatetimeFormat)), nil
		},
		"date": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to mysql csv :: %v", err)
			}
			return fmt.Sprintf("'%v'", valTime.Format(mysqlDateFormat)), nil
		},
		"time": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to mysql csv :: %v", err)
			}

			return fmt.Sprintf("'%v'", valTime.Format(mysqlTimeFormat)), nil
		},
		"varbinary": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf(`UNHEX('%s')`, v), nil
		},
		"blob": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf(`UNHEX('%s'`, v), nil
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

func (system Mysql) getTableColumnInfosRows(schema, table string) (rows *sql.Rows, err error) {
	query := fmt.Sprintf(`
		WITH PrimaryKeys AS (
			SELECT
				kcu.COLUMN_NAME
			FROM
				information_schema.KEY_COLUMN_USAGE AS kcu
			JOIN information_schema.TABLE_CONSTRAINTS AS tc
				ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
				AND kcu.TABLE_NAME = tc.TABLE_NAME
			WHERE
				tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
				AND kcu.TABLE_NAME = '%v'
		)
		
		SELECT
			columns.COLUMN_NAME AS col_name,
			columns.DATA_TYPE AS col_type,
			COALESCE(columns.NUMERIC_PRECISION, -1) AS col_precision,
			COALESCE(columns.NUMERIC_SCALE, -1) AS col_scale,
			COALESCE(columns.CHARACTER_MAXIMUM_LENGTH, -1) AS col_length,
			CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN true ELSE false END AS col_is_primary
		FROM
			information_schema.COLUMNS AS columns
		LEFT JOIN PrimaryKeys pk ON columns.COLUMN_NAME = pk.COLUMN_NAME
		WHERE
			columns.TABLE_NAME = '%v'
		ORDER BY
			columns.ORDINAL_POSITION;
	`, table, table)

	rows, err = system.query(query)
	if err != nil {
		return nil, fmt.Errorf("error getting table column infos rows :: %v", err)
	}

	return rows, nil
}
