package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sqlpipe/sqlpipe/internal/data"
)

type Postgresql struct {
	Connection     *sql.DB
	ConnectionInfo ConnectionInfo
}

func newPostgresql(connectionInfo ConnectionInfo) (postgresql Postgresql, err error) {

	connectionString := fmt.Sprintf("postgresql://%v:%v@%v:%v/%v", connectionInfo.Username,
		// connectionString := fmt.Sprintf("postgresql://%v:%v@%v:%v/%v?sslmode=verify-full", connectionInfo.Username,
		connectionInfo.Password, connectionInfo.Hostname, connectionInfo.Port, connectionInfo.Database)

	db, err := openConnectionPool(connectionString, DriverPostgreSQL)
	if err != nil {
		return postgresql, fmt.Errorf("error opening postgresql db :: %v", err)
	}

	postgresql.Connection = db
	postgresql.ConnectionInfo = connectionInfo

	return postgresql, nil
}

func (system Postgresql) closeConnectionPool(printError bool) (err error) {
	err = system.Connection.Close()
	if err != nil && printError {
		logger.Error(fmt.Sprintf("error closing connection pool :: %v", err))
	}
	return err
}

func (system Postgresql) query(query string) (rows *sql.Rows, err error) {
	rows, err = system.Connection.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error running dql :: %v :: %v", query, err)
	}
	return rows, nil
}

func (system Postgresql) queryRow(query string) (row *sql.Row) {
	return system.Connection.QueryRow(query)
}

func (system Postgresql) exec(query string) (err error) {
	_, err = system.Connection.Exec(query)
	if err != nil {
		return fmt.Errorf("error running ddl/dml on :: %v :: %v", query, err)
	}
	return nil
}

func (system Postgresql) dropTableIfExistsOverride(schema, table string) (overridden bool, err error) {
	return false, nil
}

func (system Postgresql) driverTypeToPipeType(
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

func (system Postgresql) dbTypeToPipeType(
	databaseTypeName string,
) (
	pipeType string,
	err error,
) {
	switch databaseTypeName {
	case "character varying":
		return "nvarchar", nil
	case "character":
		return "nvarchar", nil
	case "text":
		return "ntext", nil
	case "bigint":
		return "int64", nil
	case "integer":
		return "int32", nil
	case "smallint":
		return "int16", nil
	case "double precision":
		return "float64", nil
	case "real":
		return "float32", nil
	case "numeric":
		return "decimal", nil
	case "timestamp without time zone":
		return "datetime", nil
	case "timestamp with time zone":
		return "datetimetz", nil
	case "date":
		return "date", nil
	case "interval":
		return "nvarchar", nil
	case "time without time zone":
		return "time", nil
	case "time with time zone":
		return "nvarchar", nil
	case "bytea":
		return "blob", nil
	case "uuid":
		return "uuid", nil
	case "boolean":
		return "bool", nil
	case "json":
		return "json", nil
	case "jsonb":
		return "json", nil
	case "xml":
		return "xml", nil
	case "bit":
		return "varbit", nil
	case "bit varying":
		return "varbit", nil
	case "box":
		return "nvarchar", nil
	case "circle":
		return "nvarchar", nil
	case "line":
		return "nvarchar", nil
	case "path":
		return "nvarchar", nil
	case "point":
		return "nvarchar", nil
	case "polygon":
		return "nvarchar", nil
	case "lseg":
		return "nvarchar", nil
	case "inet":
		return "nvarchar", nil
	case "macaddr":
		return "nvarchar", nil
	case "cidr":
		return "nvarchar", nil
	case "tsvector":
		return "nvarchar", nil
	case "tsquery":
		return "nvarchar", nil
	case "txid_snapshot":
		return "nvarchar", nil
	case "pg_lsn":
		return "nvarchar", nil
	case "pg_snapshot":
		return "nvarchar", nil
	case "USER-DEFINED":
		return "nvarchar", nil
	default:
		return "", fmt.Errorf("unsupported database type for postgresql: %v", databaseTypeName)
	}
}

func (system Postgresql) pipeTypeToCreateType(
	columnInfo *data.ColumnInfo,
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

		if columnInfo.DecimalOk {
			if columnInfo.Scale > 0 && columnInfo.Scale <= 1000 {
				scaleOk = true
			}

			if columnInfo.Precision > 0 && columnInfo.Precision <= 1000 && columnInfo.Precision > columnInfo.Scale {
				precisionOk = true
			}
		}

		if scaleOk && precisionOk {
			return fmt.Sprintf("decimal(%v,%v)", columnInfo.Precision, columnInfo.Scale), nil
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
		return "", fmt.Errorf("unsupported pipeType for postgresql: %v", columnInfo.PipeType)
	}
}

func (system Postgresql) createPipeFilesOverride(pipeFileChannelIn chan PipeFileInfo, transferInfo *data.TransferInfo, rows *sql.Rows,
) (pipeFileInfoChannel chan PipeFileInfo, overridden bool) {
	return pipeFileChannelIn, false
}

func (system Postgresql) getPipeFileFormatters() (
	pipeFileFormatters map[string]func(interface{}) (pipeFileValue string, err error),
) {
	return map[string]func(interface{}) (pipeFileValue string, err error){
		"nvarchar": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprint(v), nil
		},
		"varchar": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprint(v), nil
		},
		"ntext": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprint(v), nil
		},
		"text": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprint(v), nil
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
			return fmt.Sprint(v), nil
		},
		"money": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprint(v), nil
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
				fmt.Println(timeString)
				return "", errors.New("error parsing time value in postgresqlPipeFileFormatters")
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
			return fmt.Sprint(v), nil
		},
		"bool": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%t", v), nil
		},
		"json": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprintf("%s", v), nil
		},
		"xml": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprint(v), nil
		},
		"varbit": func(v interface{}) (pipeFileValue string, err error) {
			return fmt.Sprint(v), nil
		},
	}
}

var singleQuoteReplacer = strings.NewReplacer("'", "''")
var postgresqlTimeFormatString = "2006-01-02 15:04:05.999999"

func (system Postgresql) getSqlFormatters() (
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
				return "", fmt.Errorf("error writing date value to psql csv :: %v", err)
			}
			return fmt.Sprintf("'%v'", valTime.Format(postgresqlTimeFormatString)), nil
		},
		"datetimetz": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to psql csv :: %v", err)
			}
			return fmt.Sprintf("'%v'", valTime.Format(postgresqlTimeFormatString)), nil
		},
		"date": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to psql csv :: %v", err)
			}
			return fmt.Sprintf("'%v'", valTime.Format(postgresqlTimeFormatString)), nil
		},
		"time": func(v string) (pipeFileValue string, err error) {
			valTime, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return "", fmt.Errorf("error writing date value to psql csv :: %v", err)
			}

			return fmt.Sprintf("'%v'", valTime.Format(postgresqlTimeFormatString)), nil
		},
		"varbinary": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf(`\x%s`, v), nil
		},
		"blob": func(v string) (pipeFileValue string, err error) {
			return fmt.Sprintf(`\x%s`, v), nil
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

func (system Postgresql) insertPipeFilesOverride(transferInfo *data.TransferInfo, pipeFileInfoChannel <-chan PipeFileInfo) (overridden bool, err error) {
	return false, nil
}

func (system Postgresql) convertPipeFilesOverride(pipeFilePath <-chan PipeFileInfo, finalCsvInfoChannelIn chan FinalCsvInfo, transferInfo *data.TransferInfo,
) (finalCsvInfoChannel chan FinalCsvInfo, overridden bool) {
	return finalCsvInfoChannelIn, false
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

func (system Postgresql) insertFinalCsvsOverride(transferInfo *data.TransferInfo) (overridden bool, err error) {
	return false, nil
}

func (system Postgresql) runInsertCmd(
	finalCsvInfo FinalCsvInfo,
	transferInfo *data.TransferInfo,
	schema, table string,
) (
	err error,
) {

	escapedSchemaPeriodTable := getSchemaPeriodTable(schema, table, system, true)

	copyCmd := fmt.Sprintf(`\copy %v FROM '%s' WITH 
	(FORMAT csv, HEADER false, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL '%v', ENCODING 'UTF8')`,
		escapedSchemaPeriodTable, finalCsvInfo.FilePath, transferInfo.Null)

	cmd := exec.CommandContext(transferInfo.Context, "psql", transferInfo.TargetConnectionString, "-c", copyCmd)

	result, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to upload csv to postgresql :: stderr %v :: stdout %s",
			err, string(result))
	}

	return nil
}

func (system Postgresql) schemaRequired() bool {
	return true
}

func (system Postgresql) isReservedKeyword(keyword string) bool {
	if _, ok := postgresqlReservedKeywords[keyword]; ok {
		return true
	}

	return false
}

func (system Postgresql) escape(objectName string) (escaped string) {
	return fmt.Sprintf(`"%v"`, objectName)
}

var postgresqlReservedKeywords = map[string]bool{
	"all":               true,
	"analyse":           true,
	"analyze":           true,
	"and":               true,
	"any":               true,
	"array":             true,
	"as":                true,
	"asc":               true,
	"asymmetric":        true,
	"both":              true,
	"case":              true,
	"cast":              true,
	"check":             true,
	"collate":           true,
	"column":            true,
	"constraint":        true,
	"create":            true,
	"current_catalog":   true,
	"current_date":      true,
	"current_role":      true,
	"current_time":      true,
	"current_timestamp": true,
	"current_user":      true,
	"default":           true,
	"desc":              true,
	"distinct":          true,
	"do":                true,
	"else":              true,
	"end":               true,
	"except":            true,
	"false":             true,
	"for":               true,
	"foreign":           true,
	"from":              true,
	"full":              true,
	"grant":             true,
	"group":             true,
	"having":            true,
	"in":                true,
	"initially":         true,
	"inner":             true,
	"intersect":         true,
	"into":              true,
	"is":                true,
	"join":              true,
	"leading":           true,
	"left":              true,
	"like":              true,
	"limit":             true,
	"localtime":         true,
	"localtimestamp":    true,
	"natural":           true,
	"not":               true,
	"null":              true,
	"offset":            true,
	"on":                true,
	"only":              true,
	"or":                true,
	"order":             true,
	"outer":             true,
	"overlaps":          true,
	"placing":           true,
	"primary":           true,
	"references":        true,
	"returning":         true,
	"right":             true,
	"select":            true,
	"session_user":      true,
	"similar":           true,
	"some":              true,
	"symmetric":         true,
	"table":             true,
	"then":              true,
	"to":                true,
	"trailing":          true,
	"true":              true,
	"union":             true,
	"unique":            true,
	"user":              true,
	"using":             true,
	"variadic":          true,
	"verbose":           true,
	"when":              true,
	"where":             true,
	"window":            true,
	"with":              true,
}

// func (system Postgresql) createReplicationPipeFile(schema, table string, replicationCycle, version int64, replication Replication) (pipeFilePath string, columnInfos []ColumnInfo, err error) {
// 	return createReplicationPipeFileCommon(table, replicationCycle, version, replication, system)
// }

func (system Postgresql) getTableColumnInfosRows(schema, table string) (rows *sql.Rows, err error) {
	query := fmt.Sprintf(`
		WITH PrimaryKeys AS (
			SELECT
				kcu.column_name
			FROM
				information_schema.key_column_usage AS kcu
				JOIN information_schema.table_constraints AS tc
					ON kcu.constraint_name = tc.constraint_name
					AND kcu.table_name = tc.table_name
					AND kcu.table_schema = tc.table_schema
			WHERE
				tc.constraint_type = 'PRIMARY KEY'
				AND kcu.table_schema = '%v'
				AND kcu.table_name = '%v'
		)
		
		SELECT
			columns.column_name AS col_name,
			columns.data_type AS col_type,
			coalesce(columns.numeric_precision, -1) AS col_precision,
			coalesce(columns.numeric_scale, -1) AS col_scale,
			coalesce(columns.character_maximum_length, -1) AS col_length,
			CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS col_is_primary
		FROM
			information_schema.columns
			LEFT JOIN PrimaryKeys pk ON columns.column_name = pk.column_name
		WHERE columns.table_schema = '%v'
			AND columns.table_name = '%v'
		ORDER BY
			columns.ordinal_position;`, schema, table, schema, table)

	rows, err = system.query(query)
	if err != nil {
		return nil, fmt.Errorf("error getting table column infos rows :: %v", err)
	}

	return rows, nil
}

func (system Postgresql) getPrimaryKeysRows(schema, table string) (rows *sql.Rows, err error) {

	unescapedSchemaPeriodTable := getSchemaPeriodTable(schema, table, system, false)

	query := fmt.Sprintf(`
		SELECT 
			att.attname AS column_name
		FROM 
			pg_index idx
		JOIN 
			pg_attribute att ON att.attnum = ANY(idx.indkey) AND att.attrelid = idx.indrelid
		JOIN 
			pg_class cls ON cls.oid = idx.indrelid
		WHERE 
			idx.indisprimary = TRUE
			AND cls.oid = '%v'::regclass
		`,
		unescapedSchemaPeriodTable)

	rows, err = system.query(query)
	if err != nil {
		return nil, fmt.Errorf("error getting primary keys rows :: %v", err)
	}

	return rows, nil
}

func (system Postgresql) listAllDatabases() (databases []string, err error) {
	rows, err := system.query("SELECT datname FROM pg_database WHERE datistemplate = false;")
	if err != nil {
		return nil, fmt.Errorf("error listing all databases :: %v", err)
	}

	for rows.Next() {
		var database string
		err = rows.Scan(&database)
		if err != nil {
			return nil, fmt.Errorf("error scanning database :: %v", err)
		}

		databases = append(databases, database)
	}

	return databases, nil
}

func (system Postgresql) listAllSchemasInDatabase() (schemas []string, err error) {
	rows, err := system.query(`SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
		AND schema_name NOT LIKE 'pg_%'`)
	if err != nil {
		return nil, fmt.Errorf("error listing all schemas in database :: %v", err)
	}

	for rows.Next() {
		var schema string
		err = rows.Scan(&schema)
		if err != nil {
			return nil, fmt.Errorf("error scanning schema :: %v", err)
		}

		schemas = append(schemas, schema)
	}

	return schemas, nil
}

func (system Postgresql) listAllTablesInSchema(schema string) (tables []string, err error) {
	rows, err := system.query(fmt.Sprintf(`SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = '%v'
		AND table_type = 'BASE TABLE'`, schema))
	if err != nil {
		return nil, fmt.Errorf("error listing all tables in schema :: %v", err)
	}

	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, fmt.Errorf("error scanning table :: %v", err)
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func (system Postgresql) createSchemaIfNotExistsOverride(schema string) (overridden bool, err error) {
	return false, nil
}

func (system Postgresql) createTableIfNotExistsOverride(schema, table string, transferInfo *data.TransferInfo) (overridden bool, err error) {
	return false, nil
}

func (system Postgresql) getIncrementalTimeOverride(schema, table, incrementalColumn string, initialLoad bool) (time.Time, bool, bool, error) {
	return time.Time{}, false, initialLoad, nil
}

func (system Postgresql) IsTableNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "does not exist")
}

func (system Postgresql) discoverStructure(instanceTransfer *data.InstanceTransfer) (*data.InstanceTransfer, error) {

	treeNodeInstanceID := fmt.Sprintf("%v_%v_%v_%v", instanceTransfer.SourceInstance.CloudProvider, instanceTransfer.SourceInstance.CloudAccountID, instanceTransfer.SourceInstance.Region, instanceTransfer.SourceInstance.ID)

	instanceTransfer.SchemaTree = &data.SafeTreeNode{
		ID:          treeNodeInstanceID,
		Name:        instanceTransfer.SourceInstance.ID,
		ContainsPII: false,
		Mu:          &sync.Mutex{},
	}

	databases, err := system.listAllDatabases()
	if err != nil {
		return nil, fmt.Errorf("error listing all databases :: %v", err)
	}

	dbConnInfo := system.ConnectionInfo

	for _, database := range databases {

		// skip rdsadmin database
		if database == "rdsadmin" {
			continue
		}

		treeNodeDBID := fmt.Sprintf("%v_%v", treeNodeInstanceID, database)

		dbNode := instanceTransfer.SchemaTree.AddChild(treeNodeDBID, database)

		dbConnInfo.Database = database

		dbSystem, err := newPostgresql(dbConnInfo)
		if err != nil {
			return nil, fmt.Errorf("error creating db system :: %v", err)
		}

		schemas, err := dbSystem.listAllSchemasInDatabase()
		if err != nil {
			return nil, fmt.Errorf("error listing all schemas in database %v :: %v", database, err)
		}

		placeholders := map[string]string{
			"cloud_provider":   instanceTransfer.SourceInstance.CloudProvider,
			"cloud_account_id": instanceTransfer.SourceInstance.CloudAccountID,
			"cloud_region":     instanceTransfer.SourceInstance.Region,
			"instance_id":      instanceTransfer.SourceInstance.ID,
		}

		targetDbTemplate := instanceTransfer.NamingConvention.DatabaseNameInSnowflake
		targetSchemaTemplate := instanceTransfer.NamingConvention.SchemaNameInSnowflake
		targetTableTemplate := instanceTransfer.NamingConvention.TableNameInSnowflake

		for _, schema := range schemas {
			treeNodeSchemaID := fmt.Sprintf("%v_%v", treeNodeDBID, schema)

			schemaNode := dbNode.AddChild(treeNodeSchemaID, schema)

			tables, err := dbSystem.listAllTablesInSchema(schema)
			if err != nil {
				return nil, fmt.Errorf("error listing all tables in schema %v :: %v", schema, err)
			}
			for _, table := range tables {
				treeNodeTableID := fmt.Sprintf("%v_%v", treeNodeSchemaID, table)

				tableNode := schemaNode.AddChild(treeNodeTableID, table)

				id := uuid.New().String()
				ctx, cancel := context.WithCancel(context.Background())

				placeholders["database_name"] = database
				placeholders["schema_name"] = schema
				placeholders["table_name"] = table

				pattern := regexp.MustCompile(`\[([^\]]+)\]`)

				targetDbName := pattern.ReplaceAllStringFunc(targetDbTemplate, func(m string) string {
					key := m[1 : len(m)-1]
					if val, found := placeholders[key]; found {
						return val
					}
					return m
				})

				targetDbName = dashToUnderscoreReplacer.Replace(targetDbName)

				targetSchemaName := pattern.ReplaceAllStringFunc(targetSchemaTemplate, func(m string) string {
					key := m[1 : len(m)-1]
					if val, found := placeholders[key]; found {
						return val
					}
					return m
				})

				targetSchemaName = dashToUnderscoreReplacer.Replace(targetSchemaName)

				if targetSchemaName == "" {
					targetSchemaName = instanceTransfer.NamingConvention.SchemaFallbackInSnowflake
				}

				targetTableName := pattern.ReplaceAllStringFunc(targetTableTemplate, func(m string) string {
					key := m[1 : len(m)-1]
					if val, found := placeholders[key]; found {
						return val
					}
					return m
				})

				targetTableName = dashToUnderscoreReplacer.Replace(targetTableName)

				// replace dashes with underscores in instanceTransferID
				stagingDbNameSuffix := strings.ReplaceAll(instanceTransfer.RestoredInstanceID, "-", "_")
				stagingDbName := fmt.Sprintf("%v_%v", stagingDbNameSuffix, database)

				transferInfo := &data.TransferInfo{
					ID:      id,
					Context: ctx,
					Cancel:  cancel,
					SourceInstance: data.Instance{
						ID:       instanceTransfer.SourceInstance.ID,
						Type:     instanceTransfer.SourceInstance.Type,
						Host:     instanceTransfer.SourceInstance.Host,
						Port:     instanceTransfer.SourceInstance.Port,
						Username: instanceTransfer.SourceInstance.Username,
						Password: instanceTransfer.SourceInstance.Password,
					},
					SourceDatabase:                database,
					SourceSchema:                  schema,
					SourceTable:                   table,
					TargetType:                    instanceTransfer.TargetType,
					TargetHost:                    instanceTransfer.TargetHost,
					TargetUsername:                instanceTransfer.TargetUsername,
					TargetPassword:                instanceTransfer.TargetPassword,
					TargetDatabase:                targetDbName,
					TargetSchema:                  targetSchemaName,
					TargetTable:                   targetTableName,
					DropTargetTableIfExists:       true,
					CreateTargetSchemaIfNotExists: true,
					CreateTargetTableIfNotExists:  true,
					Delimiter:                     instanceTransfer.Delimiter,
					Newline:                       instanceTransfer.Newline,
					Null:                          instanceTransfer.Null,
					PsqlAvailable:                 instanceTransfer.PsqlAvailable,
					BcpAvailable:                  instanceTransfer.BcpAvailable,
					SqlLdrAvailable:               instanceTransfer.SqlLdrAvailable,
					StagingDbName:                 stagingDbName,
					TableNode:                     tableNode,
					ScanForPII:                    instanceTransfer.ScanForPII,
				}
				instanceTransfer.TransferInfos = append(instanceTransfer.TransferInfos, transferInfo)
			}
		}
	}

	return instanceTransfer, nil
}

func (system Postgresql) createDbIfNotExistsOverride(database string) (overridden bool, err error) {
	return false, errors.New("not implemented")
}
