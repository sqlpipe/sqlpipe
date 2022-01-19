//go:build allDbs
// +build allDbs

package engine

import (
	"database/sql"
	"errors"
	"fmt"
	"sqlpipe/app/models"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

var mssql *sql.DB

type MSSQL struct {
	dsType          string
	driverName      string `json:"-"`
	connString      string `json:"-"`
	debugConnString string
}

func (dsConn MSSQL) GetDb() *sql.DB {
	return mssql
}

func getNewMSSQL(connection models.Connection) DsConnection {

	connString := fmt.Sprintf(
		"sqlserver://%s:%s@%s:%v?database=%s",
		connection.Username,
		connection.Password,
		connection.Hostname,
		connection.Port,
		connection.DbName,
	)

	var err error
	mssql, err = sql.Open("mssql", connString)

	if err != nil {
		fmt.Printf("\nCouldn't open a connection to MSSQL at host %s\n", connection.Hostname)
	}

	mssql.SetConnMaxLifetime(time.Minute * 1)

	return MSSQL{
		"mssql",
		"mssql",
		fmt.Sprintf(
			"sqlserver://%s:%s@%s:%v?database=%s",
			connection.Username,
			connection.Password,
			connection.Hostname,
			connection.Port,
			connection.DbName,
		),
		fmt.Sprintf(
			"sqlserver://<USERNAME_MASKED>:<PASSWORD_MASKED>@%s:%v?database=%s",
			connection.Hostname,
			connection.Port,
			connection.DbName,
		),
	}
}

func (dsConn MSSQL) getRows(
	transfer models.Transfer,
) (
	rows *sql.Rows,
	resultSetColumnInfo ResultSetColumnInfo,
	err error,
) {
	return standardGetRows(dsConn, transfer)
}

func (dsConn MSSQL) getFormattedResults(
	query string,
) (
	queryResults QueryResult,
	err error,
) {
	return standardGetFormattedResults(dsConn, query)
}

func (dsConn MSSQL) getIntermediateType(colTypeFromDriver string) string {
	return mssqlIntermediateTypes[colTypeFromDriver]
}

func (dsConn MSSQL) GetConnectionInfo() (dsType string, driveName string, connString string) {
	return dsConn.dsType, dsConn.driverName, dsConn.connString
}

func (dsConn MSSQL) GetDebugInfo() (string, string) {
	return dsConn.dsType, dsConn.debugConnString
}

func (dsConn MSSQL) turboTransfer(
	rows *sql.Rows,
	transfer models.Transfer,
	resultSetColumnInfo ResultSetColumnInfo,
) (err error) {
	return err
}

func (dsConn MSSQL) turboWriteMidVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) (
	err error,
) {
	return errors.New("We haven't implemented turbo write on MSSQL yet")
}

func (dsConn MSSQL) turboWriteEndVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) (
	err error,
) {
	return errors.New("We haven't implemented turbo write on MSSQL yet")
}

func (db MSSQL) insertChecker(currentLen int, currentRow int) bool {
	if currentRow%1000 == 0 {
		return true
	} else {
		return false
	}
}

func (dsConn MSSQL) dropTable(
	transfer models.Transfer,
) (
	err error,
) {
	return dropTableIfExistsWithSchema(dsConn, transfer)
}

func (dsConn MSSQL) deleteFromTable(
	transfer models.Transfer,
) (
	err error,
) {
	return deleteFromTableWithSchema(dsConn, transfer)
}

func (dsConn MSSQL) createTable(
	transfer models.Transfer,
	columnInfo ResultSetColumnInfo,
) (
	err error,
) {
	return standardCreateTable(dsConn, transfer, columnInfo)
}

func (dsConn MSSQL) getValToWriteMidRow(valType string, value interface{}) string {
	return mssqlValWriters[valType](value, ",")
}

func (dsConn MSSQL) getValToWriteRowEnd(valType string, value interface{}) string {
	return mssqlValWriters[valType](value, ")")
}

func (dsConn MSSQL) getRowStarter() string {
	return standardGetRowStarter()
}

func (dsConn MSSQL) getQueryStarter(targetTable string, columnInfo ResultSetColumnInfo) string {
	return standardGetQueryStarter(targetTable, columnInfo)
}

func mssqlWriteBit(value interface{}, terminator string) string {

	var returnVal string

	switch v := value.(type) {
	case bool:
		if v {
			returnVal = fmt.Sprintf("1%s", terminator)
		} else {
			returnVal = fmt.Sprintf("0%s", terminator)
		}
	default:
		return fmt.Sprintf("null%s", terminator)
	}
	return returnVal
}

func mssqlWriteHexBytes(value interface{}, terminator string) string {
	return fmt.Sprintf("CONVERT(VARBINARY(8000), '0x%x', 1)%s", value, terminator)
}

func mssqlWriteUniqueIdentifier(value interface{}, terminator string) string {
	// This is a really stupid fix but it works
	// https://github.com/denisenkom/go-mssqldb/issues/56
	// I guess the bits get shifted around in the first half of these bytes... whatever
	var returnVal string

	switch v := value.(type) {
	case []uint8:
		returnVal = fmt.Sprintf("N'%X%X%X%X-%X%X-%X%X-%X%X-%X'%s",
			v[3],
			v[2],
			v[1],
			v[0],
			v[5],
			v[4],
			v[7],
			v[6],
			v[8],
			v[9],
			v[10:],
			terminator,
		)
	default:
		return fmt.Sprintf("null%s", terminator)
	}
	return returnVal
}

func (dsConn MSSQL) getQueryEnder(targetTable string) string {
	return ""
}

func mssqlWriteDateTime(value interface{}, terminator string) string {
	var returnVal string

	switch v := value.(type) {
	case time.Time:
		returnVal = fmt.Sprintf("CONVERT(DATETIME2, '%s', 121)%s", v.Format("2006-01-02 15:04:05.0000000"), terminator)
	default:
		return fmt.Sprintf("null%s", terminator)
	}

	return returnVal
}

func mssqlWriteDateTimeWithTZ(value interface{}, terminator string) string {
	var returnVal string

	switch v := value.(type) {
	case time.Time:
		returnVal = fmt.Sprintf("CONVERT(DATETIMEOFFSET, '%s', 121)%s", v.Format("2006-01-02 15:04:05.000 -07:00"), terminator)
	default:
		return fmt.Sprintf("null%s", terminator)
	}

	return returnVal
}

func mssqlWriteTime(value interface{}, terminator string) string {
	var returnVal string

	switch v := value.(type) {
	case time.Time:
		returnVal = fmt.Sprintf("CONVERT(TIME, '%s', 121)%s", v.Format("15:04:05.000"), terminator)
	default:
		return fmt.Sprintf("null%s", terminator)
	}

	return returnVal
}

func (dsConn MSSQL) getCreateTableType(intermediateType string) string {
	return mssqlCreateTableTypes[resultSetColInfo.ColumnIntermediateTypes[colNum]](resultSetColInfo, colNum)
}

var mssqlIntermediateTypes = map[string]string{
	"BIGINT":           "MSSQL_BIGINT_int64",
	"BIT":              "MSSQL_BIT_bool",
	"DECIMAL":          "MSSQL_DECIMAL_[]uint8",
	"INT":              "MSSQL_INT_int64",
	"MONEY":            "MSSQL_MONEY_[]uint8",
	"SMALLINT":         "MSSQL_SMALLINT_int64",
	"SMALLMONEY":       "MSSQL_SMALLMONEY_[]uint8",
	"TINYINT":          "MSSQL_TINYINT_int64",
	"FLOAT":            "MSSQL_FLOAT_float64",
	"REAL":             "MSSQL_REAL_float64",
	"DATE":             "MSSQL_DATE_time.Time",
	"DATETIME2":        "MSSQL_DATETIME2_time.Time",
	"DATETIME":         "MSSQL_DATETIME_time.Time",
	"DATETIMEOFFSET":   "MSSQL_DATETIMEOFFSET_time.Time",
	"SMALLDATETIME":    "MSSQL_SMALLDATETIME_time.Time",
	"TIME":             "MSSQL_TIME_time.Time",
	"CHAR":             "MSSQL_CHAR_string",
	"VARCHAR":          "MSSQL_VARCHAR_string",
	"TEXT":             "MSSQL_TEXT_string",
	"NCHAR":            "MSSQL_NCHAR_string",
	"NVARCHAR":         "MSSQL_NVARCHAR_string",
	"NTEXT":            "MSSQL_NTEXT_string",
	"BINARY":           "MSSQL_BINARY_[]uint8",
	"VARBINARY":        "MSSQL_VARBINARY_[]uint8",
	"UNIQUEIDENTIFIER": "MSSQL_UNIQUEIDENTIFIER_[]uint8",
	"XML":              "MSSQL_XML_string",
}

var mssqlCreateTableTypes = map[string]func(columnInfo ResultSetColumnInfo, colNum int) string{

	// MSSQL
	"MSSQL_BIGINT_int64":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MSSQL_BIT_bool":                 func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIT" },
	"MSSQL_INT_int64":                func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MSSQL_MONEY_[]uint8":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"MSSQL_SMALLINT_int64":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MSSQL_SMALLMONEY_[]uint8":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"MSSQL_TINYINT_int64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "TINYINT" },
	"MSSQL_FLOAT_float64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MSSQL_REAL_float64":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"MSSQL_DATE_time.Time":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MSSQL_DATETIME2_time.Time":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME2" },
	"MSSQL_DATETIME_time.Time":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"MSSQL_DATETIMEOFFSET_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIMEOFFSET" },
	"MSSQL_SMALLDATETIME_time.Time":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLDATETIME" },
	"MSSQL_TIME_time.Time":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MSSQL_TEXT_string":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_NTEXT_string":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "NTEXT" },
	"MSSQL_BINARY_[]uint8":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARBINARY(8000)" },
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": func(columnInfo ResultSetColumnInfo, colNum int) string { return "UNIQUEIDENTIFIER" },
	"MSSQL_XML_string":               func(columnInfo ResultSetColumnInfo, colNum int) string { return "XML" },
	"MSSQL_DECIMAL_[]uint8": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"DECIMAL(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},
	"MSSQL_CHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"CHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_VARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_NCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_NVARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_VARBINARY_[]uint8": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARBINARY(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// PostgreSQL

	"PostgreSQL_BIGINT_int64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"PostgreSQL_BIT_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_VARBIT_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_BOOLEAN_bool":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIT" },
	"PostgreSQL_BOX_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_BYTEA_[]uint8":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARBINARY(8000)" },
	"PostgreSQL_BPCHAR_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"PostgreSQL_CIDR_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_CIRCLE_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_DATE_time.Time":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"PostgreSQL_FLOAT8_float64":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"PostgreSQL_INET_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_INT4_int32":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"PostgreSQL_INTERVAL_string":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_JSON_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"PostgreSQL_JSONB_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"PostgreSQL_LINE_string":           func(ColumnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_LSEG_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_MACADDR_string":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_MONEY_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_PATH_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_PG_LSN_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_POINT_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_POLYGON_string":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_FLOAT4_float32":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"PostgreSQL_INT2_int16":            func(ColumnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"PostgreSQL_TEXT_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NTEXT" },
	"PostgreSQL_TIME_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"PostgreSQL_TIMETZ_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_TIMESTAMP_time.Time":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME2" },
	"PostgreSQL_TIMESTAMPTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIMEOFFSET" },
	"PostgreSQL_TSQUERY_string":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"PostgreSQL_TSVECTOR_string":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"PostgreSQL_TXID_SNAPSHOT_string":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"PostgreSQL_UUID_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "UNIQUEIDENTIFIER" },
	"PostgreSQL_XML_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "XML" },
	"PostgreSQL_VARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"PostgreSQL_DECIMAL_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"FLOAT",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MySQL

	"MySQL_BIT_sql.RawBytes":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(8000)" },
	"MySQL_TINYINT_sql.RawBytes":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TINYINT" },
	"MySQL_SMALLINT_sql.RawBytes":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MySQL_MEDIUMINT_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_INT_sql.RawBytes":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_BIGINT_sql.NullInt64":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MySQL_FLOAT4_sql.NullFloat64": func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"MySQL_FLOAT8_sql.NullFloat64": func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MySQL_DATE_sql.NullTime":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MySQL_TIME_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MySQL_DATETIME_sql.NullTime":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME2" },
	"MySQL_TIMESTAMP_sql.NullTime": func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME2" },
	"MySQL_YEAR_sql.NullInt64":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_CHAR_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(255)" },
	"MySQL_VARCHAR_sql.RawBytes":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"MySQL_TEXT_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NTEXT" },
	"MySQL_BINARY_sql.RawBytes":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARBINARY(255)" },
	"MySQL_VARBINARY_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARBINARY(8000)" },
	"MySQL_BLOB_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARBINARY(8000)" },
	"MySQL_GEOMETRY_sql.RawBytes":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARBINARY(8000)" },
	"MySQL_JSON_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"MySQL_DECIMAL_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"DECIMAL(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// Oracle

	"Oracle_OCIClobLocator_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"Oracle_OCIBlobLocator_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARBINARY(8000)" },
	"Oracle_LONG_interface{}":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NTEXT" },
	"Oracle_NUMBER_interface{}":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Oracle_DATE_interface{}":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Oracle_TimeStampDTY_interface{}":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME2" },
	"Oracle_CHAR_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"Oracle_NCHAR_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// SNOWFLAKE

	"Snowflake_NUMBER_float64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Snowflake_BINARY_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARBINARY(8000)" },
	"Snowflake_REAL_float64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Snowflake_TEXT_string":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"Snowflake_BOOLEAN_bool":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIT" },
	"Snowflake_DATE_time.Time":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Snowflake_TIME_time.Time":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"Snowflake_TIMESTAMP_LTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIMEOFFSET" },
	"Snowflake_TIMESTAMP_NTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME2" },
	"Snowflake_TIMESTAMP_TZ_time.Time":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIMEOFFSET" },
	"Snowflake_VARIANT_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"Snowflake_OBJECT_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"Snowflake_ARRAY_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },

	// Redshift

	"Redshift_BIGINT_int64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"Redshift_BOOLEAN_bool":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIT" },
	"Redshift_CHAR_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"Redshift_BPCHAR_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Redshift_DATE_time.Time":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Redshift_DOUBLE_float64":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Redshift_INT_int32":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"Redshift_REAL_float32":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"Redshift_SMALLINT_int16":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"Redshift_TIME_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"Redshift_TIMETZ_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(4000)" },
	"Redshift_TIMESTAMP_time.Time":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME2" },
	"Redshift_TIMESTAMPTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIMEOFFSET" },
	"Redshift_NUMERIC_float64":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Redshift_VARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
}

var mssqlValWriters = map[string]func(value interface{}, terminator string) string{

	// PostgreSQL

	"PostgreSQL_BIGINT_int64":          writeInsertInt,
	"PostgreSQL_BIT_string":            writeInsertStringNoEscape,
	"PostgreSQL_VARBIT_string":         writeInsertStringNoEscape,
	"PostgreSQL_BOOLEAN_bool":          mssqlWriteBit,
	"PostgreSQL_BOX_string":            writeInsertStringNoEscape,
	"PostgreSQL_BYTEA_[]uint8":         mssqlWriteHexBytes,
	"PostgreSQL_CIDR_string":           writeInsertStringNoEscape,
	"PostgreSQL_CIRCLE_string":         writeInsertStringNoEscape,
	"PostgreSQL_FLOAT8_float64":        writeInsertFloat,
	"PostgreSQL_INET_string":           writeInsertStringNoEscape,
	"PostgreSQL_INT4_int32":            writeInsertInt,
	"PostgreSQL_INTERVAL_string":       writeInsertStringNoEscape,
	"PostgreSQL_LINE_string":           writeInsertStringNoEscape,
	"PostgreSQL_LSEG_string":           writeInsertStringNoEscape,
	"PostgreSQL_MACADDR_string":        writeInsertStringNoEscape,
	"PostgreSQL_MONEY_string":          writeInsertStringNoEscape,
	"PostgreSQL_DECIMAL_string":        writeInsertRawStringNoQuotes,
	"PostgreSQL_PATH_string":           writeInsertStringNoEscape,
	"PostgreSQL_PG_LSN_string":         writeInsertStringNoEscape,
	"PostgreSQL_POINT_string":          writeInsertStringNoEscape,
	"PostgreSQL_POLYGON_string":        writeInsertStringNoEscape,
	"PostgreSQL_FLOAT4_float32":        writeInsertFloat,
	"PostgreSQL_INT2_int16":            writeInsertInt,
	"PostgreSQL_TIME_string":           writeInsertStringNoEscape,
	"PostgreSQL_TIMETZ_string":         writeInsertStringNoEscape,
	"PostgreSQL_TXID_SNAPSHOT_string":  writeInsertStringNoEscape,
	"PostgreSQL_UUID_string":           writeInsertStringNoEscape,
	"PostgreSQL_VARCHAR_string":        writeInsertEscapedString,
	"PostgreSQL_BPCHAR_string":         writeInsertEscapedString,
	"PostgreSQL_DATE_time.Time":        mssqlWriteDateTime,
	"PostgreSQL_JSON_string":           writeInsertEscapedString,
	"PostgreSQL_JSONB_string":          writeInsertEscapedString,
	"PostgreSQL_TEXT_string":           writeInsertEscapedString,
	"PostgreSQL_TIMESTAMP_time.Time":   mssqlWriteDateTime,
	"PostgreSQL_TIMESTAMPTZ_time.Time": mssqlWriteDateTimeWithTZ,
	"PostgreSQL_TSQUERY_string":        writeInsertEscapedString,
	"PostgreSQL_TSVECTOR_string":       writeInsertEscapedString,
	"PostgreSQL_XML_string":            writeInsertEscapedString,

	// MYSQL

	"MySQL_BIT_sql.RawBytes":       writeInsertBinaryString,
	"MySQL_TINYINT_sql.RawBytes":   writeInsertRawStringNoQuotes,
	"MySQL_SMALLINT_sql.RawBytes":  writeInsertRawStringNoQuotes,
	"MySQL_MEDIUMINT_sql.RawBytes": writeInsertRawStringNoQuotes,
	"MySQL_INT_sql.RawBytes":       writeInsertRawStringNoQuotes,
	"MySQL_BIGINT_sql.NullInt64":   writeInsertRawStringNoQuotes,
	"MySQL_DECIMAL_sql.RawBytes":   writeInsertRawStringNoQuotes,
	"MySQL_FLOAT4_sql.NullFloat64": writeInsertRawStringNoQuotes,
	"MySQL_FLOAT8_sql.NullFloat64": writeInsertRawStringNoQuotes,
	"MySQL_DATE_sql.NullTime":      writeInsertStringNoEscape,
	"MySQL_TIME_sql.RawBytes":      writeInsertStringNoEscape,
	"MySQL_TIMESTAMP_sql.NullTime": writeInsertStringNoEscape,
	"MySQL_DATETIME_sql.NullTime":  writeInsertStringNoEscape,
	"MySQL_YEAR_sql.NullInt64":     writeInsertRawStringNoQuotes,
	"MySQL_CHAR_sql.RawBytes":      writeInsertEscapedString,
	"MySQL_VARCHAR_sql.RawBytes":   writeInsertEscapedString,
	"MySQL_TEXT_sql.RawBytes":      writeInsertEscapedString,
	"MySQL_BINARY_sql.RawBytes":    mssqlWriteHexBytes,
	"MySQL_VARBINARY_sql.RawBytes": mssqlWriteHexBytes,
	"MySQL_BLOB_sql.RawBytes":      mssqlWriteHexBytes,
	"MySQL_GEOMETRY_sql.RawBytes":  mssqlWriteHexBytes,
	"MySQL_JSON_sql.RawBytes":      writeInsertEscapedString,

	// MSSQL

	"MSSQL_BIGINT_int64":             writeInsertInt,
	"MSSQL_BIT_bool":                 mssqlWriteBit,
	"MSSQL_DECIMAL_[]uint8":          writeInsertRawStringNoQuotes,
	"MSSQL_INT_int64":                writeInsertInt,
	"MSSQL_MONEY_[]uint8":            writeInsertStringNoEscape,
	"MSSQL_SMALLINT_int64":           writeInsertInt,
	"MSSQL_SMALLMONEY_[]uint8":       writeInsertStringNoEscape,
	"MSSQL_TINYINT_int64":            writeInsertInt,
	"MSSQL_FLOAT_float64":            writeInsertFloat,
	"MSSQL_REAL_float64":             writeInsertFloat,
	"MSSQL_DATE_time.Time":           mssqlWriteDateTime,
	"MSSQL_DATETIME2_time.Time":      mssqlWriteDateTime,
	"MSSQL_DATETIME_time.Time":       mssqlWriteDateTime,
	"MSSQL_DATETIMEOFFSET_time.Time": mssqlWriteDateTime,
	"MSSQL_SMALLDATETIME_time.Time":  mssqlWriteDateTime,
	"MSSQL_TIME_time.Time":           mssqlWriteTime,
	"MSSQL_CHAR_string":              writeInsertEscapedString,
	"MSSQL_VARCHAR_string":           writeInsertEscapedString,
	"MSSQL_TEXT_string":              writeInsertEscapedString,
	"MSSQL_NCHAR_string":             writeInsertEscapedString,
	"MSSQL_NVARCHAR_string":          writeInsertEscapedString,
	"MSSQL_NTEXT_string":             writeInsertEscapedString,
	"MSSQL_BINARY_[]uint8":           mssqlWriteHexBytes,
	"MSSQL_VARBINARY_[]uint8":        mssqlWriteHexBytes,
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": mssqlWriteUniqueIdentifier,
	"MSSQL_XML_string":               writeInsertEscapedString,

	// Oracle

	"Oracle_CHAR_interface{}":           writeInsertEscapedString,
	"Oracle_NCHAR_interface{}":          writeInsertEscapedString,
	"Oracle_OCIClobLocator_interface{}": writeInsertEscapedString,
	"Oracle_OCIBlobLocator_interface{}": mssqlWriteHexBytes,
	"Oracle_LONG_interface{}":           writeInsertEscapedString,
	"Oracle_NUMBER_interface{}":         oracleWriteNumber,
	"Oracle_DATE_interface{}":           mssqlWriteDateTime,
	"Oracle_TimeStampDTY_interface{}":   mssqlWriteDateTime,

	// Snowflake

	"Snowflake_NUMBER_float64":          writeInsertRawStringNoQuotes,
	"Snowflake_REAL_float64":            writeInsertRawStringNoQuotes,
	"Snowflake_TEXT_string":             writeInsertEscapedString,
	"Snowflake_BOOLEAN_bool":            writeInsertStringNoEscape,
	"Snowflake_DATE_time.Time":          mssqlWriteDateTime,
	"Snowflake_TIME_time.Time":          mssqlWriteTime,
	"Snowflake_TIMESTAMP_LTZ_time.Time": mssqlWriteDateTime,
	"Snowflake_TIMESTAMP_NTZ_time.Time": mssqlWriteDateTime,
	"Snowflake_TIMESTAMP_TZ_time.Time":  mssqlWriteDateTime,
	"Snowflake_VARIANT_string":          writeInsertEscapedStringRemoveNewines,
	"Snowflake_OBJECT_string":           writeInsertEscapedStringRemoveNewines,
	"Snowflake_ARRAY_string":            writeInsertEscapedStringRemoveNewines,
	"Snowflake_BINARY_string":           mssqlWriteHexBytes,

	// Redshift

	"Redshift_BIGINT_int64":          writeInsertInt,
	"Redshift_BOOLEAN_bool":          mssqlWriteBit,
	"Redshift_CHAR_string":           writeInsertEscapedString,
	"Redshift_VARCHAR_string":        writeInsertEscapedString,
	"Redshift_BPCHAR_string":         writeInsertEscapedString,
	"Redshift_DATE_time.Time":        mssqlWriteDateTime,
	"Redshift_DOUBLE_float64":        writeInsertFloat,
	"Redshift_INT_int32":             writeInsertInt,
	"Redshift_NUMERIC_float64":       writeInsertRawStringNoQuotes,
	"Redshift_REAL_float32":          writeInsertFloat,
	"Redshift_SMALLINT_int16":        writeInsertInt,
	"Redshift_TIME_string":           writeInsertStringNoEscape,
	"Redshift_TIMETZ_string":         writeInsertStringNoEscape,
	"Redshift_TIMESTAMP_time.Time":   mssqlWriteDateTime,
	"Redshift_TIMESTAMPTZ_time.Time": mssqlWriteDateTime,
}
