//go:build allDbs
// +build allDbs

package engine

import (
	"database/sql"
	"fmt"
	"sqlpipe/app/models"
	"sqlpipe/global/structs"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var mysql *sql.DB
var mysqlDsInfo structs.DsInfo

type MySQL struct {
	dsType          string
	driverName      string `json:"-"`
	connString      string `json:"-"`
	debugConnString string
	canConnect      bool
}

func (dsConn MySQL) GetDb() *sql.DB {
	return mysql
}

func (dsConn MySQL) SetCanConnect(canConnect bool) {
	dsConn.canConnect = canConnect
}

func (dsConn MySQL) GetDsInfo() structs.DsInfo {
	return mysqlDsInfo
}

func getNewMySQL(connection models.Connection) (dsConn DsConnection) {

	connString := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s",
		connection.Username,
		connection.Password,
		connection.Hostname,
		connection.Port,
		connection.DbName,
	)

	var err error
	mysql, err = sql.Open("mysql", connString)

	if err != nil {
		panic(fmt.Sprintf("couldn't open a connection to MySQL at host %s", connection.Hostname))
	}

	mysql.SetConnMaxLifetime(time.Minute * 1)

	return MySQL{
		"mysql",
		"mysql",
		fmt.Sprintf(
			"%s:%s@tcp(%s:%s)/%s",
			connection.Username,
			connection.Password,
			connection.Hostname,
			connection.Port,
			connection.DbName,
		),
		fmt.Sprintf(
			"<USERNAME_MASKED>:<PASSWORD_MASKED>@tcp(%s:%s)/%s",
			connection.Hostname,
			connection.Port,
			connection.DbName,
		),
		false,
	}
}

func (dsConn MySQL) getQueryEnder(targetTable string) string {
	return ""
}

func (dsConn MySQL) getIntermediateType(colTypeFromDriver string) string {
	return mysqlIntermediateTypes[colTypeFromDriver]
}

func (dsConn MySQL) getFormattedResults(query string) structs.QueryResult {
	return standardGetFormattedResults(dsConn, queryInfo)
}

func (dsConn MySQL) getRows(transfer models.Transfer) (*sql.Rows, structs.ResultSetColumnInfo) {
	return standardGetRows(dsConn, transfer)
}

func (dsConn MySQL) GetConnectionInfo() (string, string, string) {
	return dsConn.dsType, dsConn.driverName, dsConn.connString
}

func (dsConn MySQL) GetDebugInfo() (string, string) {
	return dsConn.dsType, dsConn.debugConnString
}

func (db MySQL) insertChecker(currentLen int, currentRow int) bool {
	if currentLen > 4000000 {
		return true
	} else {
		return false
	}
}

func (dsConn MySQL) dropTable(transfer models.Transfer) {
	dropTableIfExistsNoSchema(dsConn, transfer)
}

func (dsConn MySQL) turboTransfer(
	rows *sql.Rows,
	transfer models.Transfer,
	resultSetColumnInfo structs.ResultSetColumnInfo,
) (err error) {
	return err
}

func (dsConn MySQL) turboWriteMidVal(valType string, value interface{}, builder *strings.Builder) {
	panic("postgres hasn't implemented turbo write yet")
}

func (dsConn MySQL) turboWriteEndVal(valType string, value interface{}, builder *strings.Builder) {
	panic("postgres hasn't implemented turbo write yet")
}

func (dsConn MySQL) deleteFromTable(transfer models.Transfer) {
	deleteFromTableNoSchema(dsConn, transfer)
}

func (dsConn MySQL) createTable(transfer models.Transfer, columnInfo structs.ResultSetColumnInfo) string {
	// MySQL doesn't really have schemas
	transfer.TargetSchema = ""
	return standardCreateTable(dsConn, transfer, columnInfo)
}

func (dsConn MySQL) getValToWriteMidRow(valType string, value interface{}) string {
	return mysqlInsertWriters[valType](value, ",")
}

func (dsConn MySQL) getValToWriteRowEnd(valType string, value interface{}) string {
	return mysqlInsertWriters[valType](value, ")")
}

func (dsConn MySQL) getRowStarter() string {
	return standardGetRowStarter()
}

func (dsConn MySQL) getQueryStarter(targetTable string, columnInfo structs.ResultSetColumnInfo) string {
	return standardGetQueryStarter(targetTable, columnInfo)
}

func mysqlWriteInsertBinary(value interface{}, terminator string) string {
	return fmt.Sprintf("x'%x'%s", value, terminator)
}

func mysqlWriteDateTime(value interface{}, terminator string) string {
	var returnVal string
	switch v := value.(type) {
	case time.Time:
		returnVal = fmt.Sprintf("'%s'%s", v.Format("2006-01-02 15:04:05.000000-07:00"), terminator)
	default:
		return fmt.Sprintf("null%s", terminator)
	}
	return returnVal
}

func (dsConn MySQL) getCreateTableType(resultSetColInfo structs.ResultSetColumnInfo, colNum int) string {
	return mysqlCreateTableTypes[resultSetColInfo.ColumnIntermediateTypes[colNum]](resultSetColInfo, colNum)
}

var mysqlIntermediateTypes = map[string]string{
	"BIT":       "MySQL_BIT_sql.RawBytes",
	"TINYINT":   "MySQL_TINYINT_sql.RawBytes",
	"SMALLINT":  "MySQL_SMALLINT_sql.RawBytes",
	"MEDIUMINT": "MySQL_MEDIUMINT_sql.RawBytes",
	"INT":       "MySQL_INT_sql.RawBytes",
	"BIGINT":    "MySQL_BIGINT_sql.NullInt64",
	"DECIMAL":   "MySQL_DECIMAL_sql.RawBytes",
	"FLOAT":     "MySQL_FLOAT4_sql.NullFloat64",
	"DOUBLE":    "MySQL_FLOAT8_sql.NullFloat64",
	"DATE":      "MySQL_DATE_sql.NullTime",
	"TIME":      "MySQL_TIME_sql.RawBytes",
	"DATETIME":  "MySQL_DATETIME_sql.NullTime",
	"TIMESTAMP": "MySQL_TIMESTAMP_sql.NullTime",
	"YEAR":      "MySQL_YEAR_sql.NullInt64",
	"CHAR":      "MySQL_CHAR_sql.RawBytes",
	"VARCHAR":   "MySQL_VARCHAR_sql.RawBytes",
	"TEXT":      "MySQL_TEXT_sql.RawBytes",
	"BINARY":    "MySQL_BINARY_sql.RawBytes",
	"VARBINARY": "MySQL_VARBINARY_sql.RawBytes",
	"BLOB":      "MySQL_BLOB_sql.RawBytes",
	"GEOMETRY":  "MySQL_GEOMETRY_sql.RawBytes",
	"JSON":      "MySQL_JSON_sql.RawBytes",
}

var mysqlCreateTableTypes = map[string]func(columnInfo structs.ResultSetColumnInfo, colNum int) string{

	// MySQL

	"MySQL_BIT_sql.RawBytes":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BIT(64)" },
	"MySQL_TINYINT_sql.RawBytes":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TINYINT" },
	"MySQL_SMALLINT_sql.RawBytes":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MySQL_MEDIUMINT_sql.RawBytes": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "MEDIUMINT" },
	"MySQL_INT_sql.RawBytes":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_FLOAT4_sql.NullFloat64": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MySQL_FLOAT8_sql.NullFloat64": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"MySQL_DATE_sql.NullTime":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MySQL_TIME_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MySQL_DATETIME_sql.NullTime":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"MySQL_TIMESTAMP_sql.NullTime": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_YEAR_sql.NullInt64":     func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "YEAR" },
	"MySQL_CHAR_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"MySQL_VARCHAR_sql.RawBytes":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"MySQL_TEXT_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"MySQL_BINARY_sql.RawBytes":    func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"MySQL_VARBINARY_sql.RawBytes": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"MySQL_BLOB_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"MySQL_GEOMETRY_sql.RawBytes":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "GEOMETRY" },
	"MySQL_JSON_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "JSON" },
	"MySQL_BIGINT_sql.NullInt64":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MySQL_DECIMAL_sql.RawBytes": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"DECIMAL(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// PostgreSQL

	"PostgreSQL_BIGINT_int64":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"PostgreSQL_BIT_string":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_VARBIT_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_BOOLEAN_bool":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"PostgreSQL_BOX_string":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_BYTEA_[]uint8":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"PostgreSQL_BPCHAR_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"PostgreSQL_CIDR_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_CIRCLE_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_DATE_time.Time":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"PostgreSQL_FLOAT8_float64":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"PostgreSQL_INET_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_INT4_int32":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"PostgreSQL_INTERVAL_string":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_JSON_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "JSON" },
	"PostgreSQL_JSONB_string":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "JSON" },
	"PostgreSQL_LINE_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_LSEG_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_MACADDR_string":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_MONEY_string":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_PATH_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_PG_LSN_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_POINT_string":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_POLYGON_string":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_FLOAT4_float32":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"PostgreSQL_INT2_int16":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"PostgreSQL_TEXT_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"PostgreSQL_TIME_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIME" },
	"PostgreSQL_TIMETZ_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_TIMESTAMP_time.Time":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"PostgreSQL_TIMESTAMPTZ_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"PostgreSQL_TSQUERY_string":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_TSVECTOR_string":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_TXID_SNAPSHOT_string":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_UUID_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_XML_string":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_VARCHAR_string": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"PostgreSQL_DECIMAL_string": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMERIC(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MSSQL

	"MSSQL_BIGINT_int64":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MSSQL_BIT_bool":                 func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BOOL" },
	"MSSQL_INT_int64":                func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"MSSQL_SMALLINT_int64":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MSSQL_TINYINT_int64":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TINYINT" },
	"MSSQL_FLOAT_float64":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"MSSQL_REAL_float64":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MSSQL_DATE_time.Time":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MSSQL_DATETIME2_time.Time":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"MSSQL_DATETIME_time.Time":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"MSSQL_DATETIMEOFFSET_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"MSSQL_SMALLDATETIME_time.Time":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"MSSQL_TIME_time.Time":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MSSQL_TEXT_string":              func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_NTEXT_string":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"MSSQL_BINARY_[]uint8":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"MSSQL_VARBINARY_[]uint8":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_XML_string":               func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"MSSQL_MONEY_[]uint8":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_SMALLMONEY_[]uint8":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_DECIMAL_[]uint8": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"DECIMAL(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},
	"MSSQL_CHAR_string": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"CHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_VARCHAR_string": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_NCHAR_string": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_NVARCHAR_string": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// Oracle

	"Oracle_OCIClobLocator_interface{}": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Oracle_OCIBlobLocator_interface{}": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"Oracle_LONG_interface{}":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Oracle_NUMBER_interface{}":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"Oracle_DATE_interface{}":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Oracle_TimeStampDTY_interface{}":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Oracle_CHAR_interface{}": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"Oracle_NCHAR_interface{}": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// SNOWFLAKE

	"Snowflake_NUMBER_float64":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"Snowflake_BINARY_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"Snowflake_REAL_float64":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"Snowflake_TEXT_string":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Snowflake_BOOLEAN_bool":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Snowflake_DATE_time.Time":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Snowflake_TIME_time.Time":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIME" },
	"Snowflake_TIMESTAMP_LTZ_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Snowflake_TIMESTAMP_NTZ_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Snowflake_TIMESTAMP_TZ_time.Time":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Snowflake_VARIANT_string":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Snowflake_OBJECT_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Snowflake_ARRAY_string":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },

	// Redshift

	"Redshift_BIGINT_int64":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"Redshift_BOOLEAN_bool":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Redshift_CHAR_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Redshift_DATE_time.Time":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Redshift_DOUBLE_float64":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"Redshift_INT_int32":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"Redshift_NUMERIC_float64":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"Redshift_REAL_float32":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "REAL" },
	"Redshift_SMALLINT_int16":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"Redshift_TIME_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIME" },
	"Redshift_TIMETZ_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Redshift_TIMESTAMP_time.Time":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Redshift_TIMESTAMPTZ_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Redshift_BPCHAR_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Redshift_VARCHAR_string": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
}

var mysqlInsertWriters = map[string]func(value interface{}, terminator string) string{

	// POSTGRESQL

	"PostgreSQL_BIGINT_int64":          writeInsertInt,
	"PostgreSQL_BIT_string":            writeInsertStringNoEscape,
	"PostgreSQL_VARBIT_string":         writeInsertStringNoEscape,
	"PostgreSQL_BOOLEAN_bool":          writeInsertBool,
	"PostgreSQL_BOX_string":            writeInsertStringNoEscape,
	"PostgreSQL_BYTEA_[]uint8":         mysqlWriteInsertBinary,
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
	"PostgreSQL_DECIMAL_string":        writeInsertStringNoEscape,
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
	"PostgreSQL_DATE_time.Time":        mysqlWriteDateTime,
	"PostgreSQL_JSON_string":           writeBackslashJSON,
	"PostgreSQL_JSONB_string":          writeBackslashJSON,
	"PostgreSQL_TEXT_string":           writeInsertEscapedString,
	"PostgreSQL_TIMESTAMP_time.Time":   mysqlWriteDateTime,
	"PostgreSQL_TIMESTAMPTZ_time.Time": mysqlWriteDateTime,
	"PostgreSQL_TSQUERY_string":        writeInsertEscapedString,
	"PostgreSQL_TSVECTOR_string":       writeInsertEscapedString,
	"PostgreSQL_XML_string":            writeInsertEscapedString,

	// MYSQL

	"MySQL_BIT_sql.RawBytes":       mysqlWriteInsertBinary,
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
	"MySQL_BINARY_sql.RawBytes":    mysqlWriteInsertBinary,
	"MySQL_VARBINARY_sql.RawBytes": mysqlWriteInsertBinary,
	"MySQL_BLOB_sql.RawBytes":      mysqlWriteInsertBinary,
	"MySQL_GEOMETRY_sql.RawBytes":  writeInsertStringNoEscape,
	"MySQL_JSON_sql.RawBytes":      writeBackslashJSON,

	// MSSQL

	"MSSQL_BIGINT_int64":             writeInsertInt,
	"MSSQL_BIT_bool":                 writeInsertBool,
	"MSSQL_DECIMAL_[]uint8":          writeInsertRawStringNoQuotes,
	"MSSQL_INT_int64":                writeInsertInt,
	"MSSQL_MONEY_[]uint8":            writeInsertStringNoEscape,
	"MSSQL_SMALLINT_int64":           writeInsertInt,
	"MSSQL_SMALLMONEY_[]uint8":       writeInsertStringNoEscape,
	"MSSQL_TINYINT_int64":            writeInsertInt,
	"MSSQL_FLOAT_float64":            writeInsertFloat,
	"MSSQL_REAL_float64":             writeInsertFloat,
	"MSSQL_DATE_time.Time":           mysqlWriteDateTime,
	"MSSQL_DATETIME2_time.Time":      mysqlWriteDateTime,
	"MSSQL_DATETIME_time.Time":       mysqlWriteDateTime,
	"MSSQL_DATETIMEOFFSET_time.Time": mysqlWriteDateTime,
	"MSSQL_SMALLDATETIME_time.Time":  mysqlWriteDateTime,
	"MSSQL_TIME_time.Time":           mysqlWriteDateTime,
	"MSSQL_CHAR_string":              writeInsertEscapedString,
	"MSSQL_VARCHAR_string":           writeInsertEscapedString,
	"MSSQL_TEXT_string":              writeInsertEscapedString,
	"MSSQL_NCHAR_string":             writeInsertEscapedString,
	"MSSQL_NVARCHAR_string":          writeInsertEscapedString,
	"MSSQL_NTEXT_string":             writeInsertEscapedString,
	"MSSQL_BINARY_[]uint8":           mysqlWriteInsertBinary,
	"MSSQL_VARBINARY_[]uint8":        mysqlWriteInsertBinary,
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": writeMSSQLUniqueIdentifier,
	"MSSQL_XML_string":               writeInsertEscapedString,

	// Oracle

	"Oracle_CHAR_interface{}":           writeInsertEscapedString,
	"Oracle_NCHAR_interface{}":          writeInsertEscapedString,
	"Oracle_OCIClobLocator_interface{}": writeInsertEscapedString,
	"Oracle_OCIBlobLocator_interface{}": mysqlWriteInsertBinary,
	"Oracle_LONG_interface{}":           writeInsertEscapedString,
	"Oracle_NUMBER_interface{}":         oracleWriteNumber,
	"Oracle_DATE_interface{}":           mysqlWriteDateTime,
	"Oracle_TimeStampDTY_interface{}":   mysqlWriteDateTime,

	// Snowflake

	"Snowflake_NUMBER_float64":          writeInsertRawStringNoQuotes,
	"Snowflake_REAL_float64":            writeInsertRawStringNoQuotes,
	"Snowflake_TEXT_string":             writeInsertEscapedString,
	"Snowflake_BOOLEAN_bool":            writeInsertStringNoEscape,
	"Snowflake_DATE_time.Time":          mysqlWriteDateTime,
	"Snowflake_TIME_time.Time":          snowflakeWriteTimeFromTime,
	"Snowflake_TIMESTAMP_LTZ_time.Time": mysqlWriteDateTime,
	"Snowflake_TIMESTAMP_NTZ_time.Time": mysqlWriteDateTime,
	"Snowflake_TIMESTAMP_TZ_time.Time":  mysqlWriteDateTime,
	"Snowflake_VARIANT_string":          writeInsertEscapedStringRemoveNewines,
	"Snowflake_OBJECT_string":           writeInsertEscapedStringRemoveNewines,
	"Snowflake_ARRAY_string":            writeInsertEscapedStringRemoveNewines,
	"Snowflake_BINARY_string":           mysqlWriteInsertBinary,

	// Redshift

	"Redshift_BIGINT_int64":          writeInsertInt,
	"Redshift_BOOLEAN_bool":          writeInsertBool,
	"Redshift_CHAR_string":           writeInsertEscapedString,
	"Redshift_BPCHAR_string":         writeInsertEscapedString,
	"Redshift_VARCHAR_string":        writeInsertEscapedString,
	"Redshift_DATE_time.Time":        mysqlWriteDateTime,
	"Redshift_DOUBLE_float64":        writeInsertFloat,
	"Redshift_INT_int32":             writeInsertInt,
	"Redshift_NUMERIC_float64":       writeInsertRawStringNoQuotes,
	"Redshift_REAL_float32":          writeInsertFloat,
	"Redshift_SMALLINT_int16":        writeInsertInt,
	"Redshift_TIME_string":           writeInsertStringNoEscape,
	"Redshift_TIMETZ_string":         writeInsertStringNoEscape,
	"Redshift_TIMESTAMP_time.Time":   mysqlWriteDateTime,
	"Redshift_TIMESTAMPTZ_time.Time": mysqlWriteDateTime,
}
