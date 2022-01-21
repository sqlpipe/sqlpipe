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

	_ "github.com/jackc/pgx/v4/stdlib"
)

var redshift *sql.DB
var redshiftDsInfo structs.DsInfo

type Redshift struct {
	dsType          string
	driverName      string `json:"-"`
	connString      string `json:"-"`
	debugConnString string
	canConnect      bool
}

func (dsConn Redshift) GetDb() *sql.DB {
	return redshift
}

func (dsConn Redshift) GetDsInfo() structs.DsInfo {
	return redshiftDsInfo
}

func getNewRedshift(dsInfo structs.DsInfo) DsConnection {

	redshiftDsInfo = dsInfo

	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		dsInfo.Username,
		dsInfo.Password,
		dsInfo.Host,
		dsInfo.Port,
		dsInfo.DbName,
	)

	var err error
	redshift, err = sql.Open("pgx", connString)
	if err != nil {
		panic(fmt.Sprintf("couldn't open a connection to Redshift at host %s", dsInfo.Host))
	}
	redshift.SetConnMaxLifetime(time.Minute * 1)

	return Redshift{
		"redshift",
		"pgx",
		fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s",
			dsInfo.Username,
			dsInfo.Password,
			dsInfo.Host,
			dsInfo.Port,
			dsInfo.DbName,
		),
		fmt.Sprintf(
			"postgres://<USERNAME_MASKED>:<PASSWORD_MASKED>@%s:%s/%s",
			dsInfo.Host,
			dsInfo.Port,
			dsInfo.DbName,
		),
		false,
	}
}

func (dsConn Redshift) getRows(transfer models.Transfer) (*sql.Rows, structs.ResultSetColumnInfo) {
	return standardGetRows(dsConn, transfer)
}

func (dsConn Redshift) turboTransfer(
	rows *sql.Rows,
	transfer models.Transfer,
	resultSetColumnInfo structs.ResultSetColumnInfo,
) (err error) {
	return err
}

func (dsConn Redshift) turboWriteMidVal(valType string, value interface{}, builder *strings.Builder) {
	panic("Redshift hasn't implemented turbo write yet")
}

func (dsConn Redshift) turboWriteEndVal(valType string, value interface{}, builder *strings.Builder) {
	panic("Redshift hasn't implemented turbo write yet")
}

func (dsConn Redshift) getFormattedResults(query string) structs.QueryResult {
	return standardGetFormattedResults(dsConn, queryInfo)
}

func (dsConn Redshift) getIntermediateType(colTypeFromDriver string) string {
	return redshiftIntermediateTypes[colTypeFromDriver]
}

func (dsConn Redshift) getConnectionInfo() (string, string, string) {
	return dsConn.dsType, dsConn.driverName, dsConn.connString
}

func (dsConn Redshift) GetDebugInfo() (string, string) {
	return dsConn.dsType, dsConn.debugConnString
}

func (db Redshift) insertChecker(currentLen int, currentRow int) bool {
	if currentLen > 10000000 {
		return true
	} else {
		return false
	}
}

func (dsConn Redshift) dropTable(transfer models.Transfer) {
	dropTableIfExistsWithSchema(dsConn, transfer)
}

func (dsConn Redshift) deleteFromTable(transfer models.Transfer) {
	deleteFromTableWithSchema(dsConn, transfer)
}

func (dsConn Redshift) createTable(transfer models.Transfer, columnInfo structs.ResultSetColumnInfo) string {
	return standardCreateTable(dsConn, transfer, columnInfo)
}

func (dsConn Redshift) getValToWriteMidRow(valType string, value interface{}) string {
	return redshiftValWriters[valType](value, ",")
}

func (dsConn Redshift) getValToWriteRowEnd(valType string, value interface{}) string {
	return redshiftValWriters[valType](value, ")")
}

func (dsConn Redshift) getRowStarter() string {
	return standardGetRowStarter()
}

func (dsConn Redshift) getQueryEnder(targetTable string) string {
	return ""
}

func (dsConn Redshift) getQueryStarter(targetTable string, columnInfo structs.ResultSetColumnInfo) string {
	return standardGetQueryStarter(targetTable, columnInfo)
}

func (dsConn Redshift) getCreateTableType(resultSetColInfo structs.ResultSetColumnInfo, colNum int) string {
	return redshiftCreateTableTypes[resultSetColInfo.ColumnIntermediateTypes[colNum]](resultSetColInfo, colNum)
}

func redshiftWriteBytesAsVarchar(value interface{}, terminator string) string {
	return fmt.Sprintf("'%x'%s", value, terminator)
}

var redshiftIntermediateTypes = map[string]string{
	"INT8":        "Redshift_BIGINT_int64",
	"BOOL":        "Redshift_BOOLEAN_bool",
	"BPCHAR":      "Redshift_CHAR_string",
	"VARCHAR":     "Redshift_VARCHAR_string",
	"DATE":        "Redshift_DATE_time.Time",
	"FLOAT8":      "Redshift_DOUBLE_float64",
	"INT4":        "Redshift_INT_int32",
	"NUMERIC":     "Redshift_NUMERIC_float64",
	"FLOAT4":      "Redshift_REAL_float32",
	"INT2":        "Redshift_SMALLINT_int16",
	"TIME":        "Redshift_TIME_string",
	"1266":        "Redshift_TIMETZ_string",
	"TIMESTAMP":   "Redshift_TIMESTAMP_time.Time",
	"TIMESTAMPTZ": "Redshift_TIMESTAMPTZ_time.Time",
}

var redshiftCreateTableTypes = map[string]func(columnInfo structs.ResultSetColumnInfo, colNum int) string{

	// Redshift

	"Redshift_BIGINT_int64":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"Redshift_BOOLEAN_bool":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Redshift_CHAR_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Redshift_BPCHAR_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Redshift_DATE_time.Time":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Redshift_DOUBLE_float64":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Redshift_INT_int32":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"Redshift_REAL_float32":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "REAL" },
	"Redshift_SMALLINT_int16":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"Redshift_TIME_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIME" },
	"Redshift_TIMETZ_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMETZ" },
	"Redshift_TIMESTAMP_time.Time":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Redshift_TIMESTAMPTZ_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"Redshift_NUMERIC_float64":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Redshift_VARCHAR_string": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// PostgreSQL

	"PostgreSQL_BIGINT_int64":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"PostgreSQL_BIT_string":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_VARBIT_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_BOOLEAN_bool":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"PostgreSQL_BOX_string":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_BYTEA_[]uint8":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_BPCHAR_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_CIDR_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_CIRCLE_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_DATE_time.Time":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"PostgreSQL_FLOAT8_float64":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"PostgreSQL_INET_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_INT4_int32":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"PostgreSQL_INTERVAL_string":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_JSON_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_JSONB_string":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_LINE_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_LSEG_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_MACADDR_string":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_MONEY_string":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_PATH_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_PG_LSN_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_POINT_string":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_POLYGON_string":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_FLOAT4_float32":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "REAL" },
	"PostgreSQL_INT2_int16":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"PostgreSQL_TEXT_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_TIME_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_TIMETZ_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMETZ" },
	"PostgreSQL_TIMESTAMP_time.Time":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"PostgreSQL_TIMESTAMPTZ_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"PostgreSQL_TSQUERY_string":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_TSVECTOR_string":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_TXID_SNAPSHOT_string":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_UUID_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_XML_string":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
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

	// MySQL

	"MySQL_BIT_sql.RawBytes":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_TINYINT_sql.RawBytes":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MySQL_SMALLINT_sql.RawBytes":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MySQL_MEDIUMINT_sql.RawBytes": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_INT_sql.RawBytes":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_BIGINT_sql.NullInt64":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MySQL_FLOAT4_sql.NullFloat64": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "REAL" },
	"MySQL_FLOAT8_sql.NullFloat64": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"MySQL_DATE_sql.NullTime":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MySQL_TIME_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MySQL_DATETIME_sql.NullTime":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_TIMESTAMP_sql.NullTime": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_YEAR_sql.NullInt64":     func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_CHAR_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MySQL_VARCHAR_sql.RawBytes":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MySQL_TEXT_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MySQL_BINARY_sql.RawBytes":    func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_VARBINARY_sql.RawBytes": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_BLOB_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_GEOMETRY_sql.RawBytes":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_JSON_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MySQL_DECIMAL_sql.RawBytes": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
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
	"MSSQL_MONEY_[]uint8":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_SMALLINT_int64":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MSSQL_SMALLMONEY_[]uint8":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_TINYINT_int64":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MSSQL_FLOAT_float64":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"MSSQL_REAL_float64":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "REAL" },
	"MSSQL_DATE_time.Time":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MSSQL_DATETIME2_time.Time":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIME_time.Time":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIMEOFFSET_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"MSSQL_SMALLDATETIME_time.Time":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_TIME_time.Time":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MSSQL_TEXT_string":              func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_NTEXT_string":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MSSQL_BINARY_[]uint8":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_XML_string":               func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MSSQL_VARBINARY_[]uint8":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_DECIMAL_[]uint8": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMERIC(%d,%d)",
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

	"Oracle_OCIClobLocator_interface{}": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Oracle_OCIBlobLocator_interface{}": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"Oracle_LONG_interface{}":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Oracle_NUMBER_interface{}":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
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

	"Snowflake_NUMBER_float64":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Snowflake_BINARY_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"Snowflake_REAL_float64":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Snowflake_TEXT_string":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Snowflake_BOOLEAN_bool":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Snowflake_DATE_time.Time":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Snowflake_TIME_time.Time":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"Snowflake_TIMESTAMP_LTZ_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"Snowflake_TIMESTAMP_NTZ_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Snowflake_TIMESTAMP_TZ_time.Time":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"Snowflake_VARIANT_string":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Snowflake_OBJECT_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Snowflake_ARRAY_string":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
}

var redshiftValWriters = map[string]func(value interface{}, terminator string) string{

	// Redshift

	"Redshift_BIGINT_int64":          writeInsertInt,
	"Redshift_BOOLEAN_bool":          writeInsertBool,
	"Redshift_CHAR_string":           writeInsertEscapedString,
	"Redshift_BPCHAR_string":         writeInsertEscapedString,
	"Redshift_VARCHAR_string":        writeInsertEscapedString,
	"Redshift_DATE_time.Time":        postgresqlWriteDate,
	"Redshift_DOUBLE_float64":        writeInsertFloat,
	"Redshift_INT_int32":             writeInsertInt,
	"Redshift_NUMERIC_float64":       writeInsertRawStringNoQuotes,
	"Redshift_REAL_float32":          writeInsertFloat,
	"Redshift_SMALLINT_int16":        writeInsertInt,
	"Redshift_TIME_string":           writeInsertStringNoEscape,
	"Redshift_TIMETZ_string":         writeInsertStringNoEscape,
	"Redshift_TIMESTAMP_time.Time":   postgresqlWriteTimeStampFromString,
	"Redshift_TIMESTAMPTZ_time.Time": postgresqlWriteTimeStampFromString,

	// PostgreSQL

	"PostgreSQL_BIGINT_int64":          writeInsertInt,
	"PostgreSQL_BIT_string":            writeInsertStringNoEscape,
	"PostgreSQL_VARBIT_string":         writeInsertStringNoEscape,
	"PostgreSQL_BOOLEAN_bool":          writeInsertBool,
	"PostgreSQL_BOX_string":            writeInsertStringNoEscape,
	"PostgreSQL_BYTEA_[]uint8":         redshiftWriteBytesAsVarchar,
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
	"PostgreSQL_DATE_time.Time":        postgresqlWriteDate,
	"PostgreSQL_JSON_string":           writeInsertEscapedString,
	"PostgreSQL_JSONB_string":          writeInsertEscapedString,
	"PostgreSQL_TEXT_string":           writeInsertEscapedString,
	"PostgreSQL_TIMESTAMP_time.Time":   postgresqlWriteTimeStampFromString,
	"PostgreSQL_TIMESTAMPTZ_time.Time": postgresqlWriteTimeStampFromString,
	"PostgreSQL_TSQUERY_string":        writeInsertEscapedString,
	"PostgreSQL_TSVECTOR_string":       writeInsertEscapedString,
	"PostgreSQL_XML_string":            writeInsertEscapedString,

	// MYSQL

	"MySQL_BIT_sql.RawBytes":       postgresqlWriteMySQLBitRawBytes,
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
	"MySQL_BINARY_sql.RawBytes":    writeInsertEscapedString,
	"MySQL_VARBINARY_sql.RawBytes": writeInsertEscapedString,
	"MySQL_BLOB_sql.RawBytes":      redshiftWriteBytesAsVarchar,
	"MySQL_GEOMETRY_sql.RawBytes":  redshiftWriteBytesAsVarchar,
	"MySQL_JSON_sql.RawBytes":      writeInsertEscapedString,

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
	"MSSQL_DATE_time.Time":           postgresqlWriteTimeStampFromTime,
	"MSSQL_DATETIME2_time.Time":      postgresqlWriteTimeStampFromTime,
	"MSSQL_DATETIME_time.Time":       postgresqlWriteTimeStampFromTime,
	"MSSQL_DATETIMEOFFSET_time.Time": postgresqlWriteTimeStampFromTime,
	"MSSQL_SMALLDATETIME_time.Time":  postgresqlWriteTimeStampFromTime,
	"MSSQL_TIME_time.Time":           postgresqlWriteTimeStampFromTime,
	"MSSQL_CHAR_string":              writeInsertEscapedString,
	"MSSQL_VARCHAR_string":           writeInsertEscapedString,
	"MSSQL_TEXT_string":              writeInsertEscapedString,
	"MSSQL_NCHAR_string":             writeInsertEscapedString,
	"MSSQL_NVARCHAR_string":          writeInsertEscapedString,
	"MSSQL_NTEXT_string":             writeInsertEscapedString,
	"MSSQL_BINARY_[]uint8":           redshiftWriteBytesAsVarchar,
	"MSSQL_VARBINARY_[]uint8":        redshiftWriteBytesAsVarchar,
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": writeMSSQLUniqueIdentifier,
	"MSSQL_XML_string":               writeInsertEscapedString,

	// Oracle

	"Oracle_CHAR_interface{}":           writeInsertEscapedString,
	"Oracle_NCHAR_interface{}":          writeInsertEscapedString,
	"Oracle_OCIClobLocator_interface{}": writeInsertEscapedString,
	"Oracle_OCIBlobLocator_interface{}": writeInsertEscapedString,
	"Oracle_LONG_interface{}":           writeInsertEscapedString,
	"Oracle_NUMBER_interface{}":         oracleWriteNumber,
	"Oracle_DATE_interface{}":           postgresqlWriteTimeStampFromTime,
	"Oracle_TimeStampDTY_interface{}":   postgresqlWriteTimeStampFromTime,

	// Snowflake

	"Snowflake_NUMBER_float64":          writeInsertRawStringNoQuotes,
	"Snowflake_REAL_float64":            writeInsertRawStringNoQuotes,
	"Snowflake_TEXT_string":             writeInsertEscapedString,
	"Snowflake_BOOLEAN_bool":            writeInsertStringNoEscape,
	"Snowflake_DATE_time.Time":          postgresqlWriteTimeStampFromTime,
	"Snowflake_TIME_time.Time":          writeInsertStringNoEscape,
	"Snowflake_TIMESTAMP_LTZ_time.Time": postgresqlWriteTimeStampFromTime,
	"Snowflake_TIMESTAMP_NTZ_time.Time": postgresqlWriteTimeStampFromTime,
	"Snowflake_TIMESTAMP_TZ_time.Time":  postgresqlWriteTimeStampFromTime,
	"Snowflake_VARIANT_string":          writeInsertEscapedStringRemoveNewines,
	"Snowflake_OBJECT_string":           writeInsertEscapedStringRemoveNewines,
	"Snowflake_ARRAY_string":            writeInsertEscapedStringRemoveNewines,
	"Snowflake_BINARY_string":           redshiftWriteBytesAsVarchar,
}
