package engine

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/data"

	_ "github.com/go-sql-driver/mysql"
)

var mysql *sql.DB

type MySQL struct {
	dsType          string
	driverName      string `json:"-"`
	connString      string `json:"-"`
	debugConnString string
}

func getNewMySQL(
	connection data.Connection,
) (
	dsConn DsConnection,
	errProperties map[string]string,
	err error,
) {

	connString := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s",
		connection.Username,
		connection.Password,
		connection.Hostname,
		connection.Port,
		connection.DbName,
	)

	mysql, err = sql.Open("mysql", connString)

	if err != nil {
		return dsConn, errProperties, err
	}

	mysql.SetConnMaxLifetime(time.Minute * 1)

	dsConn = MySQL{
		"mysql",
		"mysql",
		fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/%s",
			connection.Username,
			connection.Password,
			connection.Hostname,
			connection.Port,
			connection.DbName,
		),
		fmt.Sprintf(
			"<USERNAME_MASKED>:<PASSWORD_MASKED>@tcp(%s:%d)/%s",
			connection.Hostname,
			connection.Port,
			connection.DbName,
		),
	}

	return dsConn, errProperties, err
}

func (dsConn MySQL) getQueryEnder(targetTable string) string {
	return ""
}

func (dsConn MySQL) getIntermediateType(
	colTypeFromDriver string,
) (
	intermediateType string,
	errProperties map[string]string,
	err error,
) {
	switch colTypeFromDriver {
	case "BIT":
		intermediateType = "MySQL_BIT"
	case "TINYINT":
		intermediateType = "MySQL_TINYINT"
	case "SMALLINT":
		intermediateType = "MySQL_SMALLINT"
	case "MEDIUMINT":
		intermediateType = "MySQL_MEDIUMINT"
	case "INT":
		intermediateType = "MySQL_INT"
	case "BIGINT":
		intermediateType = "MySQL_BIGINT"
	case "DECIMAL":
		intermediateType = "MySQL_DECIMAL"
	case "FLOAT":
		intermediateType = "MySQL_FLOAT4"
	case "DOUBLE":
		intermediateType = "MySQL_FLOAT8"
	case "DATE":
		intermediateType = "MySQL_DATE"
	case "TIME":
		intermediateType = "MySQL_TIME"
	case "DATETIME":
		intermediateType = "MySQL_DATETIME"
	case "TIMESTAMP":
		intermediateType = "MySQL_TIMESTAMP"
	case "YEAR":
		intermediateType = "MySQL_YEAR"
	case "CHAR":
		intermediateType = "MySQL_CHAR"
	case "VARCHAR":
		intermediateType = "MySQL_VARCHAR"
	case "TEXT":
		intermediateType = "MySQL_TEXT"
	case "BINARY":
		intermediateType = "MySQL_BINARY"
	case "VARBINARY":
		intermediateType = "MySQL_VARBINARY"
	case "BLOB":
		intermediateType = "MySQL_BLOB"
	case "GEOMETRY":
		intermediateType = "MySQL_GEOMETRY"
	case "JSON":
		intermediateType = "MySQL_JSON"
	default:
		err = fmt.Errorf("no MySQL intermediate type for driver type '%v'", colTypeFromDriver)
	}

	return intermediateType, errProperties, err
}

func (dsConn MySQL) getFormattedResults(
	query string,
) (
	queryResult QueryResult,
	errProperties map[string]string,
	err error,
) {
	return standardGetFormattedResults(dsConn, query)
}

func (dsConn MySQL) getRows(
	transfer data.Transfer,
) (
	rows *sql.Rows,
	resultSetColumnInfo ResultSetColumnInfo,
	errProperties map[string]string,
	err error,
) {
	return standardGetRows(dsConn, transfer)
}

func (dsConn MySQL) getConnectionInfo() (string, string, string) {
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

func (dsConn MySQL) dropTable(
	transfer data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {
	return dropTableIfExistsNoSchema(dsConn, transfer)
}

func (dsConn MySQL) turboTransfer(
	rows *sql.Rows,
	transfer data.Transfer,
	resultSetColumnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	return errProperties, err
}

func (dsConn MySQL) turboWriteMidVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) (
	errProperties map[string]string,
	err error,
) {
	return errProperties, errors.New("mysql hasn't implemented turbo writing yet")
}

func (dsConn MySQL) turboWriteEndVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) (
	errProperties map[string]string,
	err error,
) {
	return errProperties, errors.New("mysql hasn't implemented turbo writing yet")
}

func (dsConn MySQL) deleteFromTable(
	transfer data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {
	return deleteFromTableNoSchema(dsConn, transfer)
}

func (dsConn MySQL) createTable(
	transfer data.Transfer,
	columnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	// MySQL doesn't really have schemas
	transfer.TargetSchema = ""
	return standardCreateTable(dsConn, transfer, columnInfo)
}

func (dsConn MySQL) getValToWriteMidRow(valType string, value interface{}) string {
	return mysqlInsertWriters[valType](value, ",")
}

func (dsConn MySQL) getValToWriteRaw(valType string, value interface{}) string {
	return mysqlInsertWriters[valType](value, "")
}

func (dsConn MySQL) getValToWriteRowEnd(valType string, value interface{}) string {
	return mysqlInsertWriters[valType](value, ")")
}

func (dsConn MySQL) getRowStarter() string {
	return standardGetRowStarter()
}

func (dsConn MySQL) getQueryStarter(targetTable string, columnInfo ResultSetColumnInfo) string {
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

func (dsConn MySQL) getCreateTableType(
	resultSetColInfo ResultSetColumnInfo,
	colNum int,
) (
	createType string,
) {
	scanType := resultSetColInfo.ColumnScanTypes[colNum]
	intermediateType := resultSetColInfo.ColumnIntermediateTypes[colNum]

	switch scanType.Name() {
	case "bool":
		createType = "BOOLEAN"
	case "int", "int8", "int16", "int32", "uint8", "uint16":
		createType = "INT"
	case "int64", "uint32", "uint64":
		createType = "BIGINT"
	case "float32":
		createType = "FLOAT"
	case "float64":
		createType = "DOUBLE"
	case "Time":
		createType = "DATETIME"
	}

	if createType != "" {
		return createType
	}

	switch intermediateType {
	case "PostgreSQL_BIT", "PostgreSQL_VARBIT":
		createType = "TEXT"
	case "PostgreSQL_MONEY", "PostgreSQL_BPCHAR":
		createType = "TEXT"
	case "PostgreSQL_BOX":
		createType = "TEXT"
	case "PostgreSQL_CIDR":
		createType = "TEXT"
	case "PostgreSQL_BYTEA":
		createType = "LONGBLOB"
	case "PostgreSQL_CIRCLE":
		createType = "TEXT"
	case "PostgreSQL_INET":
		createType = "TEXT"
	case "PostgreSQL_INTERVAL":
		createType = "TEXT"
	case "PostgreSQL_JSON":
		createType = "JSON"
	case "PostgreSQL_JSONB":
		createType = "JSON"
	case "PostgreSQL_LINE":
		createType = "TEXT"
	case "PostgreSQL_LSEG":
		createType = "TEXT"
	case "PostgreSQL_MACADDR":
		createType = "TEXT"
	case "PostgreSQL_PATH":
		createType = "TEXT"
	case "PostgreSQL_PG_LSN":
		createType = "TEXT"
	case "PostgreSQL_POINT":
		createType = "TEXT"
	case "PostgreSQL_POLYGON":
		createType = "TEXT"
	case "PostgreSQL_TEXT":
		createType = "TEXT"
	case "PostgreSQL_TIME":
		createType = "TIME"
	case "PostgreSQL_TIMETZ":
		createType = "TIME"
	case "PostgreSQL_TSQUERY":
		createType = "TEXT"
	case "PostgreSQL_TSVECTOR":
		createType = "TEXT"
	case "PostgreSQL_TXID_SNAPSHOT":
		createType = "TEXT"
	case "PostgreSQL_UUID":
		createType = "TEXT"
	case "PostgreSQL_XML":
		createType = "TEXT"
	case "PostgreSQL_DECIMAL":
		createType = fmt.Sprintf(
			"NUMERIC(%d,%d)",
			resultSetColInfo.ColumnPrecisions[colNum],
			resultSetColInfo.ColumnScales[colNum],
		)
	case "PostgreSQL_VARCHAR":
		createType = fmt.Sprintf(
			"VARCHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	}

	return createType
}

var mysqlCreateTableTypes = map[string]func(columnInfo ResultSetColumnInfo, colNum int) string{

	// MySQL

	"MySQL_BIT_sql.RawBytes":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIT(64)" },
	"MySQL_TINYINT_sql.RawBytes":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TINYINT" },
	"MySQL_SMALLINT_sql.RawBytes":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MySQL_MEDIUMINT_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string { return "MEDIUMINT" },
	"MySQL_INT_sql.RawBytes":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_FLOAT4_sql.NullFloat64": func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MySQL_FLOAT8_sql.NullFloat64": func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"MySQL_DATE_sql.NullTime":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MySQL_TIME_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MySQL_DATETIME_sql.NullTime":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"MySQL_TIMESTAMP_sql.NullTime": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_YEAR_sql.NullInt64":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "YEAR" },
	"MySQL_CHAR_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"MySQL_VARCHAR_sql.RawBytes":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"MySQL_TEXT_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"MySQL_BINARY_sql.RawBytes":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"MySQL_VARBINARY_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"MySQL_BLOB_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"MySQL_GEOMETRY_sql.RawBytes":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "GEOMETRY" },
	"MySQL_JSON_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "JSON" },
	"MySQL_BIGINT_sql.NullInt64":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MySQL_DECIMAL_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"DECIMAL(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MSSQL

	"MSSQL_BIGINT_int64":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MSSQL_BIT_bool":                 func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOL" },
	"MSSQL_INT_int64":                func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MSSQL_SMALLINT_int64":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MSSQL_TINYINT_int64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "TINYINT" },
	"MSSQL_FLOAT_float64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"MSSQL_REAL_float64":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MSSQL_DATE_time.Time":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MSSQL_DATETIME2_time.Time":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"MSSQL_DATETIME_time.Time":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"MSSQL_DATETIMEOFFSET_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"MSSQL_SMALLDATETIME_time.Time":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATETIME" },
	"MSSQL_TIME_time.Time":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MSSQL_TEXT_string":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_NTEXT_string":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"MSSQL_BINARY_[]uint8":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"MSSQL_VARBINARY_[]uint8":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_XML_string":               func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"MSSQL_MONEY_[]uint8":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_SMALLMONEY_[]uint8":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
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

	// Oracle

	"Oracle_OCIClobLocator_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Oracle_OCIBlobLocator_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"Oracle_LONG_interface{}":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Oracle_NUMBER_interface{}":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"Oracle_DATE_interface{}":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Oracle_TimeStampDTY_interface{}":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Oracle_CHAR_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR(%d)",
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

	"Snowflake_NUMBER_float64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"Snowflake_BINARY_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "LONGBLOB" },
	"Snowflake_REAL_float64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"Snowflake_TEXT_string":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Snowflake_BOOLEAN_bool":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Snowflake_DATE_time.Time":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Snowflake_TIME_time.Time":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"Snowflake_TIMESTAMP_LTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Snowflake_TIMESTAMP_NTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Snowflake_TIMESTAMP_TZ_time.Time":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Snowflake_VARIANT_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Snowflake_OBJECT_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Snowflake_ARRAY_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },

	// Redshift

	"Redshift_BIGINT_int64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"Redshift_BOOLEAN_bool":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Redshift_CHAR_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Redshift_DATE_time.Time":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Redshift_DOUBLE_float64":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"Redshift_INT_int32":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"Redshift_NUMERIC_float64":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE" },
	"Redshift_REAL_float32":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"Redshift_SMALLINT_int16":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"Redshift_TIME_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"Redshift_TIMETZ_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Redshift_TIMESTAMP_time.Time":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Redshift_TIMESTAMPTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Redshift_BPCHAR_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT CHARACTER SET utf8" },
	"Redshift_VARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
}

var mysqlInsertWriters = map[string]func(value interface{}, terminator string) string{

	// POSTGRESQL
	"bool":                     writeInsertBool,
	"float32":                  writeInsertFloat,
	"float64":                  writeInsertFloat,
	"int16":                    writeInsertInt,
	"int32":                    writeInsertInt,
	"int64":                    writeInsertInt,
	"Time":                     mysqlWriteDateTime,
	"PostgreSQL_BYTEA":         postgresqlWriteByteArray,
	"PostgreSQL_BIT":           writeInsertStringNoEscape,
	"PostgreSQL_VARBIT":        writeInsertStringNoEscape,
	"PostgreSQL_BOX":           writeInsertStringNoEscape,
	"PostgreSQL_CIDR":          writeInsertStringNoEscape,
	"PostgreSQL_CIRCLE":        writeInsertStringNoEscape,
	"PostgreSQL_FLOAT8":        writeInsertFloat,
	"PostgreSQL_INET":          writeInsertStringNoEscape,
	"PostgreSQL_INTERVAL":      writeInsertStringNoEscape,
	"PostgreSQL_LINE":          writeInsertStringNoEscape,
	"PostgreSQL_LSEG":          writeInsertStringNoEscape,
	"PostgreSQL_MACADDR":       writeInsertStringNoEscape,
	"PostgreSQL_MONEY":         writeInsertStringNoEscape,
	"PostgreSQL_DECIMAL":       writeInsertStringNoEscape,
	"PostgreSQL_PATH":          writeInsertStringNoEscape,
	"PostgreSQL_PG_LSN":        writeInsertStringNoEscape,
	"PostgreSQL_POINT":         writeInsertStringNoEscape,
	"PostgreSQL_POLYGON":       writeInsertStringNoEscape,
	"PostgreSQL_TIME":          writeInsertStringNoEscape,
	"PostgreSQL_TIMETZ":        writeInsertStringNoEscape,
	"PostgreSQL_TXID_SNAPSHOT": writeInsertStringNoEscape,
	"PostgreSQL_UUID":          writeInsertStringNoEscape,
	"PostgreSQL_VARCHAR":       writeInsertEscapedString,
	"PostgreSQL_BPCHAR":        writeInsertEscapedString,
	"PostgreSQL_JSON":          writeBackslashJSON,
	"PostgreSQL_JSONB":         writeBackslashJSON,
	"PostgreSQL_TEXT":          writeInsertEscapedString,
	"PostgreSQL_TSQUERY":       writeInsertEscapedString,
	"PostgreSQL_TSVECTOR":      writeInsertEscapedString,
	"PostgreSQL_XML":           writeInsertEscapedString,

	// MYSQL

	"MySQL_BIT":       mysqlWriteInsertBinary,
	"MySQL_TINYINT":   writeInsertRawStringNoQuotes,
	"MySQL_SMALLINT":  writeInsertRawStringNoQuotes,
	"MySQL_MEDIUMINT": writeInsertRawStringNoQuotes,
	"MySQL_INT":       writeInsertRawStringNoQuotes,
	"MySQL_BIGINT":    writeInsertRawStringNoQuotes,
	"MySQL_DECIMAL":   writeInsertRawStringNoQuotes,
	"MySQL_FLOAT4":    writeInsertRawStringNoQuotes,
	"MySQL_FLOAT8":    writeInsertRawStringNoQuotes,
	"MySQL_DATE":      writeInsertStringNoEscape,
	"MySQL_TIME":      writeInsertStringNoEscape,
	"MySQL_TIMESTAMP": writeInsertStringNoEscape,
	"MySQL_DATETIME":  writeInsertStringNoEscape,
	"MySQL_YEAR":      writeInsertRawStringNoQuotes,
	"MySQL_CHAR":      writeInsertEscapedString,
	"MySQL_VARCHAR":   writeInsertEscapedString,
	"MySQL_TEXT":      writeInsertEscapedString,
	"MySQL_BINARY":    mysqlWriteInsertBinary,
	"MySQL_VARBINARY": mysqlWriteInsertBinary,
	"MySQL_BLOB":      mysqlWriteInsertBinary,
	"MySQL_GEOMETRY":  writeInsertStringNoEscape,
	"MySQL_JSON":      writeBackslashJSON,

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
	"Snowflake_TIME_time.Time":          writeInsertStringNoEscape,
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
