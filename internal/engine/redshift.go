package engine

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/data"
	_ "github.com/jackc/pgx/v4/stdlib"
)

var redshift *sql.DB

type Redshift struct {
	dsType          string
	driverName      string `json:"-"`
	connString      string `json:"-"`
	debugConnString string
}

func getNewRedshift(
	connection data.Connection,
) (
	dsConn DsConnection,
	errProperties map[string]string,
	err error,
) {

	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		connection.Username,
		connection.Password,
		connection.Hostname,
		connection.Port,
		connection.DbName,
	)

	redshift, err = sql.Open("pgx", connString)
	if err != nil {
		return dsConn, errProperties, err
	}

	redshift.SetConnMaxLifetime(time.Minute * 1)

	dsConn = Redshift{
		"redshift",
		"pgx",
		fmt.Sprintf(
			"postgres://%s:%s@%s:%d/%s",
			connection.Username,
			connection.Password,
			connection.Hostname,
			connection.Port,
			connection.DbName,
		),
		fmt.Sprintf(
			"postgres://<USERNAME_MASKED>:<PASSWORD_MASKED>@%s:%d/%s",
			connection.Hostname,
			connection.Port,
			connection.DbName,
		),
	}
	return dsConn, errProperties, err
}

func (dsConn Redshift) getRows(
	transfer data.Transfer,
) (
	rows *sql.Rows,
	resultSetColumnInfo ResultSetColumnInfo,
	errProperties map[string]string,
	err error,
) {
	return standardGetRows(dsConn, transfer)
}

func (dsConn Redshift) turboTransfer(
	rows *sql.Rows,
	transfer data.Transfer,
	resultSetColumnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	return errProperties, err
}

func (dsConn Redshift) turboWriteMidVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) (
	errProperties map[string]string,
	err error,
) {
	return errProperties, errors.New("we haven't implemented redshift turbo transfer yet")
}

func (dsConn Redshift) turboWriteEndVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) (
	errProperties map[string]string,
	err error,
) {
	return errProperties, errors.New("we haven't implemented redshift turbo transfer yet")
}

func (dsConn Redshift) getFormattedResults(
	query string,
) (
	queryResult QueryResult,
	errProperties map[string]string,
	err error,
) {
	return standardGetFormattedResults(dsConn, query)
}

func (dsConn Redshift) getIntermediateType(
	colTypeFromDriver string,
) (
	intermediateType string,
	errProperties map[string]string,
	err error,
) {
	switch colTypeFromDriver {
	case "INT8":
		intermediateType = "int64"
	case "BOOL":
		intermediateType = "bool"
	case "BPCHAR":
		intermediateType = "Redshift_CHAR"
	case "VARCHAR":
		intermediateType = "Redshift_VARCHAR"
	case "DATE":
		intermediateType = "time.Time"
	case "FLOAT8":
		intermediateType = "float64"
	case "INT4":
		intermediateType = "int32"
	case "NUMERIC":
		intermediateType = "float64"
	case "FLOAT4":
		intermediateType = "float32"
	case "INT2":
		intermediateType = "int16"
	case "TIME":
		intermediateType = "Redshift_TIME"
	case "1266":
		intermediateType = "Redshift_TIMETZ"
	case "TIMESTAMP":
		intermediateType = "Time"
	case "TIMESTAMPTZ":
		intermediateType = "Time"
	default:
		err = fmt.Errorf("no Redshift intermediate type for driver type '%v'", colTypeFromDriver)
	}

	return intermediateType, errProperties, err
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

func (dsConn Redshift) dropTable(
	transfer data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {
	return dropTableIfExistsWithSchema(dsConn, transfer)
}

func (dsConn Redshift) deleteFromTable(
	transfer data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {
	return deleteFromTableWithSchema(dsConn, transfer)
}

func (dsConn Redshift) createTable(
	transfer data.Transfer,
	columnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	return standardCreateTable(dsConn, transfer, columnInfo)
}

func (dsConn Redshift) getValToWriteMidRow(valType string, value interface{}) string {
	return redshiftValWriters[valType](value, ",")
}

func (dsConn Redshift) getValToWriteRaw(valType string, value interface{}) string {
	fmt.Println(valType)
	return redshiftValWriters[valType](value, "")
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

func (dsConn Redshift) getQueryStarter(targetTable string, columnInfo ResultSetColumnInfo) string {
	return standardGetQueryStarter(targetTable, columnInfo)
}

func (dsConn Redshift) getCreateTableType(resultSetColInfo ResultSetColumnInfo, colNum int) (createType string) {

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
		createType = "REAL"
	case "float64":
		createType = "DOUBLE PRECISION"
	case "Time":
		createType = "TIMESTAMP"
	}

	if createType != "" {
		return createType
	}
	switch intermediateType {
	case "PostgreSQL_BIGINT":
		createType = "BIGINT"
	case "PostgreSQL_BIT":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_VARBIT":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_BOOLEAN":
		createType = "BOOLEAN"
	case "PostgreSQL_BOX":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_BYTEA":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_BPCHAR":
		createType = "NVARCHAR(MAX)"
	case "PostgreSQL_CIDR":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_CIRCLE":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_DATE":
		createType = "DATE"
	case "PostgreSQL_FLOAT8":
		createType = "DOUBLE PRECISION"
	case "PostgreSQL_INET":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_INT4":
		createType = "INT"
	case "PostgreSQL_INTERVAL":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_JSON":
		createType = "NVARCHAR(MAX)"
	case "PostgreSQL_JSONB":
		createType = "NVARCHAR(MAX)"
	case "PostgreSQL_LINE":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_LSEG":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_MACADDR":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_MONEY":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_PATH":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_PG_LSN":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_POINT":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_POLYGON":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_FLOAT4":
		createType = "REAL"
	case "PostgreSQL_INT2":
		createType = "SMALLINT"
	case "PostgreSQL_TEXT":
		createType = "NVARCHAR(MAX)"
	case "PostgreSQL_TIME":
		createType = "NVARCHAR(MAX)"
	case "PostgreSQL_TIMETZ":
		createType = "TIMETZ"
	case "PostgreSQL_TIMESTAMP":
		createType = "TIMESTAMP"
	case "PostgreSQL_TIMESTAMPTZ":
		createType = "TIMESTAMPTZ"
	case "PostgreSQL_TSQUERY":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_TSVECTOR":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_TXID_SNAPSHOT":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_UUID":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_XML":
		createType = "VARCHAR(MAX)"
	case "PostgreSQL_VARCHAR":
		createType = fmt.Sprintf(
			"NVARCHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	case "PostgreSQL_DECIMAL":
		createType = fmt.Sprintf(
			"NUMERIC(%d,%d)",
			resultSetColInfo.ColumnPrecisions[colNum],
			resultSetColInfo.ColumnScales[colNum],
		)
	}

	return createType
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

var redshiftCreateTableTypes = map[string]func(columnInfo ResultSetColumnInfo, colNum int) string{

	// Redshift

	"Redshift_BIGINT_int64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"Redshift_BOOLEAN_bool":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Redshift_CHAR_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Redshift_BPCHAR_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Redshift_DATE_time.Time":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Redshift_DOUBLE_float64":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Redshift_INT_int32":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"Redshift_REAL_float32":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"Redshift_SMALLINT_int16":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"Redshift_TIME_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"Redshift_TIMETZ_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMETZ" },
	"Redshift_TIMESTAMP_time.Time":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Redshift_TIMESTAMPTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"Redshift_NUMERIC_float64":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Redshift_VARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// PostgreSQL

	"PostgreSQL_BIGINT_int64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"PostgreSQL_BIT_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_VARBIT_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_BOOLEAN_bool":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"PostgreSQL_BOX_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_BYTEA_[]uint8":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_BPCHAR_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_CIDR_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_CIRCLE_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_DATE_time.Time":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"PostgreSQL_FLOAT8_float64":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"PostgreSQL_INET_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_INT4_int32":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"PostgreSQL_INTERVAL_string":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_JSON_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_JSONB_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_LINE_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_LSEG_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_MACADDR_string":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_MONEY_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_PATH_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_PG_LSN_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_POINT_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_POLYGON_string":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_FLOAT4_float32":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"PostgreSQL_INT2_int16":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"PostgreSQL_TEXT_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_TIME_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_TIMETZ_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMETZ" },
	"PostgreSQL_TIMESTAMP_time.Time":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"PostgreSQL_TIMESTAMPTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"PostgreSQL_TSQUERY_string":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_TSVECTOR_string":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_TXID_SNAPSHOT_string":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_UUID_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_XML_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_VARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"PostgreSQL_DECIMAL_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMERIC(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MySQL

	"MySQL_BIT_sql.RawBytes":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_TINYINT_sql.RawBytes":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MySQL_SMALLINT_sql.RawBytes":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MySQL_MEDIUMINT_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_INT_sql.RawBytes":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_BIGINT_sql.NullInt64":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MySQL_FLOAT4_sql.NullFloat64": func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"MySQL_FLOAT8_sql.NullFloat64": func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"MySQL_DATE_sql.NullTime":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MySQL_TIME_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MySQL_DATETIME_sql.NullTime":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_TIMESTAMP_sql.NullTime": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_YEAR_sql.NullInt64":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_CHAR_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MySQL_VARCHAR_sql.RawBytes":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MySQL_TEXT_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MySQL_BINARY_sql.RawBytes":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_VARBINARY_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_BLOB_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_GEOMETRY_sql.RawBytes":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_JSON_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MySQL_DECIMAL_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMERIC(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MSSQL

	"MSSQL_BIGINT_int64":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MSSQL_BIT_bool":                 func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOL" },
	"MSSQL_INT_int64":                func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MSSQL_MONEY_[]uint8":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_SMALLINT_int64":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MSSQL_SMALLMONEY_[]uint8":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_TINYINT_int64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MSSQL_FLOAT_float64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"MSSQL_REAL_float64":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"MSSQL_DATE_time.Time":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MSSQL_DATETIME2_time.Time":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIME_time.Time":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIMEOFFSET_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"MSSQL_SMALLDATETIME_time.Time":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_TIME_time.Time":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MSSQL_TEXT_string":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_NTEXT_string":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MSSQL_BINARY_[]uint8":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_XML_string":               func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MSSQL_VARBINARY_[]uint8":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_DECIMAL_[]uint8": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMERIC(%d,%d)",
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

	"Oracle_OCIClobLocator_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Oracle_OCIBlobLocator_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"Oracle_LONG_interface{}":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Oracle_NUMBER_interface{}":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
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

	"Snowflake_NUMBER_float64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Snowflake_BINARY_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"Snowflake_REAL_float64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Snowflake_TEXT_string":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Snowflake_BOOLEAN_bool":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Snowflake_DATE_time.Time":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Snowflake_TIME_time.Time":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"Snowflake_TIMESTAMP_LTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"Snowflake_TIMESTAMP_NTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Snowflake_TIMESTAMP_TZ_time.Time":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"Snowflake_VARIANT_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Snowflake_OBJECT_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Snowflake_ARRAY_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
}

var redshiftValWriters = map[string]func(value interface{}, terminator string) string{

	// Generics
	"bool":    writeInsertBool,
	"float32": writeInsertFloat,
	"float64": writeInsertFloat,
	"int16":   writeInsertInt,
	"int32":   writeInsertInt,
	"int64":   writeInsertInt,
	"Time":    postgresqlWriteTimeStampFromTime,

	// Redshift

	"Redshift_BIGINT":      writeInsertInt,
	"Redshift_BOOLEAN":     writeInsertBool,
	"Redshift_CHAR":        writeInsertEscapedString,
	"Redshift_BPCHAR":      writeInsertEscapedString,
	"Redshift_VARCHAR":     writeInsertEscapedString,
	"Redshift_DATE":        postgresqlWriteDate,
	"Redshift_DOUBLE":      writeInsertFloat,
	"Redshift_INT":         writeInsertInt,
	"Redshift_NUMERIC":     writeInsertRawStringNoQuotes,
	"Redshift_REAL":        writeInsertFloat,
	"Redshift_SMALLINT":    writeInsertInt,
	"Redshift_TIME":        writeInsertStringNoEscape,
	"Redshift_TIMETZ":      writeInsertStringNoEscape,
	"Redshift_TIMESTAMP":   postgresqlWriteTimeStampFromString,
	"Redshift_TIMESTAMPTZ": postgresqlWriteTimeStampFromString,

	// PostgreSQL

	"PostgreSQL_BIGINT":        writeInsertInt,
	"PostgreSQL_BIT":           writeInsertStringNoEscape,
	"PostgreSQL_VARBIT":        writeInsertStringNoEscape,
	"PostgreSQL_BOOLEAN":       writeInsertBool,
	"PostgreSQL_BOX":           writeInsertStringNoEscape,
	"PostgreSQL_BYTEA":         redshiftWriteBytesAsVarchar,
	"PostgreSQL_CIDR":          writeInsertStringNoEscape,
	"PostgreSQL_CIRCLE":        writeInsertStringNoEscape,
	"PostgreSQL_FLOAT8":        writeInsertFloat,
	"PostgreSQL_INET":          writeInsertStringNoEscape,
	"PostgreSQL_INT4":          writeInsertInt,
	"PostgreSQL_INTERVAL":      writeInsertStringNoEscape,
	"PostgreSQL_LINE":          writeInsertStringNoEscape,
	"PostgreSQL_LSEG":          writeInsertStringNoEscape,
	"PostgreSQL_MACADDR":       writeInsertStringNoEscape,
	"PostgreSQL_MONEY":         writeInsertStringNoEscape,
	"PostgreSQL_DECIMAL":       writeInsertRawStringNoQuotes,
	"PostgreSQL_PATH":          writeInsertStringNoEscape,
	"PostgreSQL_PG_LSN":        writeInsertStringNoEscape,
	"PostgreSQL_POINT":         writeInsertStringNoEscape,
	"PostgreSQL_POLYGON":       writeInsertStringNoEscape,
	"PostgreSQL_FLOAT4":        writeInsertFloat,
	"PostgreSQL_INT2":          writeInsertInt,
	"PostgreSQL_TIME":          writeInsertStringNoEscape,
	"PostgreSQL_TIMETZ":        writeInsertStringNoEscape,
	"PostgreSQL_TXID_SNAPSHOT": writeInsertStringNoEscape,
	"PostgreSQL_UUID":          writeInsertStringNoEscape,
	"PostgreSQL_VARCHAR":       writeInsertEscapedString,
	"PostgreSQL_BPCHAR":        writeInsertEscapedString,
	"PostgreSQL_DATE":          postgresqlWriteDate,
	"PostgreSQL_JSON":          writeInsertEscapedString,
	"PostgreSQL_JSONB":         writeInsertEscapedString,
	"PostgreSQL_TEXT":          writeInsertEscapedString,
	"PostgreSQL_TIMESTAMP":     postgresqlWriteTimeStampFromString,
	"PostgreSQL_TIMESTAMPTZ":   postgresqlWriteTimeStampFromString,
	"PostgreSQL_TSQUERY":       writeInsertEscapedString,
	"PostgreSQL_TSVECTOR":      writeInsertEscapedString,
	"PostgreSQL_XML":           writeInsertEscapedString,

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
