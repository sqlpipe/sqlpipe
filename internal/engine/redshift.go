package engine

import (
	"database/sql"
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
) {
}

func (dsConn Redshift) turboWriteEndVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) {
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
	"INT8":        "Redshift_BIGINT",
	"BOOL":        "Redshift_BOOLEAN",
	"BPCHAR":      "Redshift_CHAR",
	"VARCHAR":     "Redshift_VARCHAR",
	"DATE":        "Redshift_DATE",
	"FLOAT8":      "Redshift_DOUBLE",
	"INT4":        "Redshift_INT",
	"NUMERIC":     "Redshift_NUMERIC",
	"FLOAT4":      "Redshift_REAL",
	"INT2":        "Redshift_SMALLINT",
	"TIME":        "Redshift_TIME",
	"1266":        "Redshift_TIMETZ",
	"TIMESTAMP":   "Redshift_TIMESTAMP",
	"TIMESTAMPTZ": "Redshift_TIMESTAMPTZ",
}

var redshiftCreateTableTypes = map[string]func(columnInfo ResultSetColumnInfo, colNum int) string{

	// Redshift

	"Redshift_BIGINT":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"Redshift_BOOLEAN":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Redshift_CHAR":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Redshift_BPCHAR":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Redshift_DATE":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Redshift_DOUBLE":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Redshift_INT":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"Redshift_REAL":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"Redshift_SMALLINT":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"Redshift_TIME":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"Redshift_TIMETZ":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMETZ" },
	"Redshift_TIMESTAMP":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Redshift_TIMESTAMPTZ": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"Redshift_NUMERIC":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Redshift_VARCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// PostgreSQL

	"PostgreSQL_BIGINT":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"PostgreSQL_BIT":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_VARBIT":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_BOOLEAN":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"PostgreSQL_BOX":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_BYTEA":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_BPCHAR":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_CIDR":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_CIRCLE":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_DATE":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"PostgreSQL_FLOAT8":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"PostgreSQL_INET":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_INT4":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"PostgreSQL_INTERVAL":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_JSON":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_JSONB":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_LINE":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_LSEG":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_MACADDR":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_MONEY":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_PATH":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_PG_LSN":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_POINT":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_POLYGON":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_FLOAT4":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"PostgreSQL_INT2":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"PostgreSQL_TEXT":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_TIME":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"PostgreSQL_TIMETZ":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMETZ" },
	"PostgreSQL_TIMESTAMP":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"PostgreSQL_TIMESTAMPTZ":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"PostgreSQL_TSQUERY":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_TSVECTOR":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_TXID_SNAPSHOT": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_UUID":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_XML":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"PostgreSQL_VARCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"PostgreSQL_DECIMAL": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMERIC(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MySQL

	"MySQL_BIT":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_TINYINT":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MySQL_SMALLINT":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MySQL_MEDIUMINT": func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_INT":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_BIGINT":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MySQL_FLOAT4":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"MySQL_FLOAT8":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"MySQL_DATE":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MySQL_TIME":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MySQL_DATETIME":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_TIMESTAMP": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_YEAR":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_CHAR":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MySQL_VARCHAR":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MySQL_TEXT":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MySQL_BINARY":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_VARBINARY": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_BLOB":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_GEOMETRY":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MySQL_JSON":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MySQL_DECIMAL": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMERIC(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MSSQL

	"MSSQL_BIGINT":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MSSQL_BIT":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOL" },
	"MSSQL_INT":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MSSQL_MONEY":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_SMALLINT":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MSSQL_SMALLMONEY":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_TINYINT":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MSSQL_FLOAT":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"MSSQL_REAL":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
	"MSSQL_DATE":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MSSQL_DATETIME2":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIME":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIMEOFFSET":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"MSSQL_SMALLDATETIME":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_TIME":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MSSQL_TEXT":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_NTEXT":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MSSQL_BINARY":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_UNIQUEIDENTIFIER": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_XML":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"MSSQL_VARBINARY":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"MSSQL_DECIMAL": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMERIC(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},
	"MSSQL_CHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"CHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_VARCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_NCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_NVARCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// Oracle

	"Oracle_OCIClobLocator": func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Oracle_OCIBlobLocator": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"Oracle_LONG":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Oracle_NUMBER":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Oracle_DATE":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Oracle_TimeStampDTY":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Oracle_CHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"Oracle_NCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// SNOWFLAKE

	"Snowflake_NUMBER":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Snowflake_BINARY":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"Snowflake_REAL":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
	"Snowflake_TEXT":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Snowflake_BOOLEAN":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Snowflake_DATE":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Snowflake_TIME":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR(MAX)" },
	"Snowflake_TIMESTAMP_LTZ": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"Snowflake_TIMESTAMP_NTZ": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Snowflake_TIMESTAMP_TZ":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
	"Snowflake_VARIANT":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Snowflake_OBJECT":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
	"Snowflake_ARRAY":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR(MAX)" },
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

	"MySQL_BIT":       postgresqlWriteMySQLBitRawBytes,
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
	"MySQL_BINARY":    writeInsertEscapedString,
	"MySQL_VARBINARY": writeInsertEscapedString,
	"MySQL_BLOB":      redshiftWriteBytesAsVarchar,
	"MySQL_GEOMETRY":  redshiftWriteBytesAsVarchar,
	"MySQL_JSON":      writeInsertEscapedString,

	// MSSQL

	"MSSQL_BIGINT":           writeInsertInt,
	"MSSQL_BIT":              writeInsertBool,
	"MSSQL_DECIMAL":          writeInsertRawStringNoQuotes,
	"MSSQL_INT":              writeInsertInt,
	"MSSQL_MONEY":            writeInsertStringNoEscape,
	"MSSQL_SMALLINT":         writeInsertInt,
	"MSSQL_SMALLMONEY":       writeInsertStringNoEscape,
	"MSSQL_TINYINT":          writeInsertInt,
	"MSSQL_FLOAT":            writeInsertFloat,
	"MSSQL_REAL":             writeInsertFloat,
	"MSSQL_DATE":             postgresqlWriteTimeStampFromTime,
	"MSSQL_DATETIME2":        postgresqlWriteTimeStampFromTime,
	"MSSQL_DATETIME":         postgresqlWriteTimeStampFromTime,
	"MSSQL_DATETIMEOFFSET":   postgresqlWriteTimeStampFromTime,
	"MSSQL_SMALLDATETIME":    postgresqlWriteTimeStampFromTime,
	"MSSQL_TIME":             postgresqlWriteTimeStampFromTime,
	"MSSQL_CHAR":             writeInsertEscapedString,
	"MSSQL_VARCHAR":          writeInsertEscapedString,
	"MSSQL_TEXT":             writeInsertEscapedString,
	"MSSQL_NCHAR":            writeInsertEscapedString,
	"MSSQL_NVARCHAR":         writeInsertEscapedString,
	"MSSQL_NTEXT":            writeInsertEscapedString,
	"MSSQL_BINARY":           redshiftWriteBytesAsVarchar,
	"MSSQL_VARBINARY":        redshiftWriteBytesAsVarchar,
	"MSSQL_UNIQUEIDENTIFIER": writeMSSQLUniqueIdentifier,
	"MSSQL_XML":              writeInsertEscapedString,

	// Oracle

	"Oracle_CHAR":           writeInsertEscapedString,
	"Oracle_NCHAR":          writeInsertEscapedString,
	"Oracle_OCIClobLocator": writeInsertEscapedString,
	"Oracle_OCIBlobLocator": writeInsertEscapedString,
	"Oracle_LONG":           writeInsertEscapedString,
	"Oracle_NUMBER":         oracleWriteNumber,
	"Oracle_DATE":           postgresqlWriteTimeStampFromTime,
	"Oracle_TimeStampDTY":   postgresqlWriteTimeStampFromTime,

	// Snowflake

	"Snowflake_NUMBER":        writeInsertRawStringNoQuotes,
	"Snowflake_REAL":          writeInsertRawStringNoQuotes,
	"Snowflake_TEXT":          writeInsertEscapedString,
	"Snowflake_BOOLEAN":       writeInsertStringNoEscape,
	"Snowflake_DATE":          postgresqlWriteTimeStampFromTime,
	"Snowflake_TIME":          writeInsertStringNoEscape,
	"Snowflake_TIMESTAMP_LTZ": postgresqlWriteTimeStampFromTime,
	"Snowflake_TIMESTAMP_NTZ": postgresqlWriteTimeStampFromTime,
	"Snowflake_TIMESTAMP_TZ":  postgresqlWriteTimeStampFromTime,
	"Snowflake_VARIANT":       writeInsertEscapedStringRemoveNewines,
	"Snowflake_OBJECT":        writeInsertEscapedStringRemoveNewines,
	"Snowflake_ARRAY":         writeInsertEscapedStringRemoveNewines,
	"Snowflake_BINARY":        redshiftWriteBytesAsVarchar,
}
