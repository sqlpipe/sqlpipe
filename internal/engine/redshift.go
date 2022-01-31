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
	db              *sql.DB
}

func (dsConn Redshift) execute(query string) (rows *sql.Rows, errProperties map[string]string, err error) {
	return standardExecute(query, dsConn.dsType, dsConn.db)
}

func (dsConn Redshift) closeDb() {
	dsConn.db.Close()
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

	redshift.SetMaxIdleConns(5)
	duration, _ := time.ParseDuration("10s")
	redshift.SetConnMaxIdleTime(duration)

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
		redshift,
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
		intermediateType = "Time"
	case "FLOAT8":
		intermediateType = "float64"
	case "INT4":
		intermediateType = "int32"
	case "NUMERIC":
		intermediateType = "Redshift_NUMERIC"
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
	// Generics
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
	// PostgreSQL
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

	case "MySQL_BIT":
		createType = "VARCHAR(MAX)"
	case "MySQL_TINYINT":
		createType = "SMALLINT"
	case "MySQL_SMALLINT":
		createType = "SMALLINT"
	case "MySQL_MEDIUMINT":
		createType = "INT"
	case "MySQL_INT":
		createType = "INT"
	case "MySQL_BIGINT":
		createType = "BIGINT"
	case "MySQL_FLOAT4":
		createType = "REAL"
	case "MySQL_FLOAT8":
		createType = "DOUBLE PRECISION"
	case "MySQL_DATE":
		createType = "DATE"
	case "MySQL_TIME":
		createType = "TIME"
	case "MySQL_DATETIME":
		createType = "TIMESTAMP"
	case "MySQL_TIMESTAMP":
		createType = "TIMESTAMP"
	case "MySQL_YEAR":
		createType = "INT"
	case "MySQL_CHAR":
		createType = "NVARCHAR(MAX)"
	case "MySQL_VARCHAR":
		createType = "NVARCHAR(MAX)"
	case "MySQL_TEXT":
		createType = "NVARCHAR(MAX)"
	case "MySQL_BINARY":
		createType = "VARCHAR(MAX)"
	case "MySQL_VARBINARY":
		createType = "VARCHAR(MAX)"
	case "MySQL_BLOB":
		createType = "VARCHAR(MAX)"
	case "MySQL_GEOMETRY":
		createType = "VARCHAR(MAX)"
	case "MySQL_JSON":
		createType = "NVARCHAR(MAX)"
	case "MySQL_DECIMAL":
		createType = fmt.Sprintf(
			"NUMERIC(%d,%d)",
			resultSetColInfo.ColumnPrecisions[colNum],
			resultSetColInfo.ColumnScales[colNum],
		)
	case "Redshift_BIGINT":
		createType = "BIGINT"
	case "Redshift_BOOLEAN":
		createType = "BOOLEAN"
	case "Redshift_CHAR":
		createType = "NVARCHAR(MAX)"
	case "Redshift_BPCHAR":
		createType = "NVARCHAR(MAX)"
	case "Redshift_DATE":
		createType = "DATE"
	case "Redshift_DOUBLE":
		createType = "DOUBLE PRECISION"
	case "Redshift_INT":
		createType = "INT"
	case "Redshift_REAL":
		createType = "REAL"
	case "Redshift_SMALLINT":
		createType = "SMALLINT"
	case "Redshift_TIME":
		createType = "TIME"
	case "Redshift_TIMETZ":
		createType = "TIMETZ"
	case "Redshift_TIMESTAMP":
		createType = "TIMESTAMP"
	case "Redshift_TIMESTAMPTZ":
		createType = "TIMESTAMPTZ"
	case "Redshift_NUMERIC":
		createType = "DOUBLE PRECISION"
	case "Redshift_VARCHAR":
		createType = fmt.Sprintf(
			"NVARCHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)

	case "MSSQL_BIGINT":
		createType = "BIGINT"
	case "MSSQL_BIT":
		createType = "BOOL"
	case "MSSQL_INT":
		createType = "INT"
	case "MSSQL_MONEY":
		createType = "VARCHAR(MAX)"
	case "MSSQL_SMALLINT":
		createType = "SMALLINT"
	case "MSSQL_SMALLMONEY":
		createType = "VARCHAR(MAX)"
	case "MSSQL_TINYINT":
		createType = "SMALLINT"
	case "MSSQL_FLOAT":
		createType = "DOUBLE PRECISION"
	case "MSSQL_REAL":
		createType = "REAL"
	case "MSSQL_DATE":
		createType = "DATE"
	case "MSSQL_DATETIME2":
		createType = "TIMESTAMP"
	case "MSSQL_DATETIME":
		createType = "TIMESTAMP"
	case "MSSQL_DATETIMEOFFSET":
		createType = "TIMESTAMPTZ"
	case "MSSQL_SMALLDATETIME":
		createType = "TIMESTAMP"
	case "MSSQL_TIME":
		createType = "TIME"
	case "MSSQL_TEXT":
		createType = "VARCHAR(MAX)"
	case "MSSQL_NTEXT":
		createType = "NVARCHAR(MAX)"
	case "MSSQL_BINARY":
		createType = "VARCHAR(MAX)"
	case "MSSQL_UNIQUEIDENTIFIER":
		createType = "VARCHAR(MAX)"
	case "MSSQL_XML":
		createType = "NVARCHAR(MAX)"
	case "MSSQL_VARBINARY":
		createType = "VARCHAR(MAX)"
	case "MSSQL_DECIMAL":
		createType = fmt.Sprintf(
			"NUMERIC(%d,%d)",
			resultSetColInfo.ColumnPrecisions[colNum],
			resultSetColInfo.ColumnScales[colNum],
		)
	case "MSSQL_CHAR":
		createType = fmt.Sprintf(
			"CHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	case "MSSQL_VARCHAR":
		createType = fmt.Sprintf(
			"VARCHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	case "MSSQL_NCHAR":
		createType = fmt.Sprintf(
			"NCHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	case "MSSQL_NVARCHAR":
		createType = fmt.Sprintf(
			"NVARCHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)

	// Oracle

	case "Oracle_OCIClobLocator":
		createType = "NVARCHAR(MAX)"
	case "Oracle_OCIBlobLocator":
		createType = "VARCHAR(MAX)"
	case "Oracle_LONG":
		createType = "NVARCHAR(MAX)"
	case "Oracle_NUMBER":
		createType = "DOUBLE PRECISION"
	case "Oracle_DATE":
		createType = "DATE"
	case "Oracle_TimeStampDTY":
		createType = "TIMESTAMP"
	case "Oracle_CHAR":
		createType = "NVARCHAR(MAX)"
	case "Oracle_NCHAR":
		createType = "NVARCHAR(MAX)"

	// SNOWFLAKE

	case "Snowflake_NUMBER":
		createType = "DOUBLE PRECISION"
	case "Snowflake_BINARY":
		createType = "VARCHAR(MAX)"
	case "Snowflake_REAL":
		createType = "DOUBLE PRECISION"
	case "Snowflake_TEXT":
		createType = "NVARCHAR(MAX)"
	case "Snowflake_BOOLEAN":
		createType = "BOOLEAN"
	case "Snowflake_DATE":
		createType = "DATE"
	case "Snowflake_TIME":
		createType = "VARCHAR(MAX)"
	case "Snowflake_TIMESTAMP_LTZ":
		createType = "TIMESTAMPTZ"
	case "Snowflake_TIMESTAMP_NTZ":
		createType = "TIMESTAMP"
	case "Snowflake_TIMESTAMP_TZ":
		createType = "TIMESTAMPTZ"
	case "Snowflake_VARIANT":
		createType = "NVARCHAR(MAX)"
	case "Snowflake_OBJECT":
		createType = "NVARCHAR(MAX)"
	case "Snowflake_ARRAY":
		createType = "NVARCHAR(MAX)"
	default:
		createType = "NVARCHAR(MAX)"
	}

	return createType
}

func redshiftWriteBytesAsVarchar(value interface{}, terminator string) string {
	return fmt.Sprintf("'%x'%s", value, terminator)
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
