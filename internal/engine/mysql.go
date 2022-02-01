package engine

import (
	"database/sql"
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
	db              *sql.DB
}

func (dsConn MySQL) execute(query string) (rows *sql.Rows, errProperties map[string]string, err error) {
	return standardExecute(query, dsConn.dsType, dsConn.db)
}

func (dsConn MySQL) closeDb() {
	dsConn.db.Close()
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
		mysql,
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
) {
}

func (dsConn MySQL) turboWriteEndVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) {
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
	// PostgreSQL
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

	// MySQL
	case "MySQL_BIT":
		createType = "BIT(64)"
	case "MySQL_TINYINT":
		createType = "TINYINT"
	case "MySQL_SMALLINT":
		createType = "SMALLINT"
	case "MySQL_MEDIUMINT":
		createType = "MEDIUMINT"
	case "MySQL_INT":
		createType = "INT"
	case "MySQL_FLOAT4":
		createType = "FLOAT"
	case "MySQL_FLOAT8":
		createType = "DOUBLE"
	case "MySQL_DATE":
		createType = "DATE"
	case "MySQL_TIME":
		createType = "TIME"
	case "MySQL_DATETIME":
		createType = "DATETIME"
	case "MySQL_TIMESTAMP":
		createType = "TIMESTAMP"
	case "MySQL_YEAR":
		createType = "YEAR"
	case "MySQL_CHAR":
		createType = "TEXT CHARACTER SET utf8"
	case "MySQL_VARCHAR":
		createType = "TEXT CHARACTER SET utf8"
	case "MySQL_TEXT":
		createType = "TEXT CHARACTER SET utf8"
	case "MySQL_BINARY":
		createType = "LONGBLOB"
	case "MySQL_VARBINARY":
		createType = "LONGBLOB"
	case "MySQL_BLOB":
		createType = "LONGBLOB"
	case "MySQL_GEOMETRY":
		createType = "GEOMETRY"
	case "MySQL_JSON":
		createType = "JSON"
	case "MySQL_BIGINT":
		createType = "BIGINT"
	case "MySQL_DECIMAL":
		createType = fmt.Sprintf(
			"DECIMAL(%d,%d)",
			resultSetColInfo.ColumnPrecisions[colNum],
			resultSetColInfo.ColumnScales[colNum],
		)

	case "MSSQL_BIGINT":
		createType = "BIGINT"
	case "MSSQL_BIT":
		createType = "BOOL"
	case "MSSQL_INT":
		createType = "INT"
	case "MSSQL_SMALLINT":
		createType = "SMALLINT"
	case "MSSQL_TINYINT":
		createType = "TINYINT"
	case "MSSQL_FLOAT":
		createType = "DOUBLE"
	case "MSSQL_REAL":
		createType = "FLOAT"
	case "MSSQL_DATE":
		createType = "DATE"
	case "MSSQL_DATETIME2":
		createType = "DATETIME"
	case "MSSQL_DATETIME":
		createType = "DATETIME"
	case "MSSQL_DATETIMEOFFSET":
		createType = "DATETIME"
	case "MSSQL_SMALLDATETIME":
		createType = "DATETIME"
	case "MSSQL_TIME":
		createType = "TIME"
	case "MSSQL_TEXT":
		createType = "TEXT"
	case "MSSQL_NTEXT":
		createType = "TEXT CHARACTER SET utf8"
	case "MSSQL_BINARY":
		createType = "LONGBLOB"
	case "MSSQL_VARBINARY":
		createType = "LONGBLOB"
	case "MSSQL_UNIQUEIDENTIFIER":
		createType = "TEXT"
	case "MSSQL_XML":
		createType = "TEXT CHARACTER SET utf8"
	case "MSSQL_MONEY":
		createType = "TEXT"
	case "MSSQL_SMALLMONEY":
		createType = "TEXT"
	case "MSSQL_DECIMAL":
		createType = fmt.Sprintf(
			"DECIMAL(%d,%d)",
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

	case "Oracle_OCIClobLocator":
		createType = "TEXT CHARACTER SET utf8"
	case "Oracle_OCIBlobLocator":
		createType = "LONGBLOB"
	case "Oracle_LONG":
		createType = "TEXT CHARACTER SET utf8"
	case "Oracle_NUMBER":
		createType = "DOUBLE"
	case "Oracle_DATE":
		createType = "DATE"
	case "Oracle_TimeStampDTY":
		createType = "TIMESTAMP"
	case "Oracle_CHAR":
		createType = "TEXT"

	case "Oracle_NCHAR":
		createType = "TEXT"

	case "Snowflake_NUMBER":
		createType = "DOUBLE"
	case "Snowflake_BINARY":
		createType = "LONGBLOB"
	case "Snowflake_REAL":
		createType = "DOUBLE"
	case "Snowflake_TEXT":
		createType = "TEXT CHARACTER SET utf8"
	case "Snowflake_BOOLEAN":
		createType = "BOOLEAN"
	case "Snowflake_DATE":
		createType = "DATE"
	case "Snowflake_TIME":
		createType = "TIME"
	case "Snowflake_TIMESTAMP_LTZ":
		createType = "TIMESTAMP"
	case "Snowflake_TIMESTAMP_NTZ":
		createType = "TIMESTAMP"
	case "Snowflake_TIMESTAMP_TZ":
		createType = "TIMESTAMP"
	case "Snowflake_VARIANT":
		createType = "TEXT CHARACTER SET utf8"
	case "Snowflake_OBJECT":
		createType = "TEXT CHARACTER SET utf8"
	case "Snowflake_ARRAY":
		createType = "TEXT CHARACTER SET utf8"

	case "Redshift_BIGINT":
		createType = "BIGINT"
	case "Redshift_BOOLEAN":
		createType = "BOOLEAN"
	case "Redshift_CHAR":
		createType = "TEXT CHARACTER SET utf8"
	case "Redshift_DATE":
		createType = "DATE"
	case "Redshift_DOUBLE":
		createType = "DOUBLE"
	case "Redshift_INT":
		createType = "INT"
	case "Redshift_NUMERIC":
		createType = "DOUBLE"
	case "Redshift_REAL":
		createType = "REAL"
	case "Redshift_SMALLINT":
		createType = "SMALLINT"
	case "Redshift_TIME":
		createType = "TIME"
	case "Redshift_TIMETZ":
		createType = "TEXT"
	case "Redshift_TIMESTAMP":
		createType = "TIMESTAMP"
	case "Redshift_TIMESTAMPTZ":
		createType = "TIMESTAMP"
	case "Redshift_BPCHAR":
		createType = "TEXT CHARACTER SET utf8"
	case "Redshift_VARCHAR":
		createType = fmt.Sprintf(
			"NVARCHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	default:
		createType = "TEXT"

	}

	return createType
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
	"MSSQL_DATE":             mysqlWriteDateTime,
	"MSSQL_DATETIME2":        mysqlWriteDateTime,
	"MSSQL_DATETIME":         mysqlWriteDateTime,
	"MSSQL_DATETIMEOFFSET":   mysqlWriteDateTime,
	"MSSQL_SMALLDATETIME":    mysqlWriteDateTime,
	"MSSQL_TIME":             mysqlWriteDateTime,
	"MSSQL_CHAR":             writeInsertEscapedString,
	"MSSQL_VARCHAR":          writeInsertEscapedString,
	"MSSQL_TEXT":             writeInsertEscapedString,
	"MSSQL_NCHAR":            writeInsertEscapedString,
	"MSSQL_NVARCHAR":         writeInsertEscapedString,
	"MSSQL_NTEXT":            writeInsertEscapedString,
	"MSSQL_BINARY":           mysqlWriteInsertBinary,
	"MSSQL_VARBINARY":        mysqlWriteInsertBinary,
	"MSSQL_UNIQUEIDENTIFIER": writeMSSQLUniqueIdentifier,
	"MSSQL_XML":              writeInsertEscapedString,

	// Oracle

	"Oracle_CHAR":           writeInsertEscapedString,
	"Oracle_NCHAR":          writeInsertEscapedString,
	"Oracle_OCIClobLocator": writeInsertEscapedString,
	"Oracle_OCIBlobLocator": mysqlWriteInsertBinary,
	"Oracle_LONG":           writeInsertEscapedString,
	"Oracle_NUMBER":         oracleWriteNumber,
	"Oracle_DATE":           mysqlWriteDateTime,
	"Oracle_TimeStampDTY":   mysqlWriteDateTime,

	// Snowflake

	"Snowflake_NUMBER":        writeInsertRawStringNoQuotes,
	"Snowflake_REAL":          writeInsertRawStringNoQuotes,
	"Snowflake_TEXT":          writeInsertEscapedString,
	"Snowflake_BOOLEAN":       writeInsertStringNoEscape,
	"Snowflake_DATE":          mysqlWriteDateTime,
	"Snowflake_TIME":          writeInsertStringNoEscape,
	"Snowflake_TIMESTAMP_LTZ": mysqlWriteDateTime,
	"Snowflake_TIMESTAMP_NTZ": mysqlWriteDateTime,
	"Snowflake_TIMESTAMP_TZ":  mysqlWriteDateTime,
	"Snowflake_VARIANT":       writeInsertEscapedStringRemoveNewines,
	"Snowflake_OBJECT":        writeInsertEscapedStringRemoveNewines,
	"Snowflake_ARRAY":         writeInsertEscapedStringRemoveNewines,
	"Snowflake_BINARY":        mysqlWriteInsertBinary,

	// Redshift

	"Redshift_BIGINT":      writeInsertInt,
	"Redshift_BOOLEAN":     writeInsertBool,
	"Redshift_CHAR":        writeInsertEscapedString,
	"Redshift_BPCHAR":      writeInsertEscapedString,
	"Redshift_VARCHAR":     writeInsertEscapedString,
	"Redshift_DATE":        mysqlWriteDateTime,
	"Redshift_DOUBLE":      writeInsertFloat,
	"Redshift_INT":         writeInsertInt,
	"Redshift_NUMERIC":     writeInsertRawStringNoQuotes,
	"Redshift_REAL":        writeInsertFloat,
	"Redshift_SMALLINT":    writeInsertInt,
	"Redshift_TIME":        writeInsertStringNoEscape,
	"Redshift_TIMETZ":      writeInsertStringNoEscape,
	"Redshift_TIMESTAMP":   mysqlWriteDateTime,
	"Redshift_TIMESTAMPTZ": mysqlWriteDateTime,
}
