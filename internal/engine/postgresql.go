package engine

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/data"
	_ "github.com/jackc/pgx/v4/stdlib"
)

var postgresql *sql.DB

type PostgreSQL struct {
	dsType          string
	driverName      string `json:"-"`
	connString      string `json:"-"`
	debugConnString string
}

func getNewPostgreSQL(
	connection data.Connection,
) (
	dsConn DsConnection,
	errProperties map[string]string,
	err error,
) {

	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%v/%s",
		connection.Username,
		connection.Password,
		connection.Hostname,
		connection.Port,
		connection.DbName,
	)

	postgresql, err = sql.Open("pgx", connString)

	if err != nil {
		return dsConn, errProperties, err
	}

	postgresql.SetConnMaxLifetime(time.Minute * 1)

	dsConn = PostgreSQL{
		"postgresql",
		"pgx",
		fmt.Sprintf(
			"postgres://%s:%s@%s:%v/%s",
			connection.Username,
			connection.Password,
			connection.Hostname,
			connection.Port,
			connection.DbName,
		),
		fmt.Sprintf(
			"postgres://<USERNAME_MASKED>:<PASSWORD_MASKED>@%s:%v/%s",
			connection.Hostname,
			connection.Port,
			connection.DbName,
		),
	}

	return dsConn, errProperties, err
}

func (dsConn PostgreSQL) turboTransfer(
	rows *sql.Rows,
	transfer data.Transfer,
	resultSetColumnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	return errProperties, err
}

func (dsConn PostgreSQL) getRows(
	transfer data.Transfer,
) (
	rows *sql.Rows,
	resultSetColumnInfo ResultSetColumnInfo,
	errProperties map[string]string,
	err error,
) {
	return standardGetRows(dsConn, transfer)
}

func (dsConn PostgreSQL) getFormattedResults(
	query string,
) (
	queryResult QueryResult,
	errProperties map[string]string,
	err error,
) {
	return standardGetFormattedResults(dsConn, query)
}

func (dsConn PostgreSQL) getConnectionInfo() (string, string, string) {
	return dsConn.dsType, dsConn.driverName, dsConn.connString
}

func (dsConn PostgreSQL) GetDebugInfo() (string, string) {
	return dsConn.dsType, dsConn.debugConnString
}

func (db PostgreSQL) insertChecker(currentLen int, currentRow int) bool {
	if currentLen > 10000000 {
		return true
	} else {
		return false
	}
}

func (dsConn PostgreSQL) dropTable(
	transfer data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {
	return dropTableIfExistsWithSchema(dsConn, transfer)
}

func (dsConn PostgreSQL) deleteFromTable(
	transfer data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {
	return deleteFromTableWithSchema(dsConn, transfer)
}

func (dsConn PostgreSQL) createTable(
	transfer data.Transfer,
	columnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	return standardCreateTable(dsConn, transfer, columnInfo)
}

func (dsConn PostgreSQL) getValToWriteMidRow(valType string, value interface{}) string {
	return postgresValWriters[valType](value, ",")
}

func (dsConn PostgreSQL) getValToWriteRaw(valType string, value interface{}) string {
	return postgresValWriters[valType](value, "")
}

func (dsConn PostgreSQL) getValToWriteRowEnd(valType string, value interface{}) string {
	return postgresValWriters[valType](value, ")")
}

func (dsConn PostgreSQL) turboWriteMidVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) {
}

func (dsConn PostgreSQL) turboWriteEndVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) {
}

func (dsConn PostgreSQL) getRowStarter() string {
	return standardGetRowStarter()
}

func (dsConn PostgreSQL) getQueryEnder(targetTable string) string {
	return ""
}

func (dsConn PostgreSQL) getQueryStarter(targetTable string, columnInfo ResultSetColumnInfo) string {
	return standardGetQueryStarter(targetTable, columnInfo)
}

func postgresqlWriteByteArray(value interface{}, terminator string) string {
	return fmt.Sprintf("'\\x%x'%s", value, terminator)
}

func postgresqlWriteDate(value interface{}, terminator string) string {
	return fmt.Sprintf("'%s'%s", fmt.Sprintf("%s", value)[:10], terminator)
}

func postgresqlWriteTimeStampFromString(value interface{}, terminator string) string {
	return fmt.Sprintf("'%s'%s", strings.ReplaceAll(fmt.Sprintf("%s", value), " +", "."), terminator)
}

func postgresqlWriteTimeStampFromTime(value interface{}, terminator string) string {
	var returnVal string

	switch v := value.(type) {
	case time.Time:
		returnVal = fmt.Sprintf("'%s'%s", v.Format("2006-01-02 15:04:05.000000 -0700"), terminator)
	default:
		return fmt.Sprintf("null%s", terminator)
	}

	return returnVal
}

// func postgresqlWriteTimeFromTime(value interface{}, terminator string) string {
// 	timeVal := value.(time.Time)
// 	return fmt.Sprintf("'%s'%s", timeVal.Format("15:04:05.000000 -0700"), terminator)
// }

func postgresqlWriteMySQLBitRawBytes(value interface{}, terminator string) string {
	bracketsRemoved := strings.Trim(fmt.Sprintf("%b", value), "[]")
	noSpaces := strings.Replace(bracketsRemoved, " ", "", -1)
	return fmt.Sprintf("'%s'%s", noSpaces, terminator)
}

func (dsConn PostgreSQL) getIntermediateType(
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
	case "DATE":
		intermediateType = "Time"
	case "FLOAT8":
		intermediateType = "float64"
	case "INT4":
		intermediateType = "int32"
	case "FLOAT4":
		intermediateType = "float32"
	case "INT2":
		intermediateType = "int16"
	case "TIMESTAMP":
		intermediateType = "Time"
	case "TIMESTAMPTZ":
		intermediateType = "Time"
	case "BYTEA":
		intermediateType = "PostgreSQL_BYTEA"
	case "UUID":
		intermediateType = "PostgreSQL_UUID"
	case "BIT":
		intermediateType = "PostgreSQL_BIT"
	case "VARBIT":
		intermediateType = "PostgreSQL_VARBIT"
	case "BOX":
		intermediateType = "PostgreSQL_BOX"
	case "BPCHAR":
		intermediateType = "PostgreSQL_BPCHAR"
	case "VARCHAR":
		intermediateType = "PostgreSQL_VARCHAR"
	case "CIDR":
		intermediateType = "PostgreSQL_CIDR"
	case "CIRCLE":
		intermediateType = "PostgreSQL_CIRCLE"
	case "INET":
		intermediateType = "PostgreSQL_INET"
	case "INTERVAL":
		intermediateType = "PostgreSQL_INTERVAL"
	case "JSON":
		intermediateType = "PostgreSQL_JSON"
	case "JSONB":
		intermediateType = "PostgreSQL_JSONB"
	case "LINE":
		intermediateType = "PostgreSQL_LINE"
	case "LSEG":
		intermediateType = "PostgreSQL_LSEG"
	case "MACADDR":
		intermediateType = "PostgreSQL_MACADDR"
	case "790":
		intermediateType = "PostgreSQL_MONEY"
	case "NUMERIC":
		intermediateType = "PostgreSQL_DECIMAL"
	case "PATH":
		intermediateType = "PostgreSQL_PATH"
	case "3220":
		intermediateType = "PostgreSQL_PG_LSN"
	case "POINT":
		intermediateType = "PostgreSQL_POINT"
	case "TEXTcase ":
		intermediateType = "PostgreSQL_TEXT"
	case "POLYGON":
		intermediateType = "PostgreSQL_POLYGON"
	case "TIME":
		intermediateType = "PostgreSQL_TIME"
	case "1266":
		intermediateType = "PostgreSQL_TIMETZ"
	case "3615":
		intermediateType = "PostgreSQL_TSQUERY"
	case "3614":
		intermediateType = "PostgreSQL_TSVECTOR"
	case "2970":
		intermediateType = "PostgreSQL_TXID_SNAPSHOT"
	case "142":
		intermediateType = "PostgreSQL_XML"
	case "TEXT":
		intermediateType = "PostgreSQL_TEXT"
	default:
		err = fmt.Errorf("no PostgreSQL intermediate type for driver type '%v'", colTypeFromDriver)
	}

	return intermediateType, errProperties, err
}

func (dsConn PostgreSQL) getCreateTableType(resultSetColInfo ResultSetColumnInfo, colNum int) (createType string) {

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
		createType = "TIMESTAMPTZ"
	}

	if createType != "" {
		return createType
	}

	switch intermediateType {
	// PostgreSQL
	case "PostgreSQL_BIT", "PostgreSQL_VARBIT":
		createType = "VARBIT"
	case "PostgreSQL_MONEY", "PostgreSQL_BPCHAR":
		createType = "VARCHAR"
	case "PostgreSQL_BOX":
		createType = "BOX"
	case "PostgreSQL_BYTEA":
		createType = "BYTEA"
	case "PostgreSQL_CIDR":
		createType = "CIDR"
	case "PostgreSQL_CIRCLE":
		createType = "CIRCLE"
	case "PostgreSQL_INET":
		createType = "INET"
	case "PostgreSQL_INTERVAL":
		createType = "INTERVAL"
	case "PostgreSQL_JSON":
		createType = "JSON"
	case "PostgreSQL_JSONB":
		createType = "JSONB"
	case "PostgreSQL_LINE":
		createType = "LINE"
	case "PostgreSQL_LSEG":
		createType = "LSEG"
	case "PostgreSQL_MACADDR":
		createType = "MACADDR"
	case "PostgreSQL_PATH":
		createType = "PATH"
	case "PostgreSQL_PG_LSN":
		createType = "PG_LSN"
	case "PostgreSQL_POINT":
		createType = "POINT"
	case "PostgreSQL_POLYGON":
		createType = "POLYGON"
	case "PostgreSQL_TEXT":
		createType = "TEXT"
	case "PostgreSQL_TIME":
		createType = "TIME"
	case "PostgreSQL_TIMETZ":
		createType = "TIMETZ"
	case "PostgreSQL_TSQUERY":
		createType = "TSQUERY"
	case "PostgreSQL_TSVECTOR":
		createType = "TSVECTOR"
	case "PostgreSQL_TXID_SNAPSHOT":
		createType = "TXID_SNAPSHOT"
	case "PostgreSQL_UUID":
		createType = "UUID"
	case "PostgreSQL_XML":
		createType = "XML"
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
		createType = "VARBIT"
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
		createType = "VARCHAR"
	case "MySQL_VARCHAR":
		createType = "VARCHAR"
	case "MySQL_TEXT":
		createType = "TEXT"
	case "MySQL_BINARY":
		createType = "BYTEA"
	case "MySQL_VARBINARY":
		createType = "BYTEA"
	case "MySQL_BLOB":
		createType = "BYTEA"
	case "MySQL_GEOMETRY":
		createType = "BYTEA"
	case "MySQL_JSON":
		createType = "JSON"
	case "MySQL_DECIMAL":
		createType = fmt.Sprintf("NUMERIC(%d,%d)", resultSetColInfo.ColumnPrecisions[colNum], resultSetColInfo.ColumnScales[colNum])
	}

	return createType
}

// var postgresqlCreateTableTypes = map[string]func(columnInfo ResultSetColumnInfo, colNum int) string{

// 	// MySQL

// "MySQL_BIT":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARBIT" },
// "MySQL_TINYINT":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
// "MySQL_SMALLINT":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
// "MySQL_MEDIUMINT": func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
// "MySQL_INT":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
// "MySQL_BIGINT":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
// "MySQL_FLOAT4": func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
// "MySQL_FLOAT8": func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
// "MySQL_DATE":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
// "MySQL_TIME":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
// "MySQL_DATETIME":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
// "MySQL_TIMESTAMP": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
// "MySQL_YEAR":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
// "MySQL_CHAR":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR" },
// "MySQL_VARCHAR":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR" },
// "MySQL_TEXT":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
// "MySQL_BINARY":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "BYTEA" },
// "MySQL_VARBINARY": func(columnInfo ResultSetColumnInfo, colNum int) string { return "BYTEA" },
// "MySQL_BLOB":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "BYTEA" },
// "MySQL_GEOMETRY":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "BYTEA" },
// "MySQL_JSON":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "JSON" },
// "MySQL_DECIMAL": func(columnInfo ResultSetColumnInfo, colNum int) string {
// 	return fmt.Sprintf(
// 		"NUMERIC(%d,%d)",
// 		columnInfo.ColumnPrecisions[colNum],
// 		columnInfo.ColumnScales[colNum],
// 	)
// },

// 	// MSSQL

// 	"MSSQL_BIGINT":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
// 	"MSSQL_BIT":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOL" },
// 	"MSSQL_INT":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
// 	"MSSQL_MONEY":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
// 	"MSSQL_SMALLINT":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
// 	"MSSQL_SMALLMONEY":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
// 	"MSSQL_TINYINT":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
// 	"MSSQL_FLOAT":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
// 	"MSSQL_REAL":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
// 	"MSSQL_DATE":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
// 	"MSSQL_DATETIME2":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
// 	"MSSQL_DATETIME":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
// 	"MSSQL_DATETIMEOFFSET":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
// 	"MSSQL_SMALLDATETIME":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
// 	"MSSQL_TIME":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
// 	"MSSQL_TEXT":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
// 	"MSSQL_NTEXT":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
// 	"MSSQL_BINARY":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "BYTEA" },
// 	"MSSQL_VARBINARY":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BYTEA" },
// 	"MSSQL_UNIQUEIDENTIFIER": func(columnInfo ResultSetColumnInfo, colNum int) string { return "UUID" },
// 	"MSSQL_XML":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "XML" },
// 	"MSSQL_DECIMAL": func(columnInfo ResultSetColumnInfo, colNum int) string {
// 		return fmt.Sprintf(
// 			"NUMERIC(%d,%d)",
// 			columnInfo.ColumnPrecisions[colNum],
// 			columnInfo.ColumnScales[colNum],
// 		)
// 	},
// 	"MSSQL_CHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
// 		return fmt.Sprintf(
// 			"CHAR(%d)",
// 			columnInfo.ColumnLengths[colNum],
// 		)
// 	},
// 	"MSSQL_VARCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
// 		return fmt.Sprintf(
// 			"VARCHAR(%d)",
// 			columnInfo.ColumnLengths[colNum],
// 		)
// 	},
// 	"MSSQL_NCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
// 		return fmt.Sprintf(
// 			"CHAR(%d)",
// 			columnInfo.ColumnLengths[colNum],
// 		)
// 	},
// 	"MSSQL_NVARCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
// 		return fmt.Sprintf(
// 			"VARCHAR(%d)",
// 			columnInfo.ColumnLengths[colNum],
// 		)
// 	},

// 	// Oracle

// 	"Oracle_OCIClobLocator": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR" },
// 	"Oracle_OCIBlobLocator": func(columnInfo ResultSetColumnInfo, colNum int) string { return "BYTEA" },
// 	"Oracle_LONG":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
// 	"Oracle_NUMBER":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMERIC" },
// 	"Oracle_DATE":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
// 	"Oracle_TimeStampDTY":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
// 	"Oracle_CHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
// 		return fmt.Sprintf(
// 			"VARCHAR(%d)",
// 			columnInfo.ColumnLengths[colNum],
// 		)
// 	},
// 	"Oracle_NCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
// 		return fmt.Sprintf(
// 			"VARCHAR(%d)",
// 			columnInfo.ColumnLengths[colNum],
// 		)
// 	},

// 	// SNOWFLAKE

// 	"Snowflake_NUMBER":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
// 	"Snowflake_BINARY":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BYTEA" },
// 	"Snowflake_REAL":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
// 	"Snowflake_TEXT":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR" },
// 	"Snowflake_BOOLEAN":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
// 	"Snowflake_DATE":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
// 	"Snowflake_TIME":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
// 	"Snowflake_TIMESTAMP_LTZ": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
// 	"Snowflake_TIMESTAMP_NTZ": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
// 	"Snowflake_TIMESTAMP_TZ":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
// 	"Snowflake_VARIANT":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR" },
// 	"Snowflake_OBJECT":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR" },
// 	"Snowflake_ARRAY":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR" },

// 	// Redshift

// 	"Redshift_BIGINT":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
// 	"Redshift_BOOLEAN":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
// 	"Redshift_CHAR":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR" },
// 	"Redshift_BPCHAR":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR" },
// 	"Redshift_DATE":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
// 	"Redshift_DOUBLE":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
// 	"Redshift_INT":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
// 	"Redshift_NUMERIC":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "DOUBLE PRECISION" },
// 	"Redshift_REAL":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "REAL" },
// 	"Redshift_SMALLINT":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
// 	"Redshift_TIME":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
// 	"Redshift_TIMETZ":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMETZ" },
// 	"Redshift_TIMESTAMP":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
// 	"Redshift_TIMESTAMPTZ": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMPTZ" },
// 	"Redshift_VARCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
// 		return fmt.Sprintf(
// 			"VARCHAR(%d)",
// 			columnInfo.ColumnLengths[colNum],
// 		)
// 	},
// }

var postgresValWriters = map[string]func(value interface{}, terminator string) string{

	// Generics
	"bool":    writeInsertBool,
	"float32": writeInsertFloat,
	"float64": writeInsertFloat,
	"int16":   writeInsertInt,
	"int32":   writeInsertInt,
	"int64":   writeInsertInt,
	"Time":    postgresqlWriteTimeStampFromTime,

	// PostgreSQL
	"PostgreSQL_BYTEA":         postgresqlWriteByteArray,
	"PostgreSQL_BIT":           writeInsertStringNoEscape,
	"PostgreSQL_VARBIT":        writeInsertStringNoEscape,
	"PostgreSQL_BOX":           writeInsertStringNoEscape,
	"PostgreSQL_CIDR":          writeInsertStringNoEscape,
	"PostgreSQL_CIRCLE":        writeInsertStringNoEscape,
	"PostgreSQL_INET":          writeInsertStringNoEscape,
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
	"PostgreSQL_TIME":          writeInsertStringNoEscape,
	"PostgreSQL_TIMETZ":        writeInsertStringNoEscape,
	"PostgreSQL_TXID_SNAPSHOT": writeInsertStringNoEscape,
	"PostgreSQL_UUID":          writeInsertStringNoEscape,
	"PostgreSQL_VARCHAR":       writeInsertEscapedString,
	"PostgreSQL_BPCHAR":        writeInsertEscapedString,
	"PostgreSQL_JSON":          writeInsertEscapedString,
	"PostgreSQL_JSONB":         writeInsertEscapedString,
	"PostgreSQL_TEXT":          writeInsertEscapedString,
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
	"MySQL_BINARY":    postgresqlWriteByteArray,
	"MySQL_VARBINARY": postgresqlWriteByteArray,
	"MySQL_BLOB":      postgresqlWriteByteArray,
	"MySQL_GEOMETRY":  postgresqlWriteByteArray,
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
	"MSSQL_BINARY":           postgresqlWriteByteArray,
	"MSSQL_VARBINARY":        postgresqlWriteByteArray,
	"MSSQL_UNIQUEIDENTIFIER": writeMSSQLUniqueIdentifier,
	"MSSQL_XML":              writeInsertEscapedString,

	// Oracle

	"Oracle_CHAR":           writeInsertEscapedString,
	"Oracle_NCHAR":          writeInsertEscapedString,
	"Oracle_OCIClobLocator": writeInsertEscapedString,
	"Oracle_OCIBlobLocator": postgresqlWriteByteArray,
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
	"Snowflake_TIME":          postgresqlWriteTimeStampFromTime,
	"Snowflake_TIMESTAMP_LTZ": postgresqlWriteTimeStampFromTime,
	"Snowflake_TIMESTAMP_NTZ": postgresqlWriteTimeStampFromTime,
	"Snowflake_TIMESTAMP_TZ":  postgresqlWriteTimeStampFromTime,
	"Snowflake_VARIANT":       writeInsertEscapedStringRemoveNewines,
	"Snowflake_OBJECT":        writeInsertEscapedStringRemoveNewines,
	"Snowflake_ARRAY":         writeInsertEscapedStringRemoveNewines,
	"Snowflake_BINARY":        postgresqlWriteByteArray,

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
}
