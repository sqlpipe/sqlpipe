package engine

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/data"
	_ "github.com/sijms/go-ora/v2"
)

var oracle *sql.DB

type Oracle struct {
	dsType          string
	driverName      string `json:"-"`
	connString      string `json:"-"`
	debugConnString string
}

func getNewOracle(
	connection data.Connection,
) (
	dsConn DsConnection,
	errProperties map[string]string,
	err error,
) {

	connString := fmt.Sprintf(
		"oracle://%s:%s@%s:%d/%s",
		connection.Username,
		connection.Password,
		connection.Hostname,
		connection.Port,
		connection.DbName,
	)

	oracle, err = sql.Open("oracle", connString)

	if err != nil {
		return dsConn, errProperties, err
	}

	oracle.SetConnMaxLifetime(time.Minute * 1)

	dsConn = Oracle{
		"oracle",
		"oracle",
		fmt.Sprintf(
			"oracle://%s:%s@%s:%d/%s",
			connection.Username,
			connection.Password,
			connection.Hostname,
			connection.Port,
			connection.DbName,
		),
		fmt.Sprintf(
			"oracle://<USERNAME_MASKED>:<PASSWORD_MASKED>@%s:%d/%s",
			connection.Hostname,
			connection.Port,
			connection.DbName,
		),
	}

	return dsConn, errProperties, err
}

func (dsConn Oracle) getIntermediateType(
	colTypeFromDriver string,
) (
	intermediateType string,
	errProperties map[string]string,
	err error,
) {
	switch colTypeFromDriver {
	case "CHAR":
		intermediateType = "Oracle_CHAR"
	case "NCHAR":
		intermediateType = "Oracle_NCHAR"
	case "OCIClobLocator":
		intermediateType = "Oracle_OCIClobLocator"
	case "OCIBlobLocator":
		intermediateType = "Oracle_OCIBlobLocator"
	case "LONG":
		intermediateType = "Oracle_LONG"
	case "NUMBER":
		intermediateType = "Oracle_NUMBER"
	case "IBFloat":
		intermediateType = "Oracle_IBFloat"
	case "IBDouble":
		intermediateType = "Oracle_IBDouble"
	case "DATE":
		intermediateType = "Oracle_DATE"
	case "TimeStampDTY":
		intermediateType = "Oracle_TimeStampDTY"
	case "TimeStampTZ_DTY":
		intermediateType = "Oracle_TimeStampTZ_DTY"
	case "TimeStampLTZ_DTY":
		intermediateType = "Oracle_TimeStampLTZ_DTY"
	case "NOT":
		intermediateType = "Oracle_NOT"
	case "OracleType(109)":
		intermediateType = "Oracle_OracleType(109)"
	default:
		err = fmt.Errorf("no Oracle intermediate type for driver type '%v'", colTypeFromDriver)
	}

	return intermediateType, errProperties, err
}

func (dsConn Oracle) turboTransfer(
	rows *sql.Rows,
	transfer data.Transfer,
	resultSetColumnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	return errProperties, err
}

func (dsConn Oracle) turboWriteMidVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) {
}

func (dsConn Oracle) turboWriteEndVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) {
}

func (dsConn Oracle) getRows(
	transfer data.Transfer,
) (
	rows *sql.Rows,
	resultSetColumnInfo ResultSetColumnInfo,
	errProperties map[string]string,
	err error,
) {
	rows, errProperties, err = execute(dsConn, transfer.Query)
	if err != nil {
		return rows, resultSetColumnInfo, errProperties, err
	}
	resultSetColumnInfo, errProperties, err = getResultSetColumnInfo(dsConn, rows)
	if err != nil {
		return rows, resultSetColumnInfo, errProperties, err
	}

	var formattedResults = QueryResult{}
	formattedResults.ColumnTypes = map[string]string{}
	formattedResults.Rows = []interface{}{}

	for i, colType := range resultSetColumnInfo.ColumnDbTypes {
		formattedResults.ColumnTypes[resultSetColumnInfo.ColumnNames[i]] = colType
	}

	columnInfo := formattedResults.ColumnTypes

	// if you need to rewrite the query to avoid certain columntypes
	for _, rowType := range columnInfo {
		switch rowType {
		case "IBFloat", "IBDouble", "TimeStampTZ_DTY", "TimeStampLTZ_DTY", "OracleType(109)", "NOT":
			transfer.Query = oracleRewriteQuery(transfer.Query, resultSetColumnInfo)
			return dsConn.getRows(transfer)
		}
	}

	return rows, resultSetColumnInfo, errProperties, err
}

func (dsConn Oracle) getFormattedResults(
	query string,
) (
	queryResult QueryResult,
	errProperties map[string]string,
	err error,
) {
	// defer func() {
	// 	if raisedValue := recover(); raisedValue != nil {
	// 		switch val := raisedValue.(type) {
	// 		case runtime.Error:
	// 			if strings.Contains(fmt.Sprint(val), "runtime error: integer divide by zero") {
	// 				return
	// 			}
	// 		case ErrorInfo:
	// 			if strings.Contains(val.ErrorMessage, "TTC error: received code 0 during stmt reading") {
	// 				panic(`Got error: "TTC error: received code 0 during stmt reading". This usually means you are trying to extract an XML or URIType column from Oracle, which is not currently supported.`)
	// 			}
	// 		}
	// 		panic(raisedValue)
	// 	}
	// }()

	rows, errProperties, err := execute(dsConn, query)
	if err != nil {
		return queryResult, errProperties, err
	}

	resultSetColumnInfo, errProperties, err := getResultSetColumnInfo(dsConn, rows)
	if err != nil {
		return queryResult, errProperties, err
	}

	queryResult = QueryResult{}
	queryResult.ColumnTypes = map[string]string{}
	queryResult.Rows = []interface{}{}

	for i, colType := range resultSetColumnInfo.ColumnDbTypes {
		queryResult.ColumnTypes[resultSetColumnInfo.ColumnNames[i]] = colType
	}

	columnInfo := queryResult.ColumnTypes

	// if you need to rewrite the query to avoid certain columntypes
	for _, rowType := range columnInfo {
		switch rowType {
		case "IBFloat", "IBDouble", "TimeStampTZ_DTY", "TimeStampLTZ_DTY", "OracleType(109)", "NOT":
			query = oracleRewriteQuery(query, resultSetColumnInfo)
			return dsConn.getFormattedResults(query)
		}
	}

	numCols := resultSetColumnInfo.NumCols
	colTypes := resultSetColumnInfo.ColumnIntermediateTypes

	values := make([]interface{}, numCols)
	valuePtrs := make([]interface{}, numCols)

	// set the pointer in valueptrs to corresponding values
	for i := 0; i < numCols; i++ {
		valuePtrs[i] = &values[i]
	}

	for i := 0; rows.Next(); i++ {
		// scan incoming values into valueptrs, which in turn points to values

		rows.Scan(valuePtrs...)
		for j := 0; j < numCols; j++ {
			queryResult.Rows = append(queryResult.Rows, dsConn.getValToWriteRaw(colTypes[j], values[j]))
		}
	}

	return queryResult, errProperties, err
}

func oracleRewriteQuery(
	query string,
	resultSetColumnInfo ResultSetColumnInfo,
) string {
	var queryBuilder strings.Builder
	columnsRemoved := strings.SplitN(strings.ToLower(query), "from", 2)[1]

	queryBuilder.WriteString("SELECT ")

	sep := ""

	colNames := resultSetColumnInfo.ColumnNames
	colTypes := resultSetColumnInfo.ColumnDbTypes

	for i, colType := range colTypes {
		colName := colNames[i]
		switch colType {
		case "TimeStampTZ_DTY", "TimeStampLTZ_DTY":
			fmt.Fprintf(
				&queryBuilder, "%sCAST(%s as TIMESTAMP) as %s",
				sep,
				colName,
				colName,
			)
			sep = ", "
		case "IBFloat", "IBDouble":
			fmt.Fprintf(
				&queryBuilder, "%sCAST(%s as NUMBER) as %s",
				sep,
				colName,
				colName,
			)
			sep = ", "
		case "OracleType(109)", "NOT":
			fmt.Fprintf(
				&queryBuilder, "%sCAST(%s as VARCHAR) as %s",
				sep,
				colName,
				colName,
			)
			sep = ", "
		default:
			fmt.Fprintf(&queryBuilder, "%s%s", sep, colName)
			sep = ", "
		}
	}

	fmt.Fprintf(&queryBuilder, " FROM%s", columnsRemoved)

	return queryBuilder.String()
}

func (dsConn Oracle) getConnectionInfo() (dsType string, driveName string, connString string) {
	return dsConn.dsType, dsConn.driverName, dsConn.connString
}

func (dsConn Oracle) GetDebugInfo() (string, string) {
	return dsConn.dsType, dsConn.debugConnString
}

func (dsConn Oracle) insertChecker(currentLen int, currentRow int) bool {
	if currentLen > 10000 {
		return true
	} else {
		return false
	}
}

func (dsConn Oracle) dropTable(
	transfer data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {
	// defer func() {
	// 	if raisedValue := recover(); raisedValue != nil {
	// 		switch value := raisedValue.(type) {
	// 		case ErrorInfo:
	// 			// its OK if the table doesn't exist
	// 			if strings.Contains(value.ErrorMessage, "ORA-00942") {
	// 				return
	// 			}
	// 		default:
	// 			panic(raisedValue)
	// 		}
	// 	}
	// }()
	errProperties, err = dropTableNoSchema(dsConn, transfer)
	if err != nil {
		if strings.HasPrefix(errProperties["error"], "ORA-00942") {
			errProperties = map[string]string{}
			err = nil
		}
	}
	return errProperties, err
}

func (dsConn Oracle) deleteFromTable(
	transfer data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {
	return deleteFromTableNoSchema(dsConn, transfer)
}

func (dsConn Oracle) createTable(
	transfer data.Transfer,
	columnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	// Oracle doesn't really have schemas
	transfer.TargetSchema = ""
	return standardCreateTable(dsConn, transfer, columnInfo)
}

func (dsConn Oracle) getValToWriteMidRow(valType string, value interface{}) string {
	return oracleValWriters[valType](value, ",")
}

func (dsConn Oracle) getValToWriteRowEnd(valType string, value interface{}) string {
	return oracleValWriters[valType](value, " FROM dual UNION ALL ")
}

func (dsConn Oracle) getValToWriteRaw(valType string, value interface{}) string {
	return oracleValWriters[valType](value, "")
}

func (dsConn Oracle) getRowStarter() string {
	return "SELECT "
}

func (dsConn Oracle) getQueryEnder(targetTable string) string {
	return fmt.Sprintf(") SELECT * FROM %s_to_insert", targetTable)
}

func (dsConn Oracle) getQueryStarter(targetTable string, columnInfo ResultSetColumnInfo) string {
	queryStarter := fmt.Sprintf("insert into %s (%s) with %s_to_insert (%s) as ( SELECT ", targetTable, strings.Join(columnInfo.ColumnNames, ", "), targetTable, strings.Join(columnInfo.ColumnNames, ", "))
	return queryStarter
}

func oracleWriteDateFromTime(value interface{}, terminator string) string {
	var returnVal string

	switch v := value.(type) {
	case time.Time:
		returnVal = fmt.Sprintf("TO_DATE('%s', 'YYYY-MM-DD')%s", v.Format("2006-01-02"), terminator)
	default:
		return fmt.Sprintf("null%s", terminator)
	}

	return returnVal
}

func oracleWriteDateFromString(value interface{}, terminator string) string {
	return fmt.Sprintf("TO_DATE('%s', 'YYYY-MM-DD')%s", value, terminator)
}

func oracleWriteDatetimeFromString(value interface{}, terminator string) string {
	return fmt.Sprintf("TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')%s", value, terminator)
}

func oracleWriteDatetimeFromTime(value interface{}, terminator string) string {
	var returnVal string

	switch v := value.(type) {
	case time.Time:
		returnVal = fmt.Sprintf("TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')%s", v.Format("2006-01-02 15:04:05.000000"), terminator)
	default:
		return fmt.Sprintf("null%s", terminator)
	}

	return returnVal
}

func oracleWriteBlob(value interface{}, terminator string) string {
	return fmt.Sprintf("hextoraw('%x')%s", value, terminator)
}

func oracleWriteBool(value interface{}, terminator string) string {

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

func (dsConn Oracle) getCreateTableType(
	resultSetColInfo ResultSetColumnInfo,
	colNum int,
) (
	createType string,
) {
	scanType := resultSetColInfo.ColumnScanTypes[colNum]
	intermediateType := resultSetColInfo.ColumnIntermediateTypes[colNum]

	switch scanType.Name() {
	// Generics
	case "bool":
		createType = "NUMBER(1)"
	case "int", "int8", "int16", "int32", "uint8", "uint16":
		createType = "INTEGER"
	case "int64", "uint32", "uint64":
		createType = "NUMBER(19, 0)"
	case "float32":
		createType = "BINARY_FLOAT"
	case "float64":
		createType = "BINARY_DOUBLE"
	case "Time":
		createType = "TIMESTAMP"
	}

	if createType != "" {
		return createType
	}

	switch intermediateType {
	// PostgreSQL
	case "PostgreSQL_BIGINT":
		createType = "NUMBER(19, 0)"
	case "PostgreSQL_BIT":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_VARBIT":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_BOOLEAN":
		createType = "NUMBER(1)"
	case "PostgreSQL_BOX":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_BYTEA":
		createType = "BLOB"
	case "PostgreSQL_BPCHAR":
		createType = "NVARCHAR2(2000)"
	case "PostgreSQL_CIDR":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_CIRCLE":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_DATE":
		createType = "DATE"
	case "PostgreSQL_FLOAT8":
		createType = "BINARY_DOUBLE"
	case "PostgreSQL_INET":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_INT4":
		createType = "INTEGER"
	case "PostgreSQL_INTERVAL":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_JSON":
		createType = "NVARCHAR2(2000)"
	case "PostgreSQL_JSONB":
		createType = "NVARCHAR2(2000)"
	case "PostgreSQL_LINE":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_LSEG":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_MACADDR":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_MONEY":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_PATH":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_PG_LSN":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_POINT":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_POLYGON":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_FLOAT4":
		createType = "BINARY_FLOAT"
	case "PostgreSQL_INT2":
		createType = "INTEGER"
	case "PostgreSQL_TEXT":
		createType = "NVARCHAR2(2000)"
	case "PostgreSQL_TIME":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_TIMETZ":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_TIMESTAMP":
		createType = "TIMESTAMP"
	case "PostgreSQL_TIMESTAMPTZ":
		createType = "TIMESTAMP WITH TIME ZONE"
	case "PostgreSQL_TSQUERY":
		createType = "NVARCHAR2(2000)"
	case "PostgreSQL_TSVECTOR":
		createType = "NVARCHAR2(2000)"
	case "PostgreSQL_TXID_SNAPSHOT":
		createType = "NVARCHAR2(2000)"
	case "PostgreSQL_UUID":
		createType = "VARCHAR2(4000)"
	case "PostgreSQL_XML":
		createType = "NVARCHAR2(2000)"
	case "PostgreSQL_VARCHAR":
		createType = fmt.Sprintf(
			"NVARCHAR2(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	case "PostgreSQL_DECIMAL":
		createType = fmt.Sprintf(
			"NUMBER(%d,%d)",
			resultSetColInfo.ColumnPrecisions[colNum],
			resultSetColInfo.ColumnScales[colNum],
		)

	// MySQL
	case "MySQL_BIT":
		createType = "BLOB"
	case "MySQL_TINYINT":
		createType = "INTEGER"
	case "MySQL_SMALLINT":
		createType = "INTEGER"
	case "MySQL_MEDIUMINT":
		createType = "INTEGER"
	case "MySQL_INT":
		createType = "INTEGER"
	case "MySQL_FLOAT4":
		createType = "BINARY_FLOAT"
	case "MySQL_FLOAT8":
		createType = "BINARY_DOUBLE"
	case "MySQL_DATE":
		createType = "DATE"
	case "MySQL_TIME":
		createType = "VARCHAR2(4000)"
	case "MySQL_DATETIME":
		createType = "TIMESTAMP"
	case "MySQL_TIMESTAMP":
		createType = "TIMESTAMP"
	case "MySQL_YEAR":
		createType = "INTEGER"
	case "MySQL_CHAR":
		createType = "NVARCHAR2(2000)"
	case "MySQL_VARCHAR":
		createType = "NVARCHAR2(2000)"
	case "MySQL_TEXT":
		createType = "NVARCHAR2(2000)"
	case "MySQL_BINARY":
		createType = "BLOB"
	case "MySQL_VARBINARY":
		createType = "BLOB"
	case "MySQL_BLOB":
		createType = "BLOB"
	case "MySQL_GEOMETRY":
		createType = "BLOB"
	case "MySQL_JSON":
		createType = "NVARCHAR2(2000)"
	case "MySQL_BIGINT":
		createType = "NUMBER(19, 0)"
	case "MySQL_DECIMAL":
		createType = fmt.Sprintf(
			"NUMBER(%d,%d)",
			resultSetColInfo.ColumnPrecisions[colNum],
			resultSetColInfo.ColumnScales[colNum],
		)
	case "Oracle_OCIClobLocator":
		createType = "NCLOB"
	case "Oracle_OCIBlobLocator":
		createType = "BLOB"
	case "Oracle_LONG":
		createType = "LONG"
	case "Oracle_NUMBER":
		createType = "NUMBER"
	case "Oracle_IBFloat":
		createType = "BINARY_FLOAT"
	case "Oracle_IBDouble":
		createType = "BINARY_DOUBLE"
	case "Oracle_DATE":
		createType = "DATE"
	case "Oracle_TimeStampDTY":
		createType = "TIMESTAMP"
	case "Oracle_TimeStampTZ_DTY":
		createType = "TIMESTAMP WITH TIME ZONE"
	case "Oracle_TimeStampLTZ_DTY":
		createType = "TIMESTAMP WITH LOCAL TIME ZONE"
	case "Oracle_CHAR":
		createType = "VARCHAR2(4000)"
	case "Oracle_NCHAR":
		createType = "VARCHAR2(4000)"

	case "MSSQL_BIGINT":
		createType = "NUMBER(19, 0)"
	case "MSSQL_BIT":
		createType = "NUMBER(1)"
	case "MSSQL_INT":
		createType = "INTEGER"
	case "MSSQL_MONEY":
		createType = "VARCHAR2(4000)"
	case "MSSQL_SMALLINT":
		createType = "INTEGER"
	case "MSSQL_SMALLMONEY":
		createType = "VARCHAR2(4000)"
	case "MSSQL_TINYINT":
		createType = "INTEGER"
	case "MSSQL_FLOAT":
		createType = "BINARY_DOUBLE"
	case "MSSQL_REAL":
		createType = "BINARY_FLOAT"
	case "MSSQL_DATE":
		createType = "DATE"
	case "MSSQL_DATETIME2":
		createType = "TIMESTAMP"
	case "MSSQL_DATETIME":
		createType = "TIMESTAMP"
	case "MSSQL_DATETIMEOFFSET":
		createType = "TIMESTAMP WITH TIME ZONE"
	case "MSSQL_SMALLDATETIME":
		createType = "TIMESTAMP"
	case "MSSQL_TIME":
		createType = "VARCHAR2(4000)"
	case "MSSQL_TEXT":
		createType = "VARCHAR2(4000)"
	case "MSSQL_NTEXT":
		createType = "NVARCHAR2(2000)"
	case "MSSQL_BINARY":
		createType = "BLOB"
	case "MSSQL_VARBINARY":
		createType = "BLOB"
	case "MSSQL_UNIQUEIDENTIFIER":
		createType = "VARCHAR2(4000)"
	case "MSSQL_XML":
		createType = "NVARCHAR2(2000)"
	case "MSSQL_CHAR":
		createType = fmt.Sprintf(
			"CHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	case "MSSQL_VARCHAR":
		createType = fmt.Sprintf(
			"VARCHAR2(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	case "MSSQL_NCHAR":
		createType = fmt.Sprintf(
			"NCHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	case "MSSQL_NVARCHAR":
		createType = fmt.Sprintf(
			"NVARCHAR2(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	case "MSSQL_DECIMAL":
		createType = fmt.Sprintf(
			"NUMBER(%d,%d)",
			resultSetColInfo.ColumnPrecisions[colNum],
			resultSetColInfo.ColumnScales[colNum],
		)

	case "Snowflake_NUMBER":
		createType = "BINARY_DOUBLE"
	case "Snowflake_BINARY":
		createType = "BLOB"
	case "Snowflake_REAL":
		createType = "BINARY_DOUBLE"
	case "Snowflake_TEXT":
		createType = "NVARCHAR2(2000)"
	case "Snowflake_BOOLEAN":
		createType = "NUMBER(1)"
	case "Snowflake_DATE":
		createType = "DATE"
	case "Snowflake_TIME":
		createType = "VARCHAR2(4000)"
	case "Snowflake_TIMESTAMP_LTZ":
		createType = "TIMESTAMP WITH LOCAL TIME ZONE"
	case "Snowflake_TIMESTAMP_NTZ":
		createType = "TIMESTAMP"
	case "Snowflake_TIMESTAMP_TZ":
		createType = "TIMESTAMP WITH TIME ZONE"
	case "Snowflake_VARIANT":
		createType = "NVARCHAR2(2000)"
	case "Snowflake_OBJECT":
		createType = "NVARCHAR2(2000)"
	case "Snowflake_ARRAY":
		createType = "NVARCHAR2(2000)"

	case "Redshift_BIGINT":
		createType = "NUMBER(19, 0)"
	case "Redshift_BOOLEAN":
		createType = "NUMBER(1)"
	case "Redshift_CHAR":
		createType = "NVARCHAR2(2000)"
	case "Redshift_BPCHAR":
		createType = "NVARCHAR2(2000)"
	case "Redshift_DATE":
		createType = "DATE"
	case "Redshift_DOUBLE":
		createType = "BINARY_DOUBLE"
	case "Redshift_INT":
		createType = "INTEGER"
	case "Redshift_NUMERIC":
		createType = "BINARY_DOUBLE"
	case "Redshift_REAL":
		createType = "BINARY_FLOAT"
	case "Redshift_SMALLINT":
		createType = "INTEGER"
	case "Redshift_TIME":
		createType = "VARCHAR2(4000)"
	case "Redshift_TIMETZ":
		createType = "VARCHAR2(4000)"
	case "Redshift_TIMESTAMP":
		createType = "TIMESTAMP"
	case "Redshift_TIMESTAMPTZ":
		createType = "TIMESTAMP WITH TIME ZONE"
	case "Redshift_VARCHAR":
		createType = fmt.Sprintf(
			"NVARCHAR2(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	default:
		createType = "NVARCHAR2(2000)"
	}

	return createType
}

var oracleIntermediateTypes = map[string]string{
	"CHAR":             "Oracle_CHAR",
	"NCHAR":            "Oracle_NCHAR",
	"OCIClobLocator":   "Oracle_OCIClobLocator",
	"OCIBlobLocator":   "Oracle_OCIBlobLocator",
	"LONG":             "Oracle_LONG",
	"NUMBER":           "Oracle_NUMBER",
	"IBFloat":          "Oracle_IBFloat",
	"IBDouble":         "Oracle_IBDouble",
	"DATE":             "Oracle_DATE",
	"TimeStampDTY":     "Oracle_TimeStampDTY",
	"TimeStampTZ_DTY":  "Oracle_TimeStampTZ_DTY",
	"TimeStampLTZ_DTY": "Oracle_TimeStampLTZ_DTY",
	"NOT":              "Oracle_NOT",
	"OracleType(109)":  "Oracle_OracleType(109)",
}

var oracleCreateTableTypes = map[string]func(columnInfo ResultSetColumnInfo, colNum int) string{

	// Oracle
	"Oracle_OCIClobLocator":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "NCLOB" },
	"Oracle_OCIBlobLocator":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"Oracle_LONG":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "LONG" },
	"Oracle_NUMBER":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER" },
	"Oracle_IBFloat":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_FLOAT" },
	"Oracle_IBDouble":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"Oracle_DATE":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Oracle_TimeStampDTY":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Oracle_TimeStampTZ_DTY": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP WITH TIME ZONE" },
	"Oracle_TimeStampLTZ_DTY": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return "TIMESTAMP WITH LOCAL TIME ZONE"
	},
	"Oracle_CHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR2(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"Oracle_NCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR2(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	// "Oracle_OracleType(109)": func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	// "Oracle_NOT":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },

	// PostgreSQL

	"PostgreSQL_BIGINT":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(19, 0)" },
	"PostgreSQL_BIT":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_VARBIT":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_BOOLEAN":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(1)" },
	"PostgreSQL_BOX":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_BYTEA":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"PostgreSQL_BPCHAR":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_CIDR":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_CIRCLE":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_DATE":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"PostgreSQL_FLOAT8":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"PostgreSQL_INET":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_INT4":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"PostgreSQL_INTERVAL":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_JSON":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_JSONB":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_LINE":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_LSEG":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_MACADDR":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_MONEY":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_PATH":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_PG_LSN":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_POINT":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_POLYGON":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_FLOAT4":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_FLOAT" },
	"PostgreSQL_INT2":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"PostgreSQL_TEXT":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_TIME":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_TIMETZ":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_TIMESTAMP":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"PostgreSQL_TIMESTAMPTZ":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP WITH TIME ZONE" },
	"PostgreSQL_TSQUERY":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_TSVECTOR":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_TXID_SNAPSHOT": func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_UUID":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_XML":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_VARCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR2(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"PostgreSQL_DECIMAL": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMBER(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MySQL

	"MySQL_BIT":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MySQL_TINYINT":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MySQL_SMALLINT":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MySQL_MEDIUMINT": func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MySQL_INT":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MySQL_FLOAT4":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_FLOAT" },
	"MySQL_FLOAT8":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"MySQL_DATE":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MySQL_TIME":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"MySQL_DATETIME":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_TIMESTAMP": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_YEAR":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MySQL_CHAR":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"MySQL_VARCHAR":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"MySQL_TEXT":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"MySQL_BINARY":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MySQL_VARBINARY": func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MySQL_BLOB":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MySQL_GEOMETRY":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MySQL_JSON":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"MySQL_BIGINT":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(19, 0)" },
	"MySQL_DECIMAL": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMBER(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MSSQL

	"MSSQL_BIGINT":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(19, 0)" },
	"MSSQL_BIT":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(1)" },
	"MSSQL_INT":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MSSQL_MONEY":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"MSSQL_SMALLINT":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MSSQL_SMALLMONEY":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"MSSQL_TINYINT":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MSSQL_FLOAT":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"MSSQL_REAL":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_FLOAT" },
	"MSSQL_DATE":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MSSQL_DATETIME2":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIME":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIMEOFFSET":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP WITH TIME ZONE" },
	"MSSQL_SMALLDATETIME":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_TIME":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"MSSQL_TEXT":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"MSSQL_NTEXT":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"MSSQL_BINARY":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MSSQL_VARBINARY":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MSSQL_UNIQUEIDENTIFIER": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"MSSQL_XML":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"MSSQL_CHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"CHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_VARCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR2(%d)",
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
			"NVARCHAR2(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_DECIMAL": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMBER(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// SNOWFLAKE

	"Snowflake_NUMBER":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"Snowflake_BINARY":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"Snowflake_REAL":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"Snowflake_TEXT":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"Snowflake_BOOLEAN": func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(1)" },
	"Snowflake_DATE":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Snowflake_TIME":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"Snowflake_TIMESTAMP_LTZ": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return "TIMESTAMP WITH LOCAL TIME ZONE"
	},
	"Snowflake_TIMESTAMP_NTZ": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Snowflake_TIMESTAMP_TZ":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP WITH TIME ZONE" },
	"Snowflake_VARIANT":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"Snowflake_OBJECT":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"Snowflake_ARRAY":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },

	// Redshift

	"Redshift_BIGINT":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(19, 0)" },
	"Redshift_BOOLEAN":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(1)" },
	"Redshift_CHAR":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"Redshift_BPCHAR":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"Redshift_DATE":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Redshift_DOUBLE":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"Redshift_INT":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"Redshift_NUMERIC":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"Redshift_REAL":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_FLOAT" },
	"Redshift_SMALLINT":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"Redshift_TIME":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"Redshift_TIMETZ":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"Redshift_TIMESTAMP":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Redshift_TIMESTAMPTZ": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP WITH TIME ZONE" },
	"Redshift_VARCHAR": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR2(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
}

var oracleValWriters = map[string]func(value interface{}, terminator string) string{

	// Generics
	"bool":    oracleWriteBool,
	"float32": writeInsertFloat,
	"float64": writeInsertFloat,
	"int16":   writeInsertInt,
	"int32":   writeInsertInt,
	"int64":   writeInsertInt,
	"Time":    oracleWriteDatetimeFromTime,

	// Oracle

	"Oracle_CHAR":             writeInsertEscapedString,
	"Oracle_NCHAR":            writeInsertEscapedString,
	"Oracle_OCIClobLocator":   writeInsertEscapedString,
	"Oracle_OCIBlobLocator":   oracleWriteBlob,
	"Oracle_LONG":             writeInsertEscapedString,
	"Oracle_NUMBER":           oracleWriteNumber,
	"Oracle_DATE":             oracleWriteDateFromTime,
	"Oracle_TimeStampDTY":     oracleWriteDatetimeFromTime,
	"Oracle_TimeStampTZ_DTY":  oracleWriteDatetimeFromTime,
	"Oracle_TimeStampLTZ_DTY": oracleWriteDatetimeFromTime,
	"Oracle_IBFloat":          oracleWriteNumber,
	"Oracle_IBDouble":         oracleWriteNumber,
	"Oracle_NOT":              writeInsertEscapedString,
	"Oracle_OracleType(109)":  writeInsertEscapedString,

	// PostgreSQL
	"PostgreSQL_BIGINT":        writeInsertInt,
	"PostgreSQL_BIT":           writeInsertStringNoEscape,
	"PostgreSQL_VARBIT":        writeInsertStringNoEscape,
	"PostgreSQL_BOOLEAN":       oracleWriteBool,
	"PostgreSQL_BOX":           writeInsertStringNoEscape,
	"PostgreSQL_BYTEA":         oracleWriteBlob,
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
	"PostgreSQL_DATE":          oracleWriteDateFromTime,
	"PostgreSQL_JSON":          writeInsertEscapedString,
	"PostgreSQL_JSONB":         writeInsertEscapedString,
	"PostgreSQL_TEXT":          writeInsertEscapedString,
	"PostgreSQL_TIMESTAMP":     oracleWriteDatetimeFromTime,
	"PostgreSQL_TIMESTAMPTZ":   oracleWriteDatetimeFromTime,
	"PostgreSQL_TSQUERY":       writeInsertEscapedString,
	"PostgreSQL_TSVECTOR":      writeInsertEscapedString,
	"PostgreSQL_XML":           writeInsertEscapedString,

	// MySQL
	"MySQL_BIT":       oracleWriteBlob,
	"MySQL_TINYINT":   writeInsertRawStringNoQuotes,
	"MySQL_SMALLINT":  writeInsertRawStringNoQuotes,
	"MySQL_MEDIUMINT": writeInsertRawStringNoQuotes,
	"MySQL_INT":       writeInsertRawStringNoQuotes,
	"MySQL_BIGINT":    writeInsertRawStringNoQuotes,
	"MySQL_DECIMAL":   writeInsertRawStringNoQuotes,
	"MySQL_FLOAT4":    writeInsertRawStringNoQuotes,
	"MySQL_FLOAT8":    writeInsertRawStringNoQuotes,
	"MySQL_DATE":      oracleWriteDateFromString,
	"MySQL_TIME":      writeInsertStringNoEscape,
	"MySQL_TIMESTAMP": oracleWriteDatetimeFromString,
	"MySQL_DATETIME":  oracleWriteDatetimeFromString,
	"MySQL_YEAR":      writeInsertRawStringNoQuotes,
	"MySQL_CHAR":      writeInsertEscapedString,
	"MySQL_VARCHAR":   writeInsertEscapedString,
	"MySQL_TEXT":      writeInsertEscapedString,
	"MySQL_BINARY":    oracleWriteBlob,
	"MySQL_VARBINARY": oracleWriteBlob,
	"MySQL_BLOB":      oracleWriteBlob,
	"MySQL_GEOMETRY":  oracleWriteBlob,
	"MySQL_JSON":      writeInsertEscapedString,

	// MSSQL
	"MSSQL_BIGINT":           writeInsertInt,
	"MSSQL_BIT":              oracleWriteBool,
	"MSSQL_DECIMAL":          writeInsertRawStringNoQuotes,
	"MSSQL_INT":              writeInsertInt,
	"MSSQL_MONEY":            writeInsertStringNoEscape,
	"MSSQL_SMALLINT":         writeInsertInt,
	"MSSQL_SMALLMONEY":       writeInsertStringNoEscape,
	"MSSQL_TINYINT":          writeInsertInt,
	"MSSQL_FLOAT":            writeInsertFloat,
	"MSSQL_REAL":             writeInsertFloat,
	"MSSQL_DATE":             oracleWriteDatetimeFromTime,
	"MSSQL_DATETIME2":        oracleWriteDatetimeFromTime,
	"MSSQL_DATETIME":         oracleWriteDatetimeFromTime,
	"MSSQL_DATETIMEOFFSET":   oracleWriteDatetimeFromTime,
	"MSSQL_SMALLDATETIME":    oracleWriteDatetimeFromTime,
	"MSSQL_TIME":             oracleWriteDatetimeFromTime,
	"MSSQL_CHAR":             writeInsertEscapedString,
	"MSSQL_VARCHAR":          writeInsertEscapedString,
	"MSSQL_TEXT":             writeInsertEscapedString,
	"MSSQL_NCHAR":            writeInsertEscapedString,
	"MSSQL_NVARCHAR":         writeInsertEscapedString,
	"MSSQL_NTEXT":            writeInsertEscapedString,
	"MSSQL_BINARY":           oracleWriteBlob,
	"MSSQL_VARBINARY":        oracleWriteBlob,
	"MSSQL_UNIQUEIDENTIFIER": oracleWriteBlob,
	"MSSQL_XML":              writeInsertEscapedString,

	// SNOWFLAKE

	"Snowflake_NUMBER":        writeInsertRawStringNoQuotes,
	"Snowflake_REAL":          writeInsertRawStringNoQuotes,
	"Snowflake_TEXT":          writeInsertEscapedString,
	"Snowflake_BOOLEAN":       writeInsertStringNoEscape,
	"Snowflake_DATE":          oracleWriteDateFromTime,
	"Snowflake_TIME":          oracleWriteDatetimeFromTime,
	"Snowflake_TIMESTAMP_LTZ": oracleWriteDatetimeFromTime,
	"Snowflake_TIMESTAMP_NTZ": oracleWriteDatetimeFromTime,
	"Snowflake_TIMESTAMP_TZ":  oracleWriteDatetimeFromTime,
	"Snowflake_VARIANT":       writeInsertEscapedStringRemoveNewines,
	"Snowflake_OBJECT":        writeInsertEscapedStringRemoveNewines,
	"Snowflake_ARRAY":         writeInsertEscapedStringRemoveNewines,
	"Snowflake_BINARY":        oracleWriteBlob,

	// Redshift

	"Redshift_BIGINT":      writeInsertInt,
	"Redshift_BOOLEAN":     oracleWriteBool,
	"Redshift_CHAR":        writeInsertEscapedString,
	"Redshift_BPCHAR":      writeInsertEscapedString,
	"Redshift_VARCHAR":     writeInsertEscapedString,
	"Redshift_DATE":        oracleWriteDateFromTime,
	"Redshift_DOUBLE":      writeInsertFloat,
	"Redshift_INT":         writeInsertInt,
	"Redshift_NUMERIC":     writeInsertRawStringNoQuotes,
	"Redshift_REAL":        writeInsertFloat,
	"Redshift_SMALLINT":    writeInsertInt,
	"Redshift_TIME":        writeInsertStringNoEscape,
	"Redshift_TIMETZ":      writeInsertStringNoEscape,
	"Redshift_TIMESTAMP":   oracleWriteDatetimeFromTime,
	"Redshift_TIMESTAMPTZ": oracleWriteDatetimeFromTime,
}
