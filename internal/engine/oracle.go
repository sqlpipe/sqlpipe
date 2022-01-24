package engine

import (
	"database/sql"
	"errors"
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
) (
	errProperties map[string]string,
	err error,
) {
	return errProperties, errors.New("oracle hasn't implemented turbo writing yet")
}

func (dsConn Oracle) turboWriteEndVal(
	valType string,
	value interface{},
	builder *strings.Builder,
) (
	errProperties map[string]string,
	err error,
) {
	return errProperties, errors.New("oracle hasn't implemented turbo writing yet")
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
	fmt.Println(valType)
	return oracleValWriters[valType](value, ",")
}

func (dsConn Oracle) getValToWriteRowEnd(valType string, value interface{}) string {
	return oracleValWriters[valType](value, " FROM dual UNION ALL ")
}

func (dsConn Oracle) getValToWriteRaw(valType string, value interface{}) string {
	fmt.Println(valType)
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
	}

	return createType
}

var oracleIntermediateTypes = map[string]string{
	"CHAR":             "Oracle_CHAR_interface{}",
	"NCHAR":            "Oracle_NCHAR_interface{}",
	"OCIClobLocator":   "Oracle_OCIClobLocator_interface{}",
	"OCIBlobLocator":   "Oracle_OCIBlobLocator_interface{}",
	"LONG":             "Oracle_LONG_interface{}",
	"NUMBER":           "Oracle_NUMBER_interface{}",
	"IBFloat":          "Oracle_IBFloat_interface{}",
	"IBDouble":         "Oracle_IBDouble_interface{}",
	"DATE":             "Oracle_DATE_interface{}",
	"TimeStampDTY":     "Oracle_TimeStampDTY_interface{}",
	"TimeStampTZ_DTY":  "Oracle_TimeStampTZ_DTY_interface{}",
	"TimeStampLTZ_DTY": "Oracle_TimeStampLTZ_DTY_interface{}",
	"NOT":              "Oracle_NOT_interface{}",
	"OracleType(109)":  "Oracle_OracleType(109)_interface{}",
}

var oracleCreateTableTypes = map[string]func(columnInfo ResultSetColumnInfo, colNum int) string{

	// Oracle
	"Oracle_OCIClobLocator_interface{}":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "NCLOB" },
	"Oracle_OCIBlobLocator_interface{}":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"Oracle_LONG_interface{}":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "LONG" },
	"Oracle_NUMBER_interface{}":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER" },
	"Oracle_IBFloat_interface{}":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_FLOAT" },
	"Oracle_IBDouble_interface{}":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"Oracle_DATE_interface{}":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Oracle_TimeStampDTY_interface{}":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Oracle_TimeStampTZ_DTY_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP WITH TIME ZONE" },
	"Oracle_TimeStampLTZ_DTY_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return "TIMESTAMP WITH LOCAL TIME ZONE"
	},
	"Oracle_CHAR_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR2(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"Oracle_NCHAR_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR2(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	// "Oracle_OracleType(109)_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	// "Oracle_NOT_interface{}":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },

	// PostgreSQL

	"PostgreSQL_BIGINT_int64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(19, 0)" },
	"PostgreSQL_BIT_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_VARBIT_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_BOOLEAN_bool":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(1)" },
	"PostgreSQL_BOX_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_BYTEA_[]uint8":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"PostgreSQL_BPCHAR_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_CIDR_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_CIRCLE_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_DATE_time.Time":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"PostgreSQL_FLOAT8_float64":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"PostgreSQL_INET_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_INT4_int32":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"PostgreSQL_INTERVAL_string":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_JSON_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_JSONB_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_LINE_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_LSEG_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_MACADDR_string":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_MONEY_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_PATH_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_PG_LSN_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_POINT_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_POLYGON_string":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_FLOAT4_float32":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_FLOAT" },
	"PostgreSQL_INT2_int16":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"PostgreSQL_TEXT_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_TIME_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_TIMETZ_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_TIMESTAMP_time.Time":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"PostgreSQL_TIMESTAMPTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP WITH TIME ZONE" },
	"PostgreSQL_TSQUERY_string":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_TSVECTOR_string":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_TXID_SNAPSHOT_string":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_UUID_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"PostgreSQL_XML_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"PostgreSQL_VARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NVARCHAR2(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"PostgreSQL_DECIMAL_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMBER(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MySQL

	"MySQL_BIT_sql.RawBytes":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MySQL_TINYINT_sql.RawBytes":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MySQL_SMALLINT_sql.RawBytes":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MySQL_MEDIUMINT_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MySQL_INT_sql.RawBytes":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MySQL_FLOAT4_sql.NullFloat64": func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_FLOAT" },
	"MySQL_FLOAT8_sql.NullFloat64": func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"MySQL_DATE_sql.NullTime":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MySQL_TIME_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"MySQL_DATETIME_sql.NullTime":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_TIMESTAMP_sql.NullTime": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_YEAR_sql.NullInt64":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MySQL_CHAR_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"MySQL_VARCHAR_sql.RawBytes":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"MySQL_TEXT_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"MySQL_BINARY_sql.RawBytes":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MySQL_VARBINARY_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MySQL_BLOB_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MySQL_GEOMETRY_sql.RawBytes":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MySQL_JSON_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"MySQL_BIGINT_sql.NullInt64":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(19, 0)" },
	"MySQL_DECIMAL_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMBER(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MSSQL

	"MSSQL_BIGINT_int64":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(19, 0)" },
	"MSSQL_BIT_bool":                 func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(1)" },
	"MSSQL_INT_int64":                func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MSSQL_MONEY_[]uint8":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"MSSQL_SMALLINT_int64":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MSSQL_SMALLMONEY_[]uint8":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"MSSQL_TINYINT_int64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"MSSQL_FLOAT_float64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"MSSQL_REAL_float64":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_FLOAT" },
	"MSSQL_DATE_time.Time":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MSSQL_DATETIME2_time.Time":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIME_time.Time":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIMEOFFSET_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP WITH TIME ZONE" },
	"MSSQL_SMALLDATETIME_time.Time":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_TIME_time.Time":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"MSSQL_TEXT_string":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"MSSQL_NTEXT_string":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"MSSQL_BINARY_[]uint8":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MSSQL_VARBINARY_[]uint8":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"MSSQL_XML_string":               func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"MSSQL_CHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"CHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_VARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR2(%d)",
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
			"NVARCHAR2(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_DECIMAL_[]uint8": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMBER(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// SNOWFLAKE

	"Snowflake_NUMBER_float64": func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"Snowflake_BINARY_string":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "BLOB" },
	"Snowflake_REAL_float64":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"Snowflake_TEXT_string":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"Snowflake_BOOLEAN_bool":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(1)" },
	"Snowflake_DATE_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Snowflake_TIME_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"Snowflake_TIMESTAMP_LTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return "TIMESTAMP WITH LOCAL TIME ZONE"
	},
	"Snowflake_TIMESTAMP_NTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Snowflake_TIMESTAMP_TZ_time.Time":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP WITH TIME ZONE" },
	"Snowflake_VARIANT_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"Snowflake_OBJECT_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"Snowflake_ARRAY_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },

	// Redshift

	"Redshift_BIGINT_int64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(19, 0)" },
	"Redshift_BOOLEAN_bool":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "NUMBER(1)" },
	"Redshift_CHAR_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"Redshift_BPCHAR_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "NVARCHAR2(2000)" },
	"Redshift_DATE_time.Time":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Redshift_DOUBLE_float64":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"Redshift_INT_int32":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"Redshift_NUMERIC_float64":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_DOUBLE" },
	"Redshift_REAL_float32":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY_FLOAT" },
	"Redshift_SMALLINT_int16":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"Redshift_TIME_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"Redshift_TIMETZ_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARCHAR2(4000)" },
	"Redshift_TIMESTAMP_time.Time":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Redshift_TIMESTAMPTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP WITH TIME ZONE" },
	"Redshift_VARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
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

	"Oracle_CHAR":            writeInsertEscapedString,
	"Oracle_NCHAR":           writeInsertEscapedString,
	"Oracle_OCIClobLocator":  writeInsertEscapedString,
	"Oracle_OCIBlobLocator":  oracleWriteBlob,
	"Oracle_LONG":            writeInsertEscapedString,
	"Oracle_NUMBER":          oracleWriteNumber,
	"Oracle_DATE":            oracleWriteDateFromTime,
	"Oracle_TimeStampDTY":    oracleWriteDatetimeFromTime,
	"Oracle_IBFloat":         oracleWriteNumber,
	"Oracle_IBDouble":        oracleWriteNumber,
	"Oracle_NOT":             writeInsertEscapedString,
	"Oracle_OracleType(109)": writeInsertEscapedString,

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
	"MySQL_BIT_sql.RawBytes":       oracleWriteBlob,
	"MySQL_TINYINT_sql.RawBytes":   writeInsertRawStringNoQuotes,
	"MySQL_SMALLINT_sql.RawBytes":  writeInsertRawStringNoQuotes,
	"MySQL_MEDIUMINT_sql.RawBytes": writeInsertRawStringNoQuotes,
	"MySQL_INT_sql.RawBytes":       writeInsertRawStringNoQuotes,
	"MySQL_BIGINT_sql.NullInt64":   writeInsertRawStringNoQuotes,
	"MySQL_DECIMAL_sql.RawBytes":   writeInsertRawStringNoQuotes,
	"MySQL_FLOAT4_sql.NullFloat64": writeInsertRawStringNoQuotes,
	"MySQL_FLOAT8_sql.NullFloat64": writeInsertRawStringNoQuotes,
	"MySQL_DATE_sql.NullTime":      oracleWriteDateFromString,
	"MySQL_TIME_sql.RawBytes":      writeInsertStringNoEscape,
	"MySQL_TIMESTAMP_sql.NullTime": oracleWriteDatetimeFromString,
	"MySQL_DATETIME_sql.NullTime":  oracleWriteDatetimeFromString,
	"MySQL_YEAR_sql.NullInt64":     writeInsertRawStringNoQuotes,
	"MySQL_CHAR_sql.RawBytes":      writeInsertEscapedString,
	"MySQL_VARCHAR_sql.RawBytes":   writeInsertEscapedString,
	"MySQL_TEXT_sql.RawBytes":      writeInsertEscapedString,
	"MySQL_BINARY_sql.RawBytes":    oracleWriteBlob,
	"MySQL_VARBINARY_sql.RawBytes": oracleWriteBlob,
	"MySQL_BLOB_sql.RawBytes":      oracleWriteBlob,
	"MySQL_GEOMETRY_sql.RawBytes":  oracleWriteBlob,
	"MySQL_JSON_sql.RawBytes":      writeInsertEscapedString,

	// MSSQL
	"MSSQL_BIGINT_int64":             writeInsertInt,
	"MSSQL_BIT_bool":                 oracleWriteBool,
	"MSSQL_DECIMAL_[]uint8":          writeInsertRawStringNoQuotes,
	"MSSQL_INT_int64":                writeInsertInt,
	"MSSQL_MONEY_[]uint8":            writeInsertStringNoEscape,
	"MSSQL_SMALLINT_int64":           writeInsertInt,
	"MSSQL_SMALLMONEY_[]uint8":       writeInsertStringNoEscape,
	"MSSQL_TINYINT_int64":            writeInsertInt,
	"MSSQL_FLOAT_float64":            writeInsertFloat,
	"MSSQL_REAL_float64":             writeInsertFloat,
	"MSSQL_DATE_time.Time":           oracleWriteDatetimeFromTime,
	"MSSQL_DATETIME2_time.Time":      oracleWriteDatetimeFromTime,
	"MSSQL_DATETIME_time.Time":       oracleWriteDatetimeFromTime,
	"MSSQL_DATETIMEOFFSET_time.Time": oracleWriteDatetimeFromTime,
	"MSSQL_SMALLDATETIME_time.Time":  oracleWriteDatetimeFromTime,
	"MSSQL_TIME_time.Time":           oracleWriteDatetimeFromTime,
	"MSSQL_CHAR_string":              writeInsertEscapedString,
	"MSSQL_VARCHAR_string":           writeInsertEscapedString,
	"MSSQL_TEXT_string":              writeInsertEscapedString,
	"MSSQL_NCHAR_string":             writeInsertEscapedString,
	"MSSQL_NVARCHAR_string":          writeInsertEscapedString,
	"MSSQL_NTEXT_string":             writeInsertEscapedString,
	"MSSQL_BINARY_[]uint8":           oracleWriteBlob,
	"MSSQL_VARBINARY_[]uint8":        oracleWriteBlob,
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": oracleWriteBlob,
	"MSSQL_XML_string":               writeInsertEscapedString,

	// SNOWFLAKE

	"Snowflake_NUMBER_float64":          writeInsertRawStringNoQuotes,
	"Snowflake_REAL_float64":            writeInsertRawStringNoQuotes,
	"Snowflake_TEXT_string":             writeInsertEscapedString,
	"Snowflake_BOOLEAN_bool":            writeInsertStringNoEscape,
	"Snowflake_DATE_time.Time":          oracleWriteDateFromTime,
	"Snowflake_TIME_time.Time":          oracleWriteDatetimeFromTime,
	"Snowflake_TIMESTAMP_LTZ_time.Time": oracleWriteDatetimeFromTime,
	"Snowflake_TIMESTAMP_NTZ_time.Time": oracleWriteDatetimeFromTime,
	"Snowflake_TIMESTAMP_TZ_time.Time":  oracleWriteDatetimeFromTime,
	"Snowflake_VARIANT_string":          writeInsertEscapedStringRemoveNewines,
	"Snowflake_OBJECT_string":           writeInsertEscapedStringRemoveNewines,
	"Snowflake_ARRAY_string":            writeInsertEscapedStringRemoveNewines,
	"Snowflake_BINARY_string":           oracleWriteBlob,

	// Redshift

	"Redshift_BIGINT_int64":          writeInsertInt,
	"Redshift_BOOLEAN_bool":          oracleWriteBool,
	"Redshift_CHAR_string":           writeInsertEscapedString,
	"Redshift_BPCHAR_string":         writeInsertEscapedString,
	"Redshift_VARCHAR_string":        writeInsertEscapedString,
	"Redshift_DATE_time.Time":        oracleWriteDateFromTime,
	"Redshift_DOUBLE_float64":        writeInsertFloat,
	"Redshift_INT_int32":             writeInsertInt,
	"Redshift_NUMERIC_float64":       writeInsertRawStringNoQuotes,
	"Redshift_REAL_float32":          writeInsertFloat,
	"Redshift_SMALLINT_int16":        writeInsertInt,
	"Redshift_TIME_string":           writeInsertStringNoEscape,
	"Redshift_TIMETZ_string":         writeInsertStringNoEscape,
	"Redshift_TIMESTAMP_time.Time":   oracleWriteDatetimeFromTime,
	"Redshift_TIMESTAMPTZ_time.Time": oracleWriteDatetimeFromTime,
}
