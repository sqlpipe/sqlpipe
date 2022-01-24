package engine

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/pkg"
	_ "github.com/snowflakedb/gosnowflake"
)

var snowflake *sql.DB

type Snowflake struct {
	dsType          string
	driverName      string `json:"-"`
	connString      string `json:"-"`
	debugConnString string
}

func getNewSnowflake(
	connection data.Connection,
) (
	dsConn DsConnection,
	errProperties map[string]string,
	err error,
) {

	connString := fmt.Sprintf(
		"%v:%v@%v/%v",
		connection.Username,
		connection.Password,
		connection.AccountId,
		connection.DbName,
	)

	snowflake, err = sql.Open("snowflake", connString)

	if err != nil {
		return dsConn, errProperties, err
	}

	snowflake.SetConnMaxLifetime(time.Minute * 1)

	dsConn = Snowflake{
		"snowflake",
		"snowflake",
		fmt.Sprintf(
			"%v:%v@%v/%v",
			connection.Username,
			connection.Password,
			connection.AccountId,
			connection.DbName,
		),
		fmt.Sprintf(
			"<USERNAME_MASKED>:<PASSWORD_MASKED>@%v/%v",
			connection.AccountId,
			connection.DbName,
		),
	}

	return dsConn, errProperties, err
}

func (dsConn Snowflake) getRows(
	transfer data.Transfer,
) (
	rows *sql.Rows,
	resultSetColumnInfo ResultSetColumnInfo,
	errProperties map[string]string,
	err error,
) {
	return standardGetRows(dsConn, transfer)
}

func (dsConn Snowflake) getFormattedResults(
	query string,
) (
	queryResult QueryResult,
	errProperties map[string]string,
	err error,
) {
	return standardGetFormattedResults(dsConn, query)
}

func (dsConn Snowflake) getIntermediateType(
	colTypeFromDriver string,
) (
	intermediateType string,
	errProperties map[string]string,
	err error,
) {
	switch colTypeFromDriver {
	case "FIXED":
		intermediateType = "float64"
	case "REAL":
		intermediateType = "float64"
	case "TEXT":
		intermediateType = "Snowflake_TEXT"
	case "BOOLEAN":
		intermediateType = "bool"
	case "DATE":
		intermediateType = "Time"
	case "TIME":
		intermediateType = "Time"
	case "TIMESTAMP_LTZ":
		intermediateType = "Time"
	case "TIMESTAMP_NTZ":
		intermediateType = "Time"
	case "TIMESTAMP_TZ":
		intermediateType = "Time"
	case "VARIANT":
		intermediateType = "Snowflake_VARIANT" // https://media.giphy.com/media/JYfcUkZgQxZgx1TlZM/giphy.gif
	case "OBJECT":
		intermediateType = "Snowflake_OBJECT"
	case "ARRAY":
		intermediateType = "Snowflake_ARRAY"
	case "BINARY":
		intermediateType = "Snowflake_BINARY"
	default:
		err = fmt.Errorf("no Snowflake intermediate type for driver type '%v'", colTypeFromDriver)
	}

	return intermediateType, errProperties, err
}

func (dsConn Snowflake) getConnectionInfo() (string, string, string) {
	return dsConn.dsType, dsConn.driverName, dsConn.connString
}

func (dsConn Snowflake) GetDebugInfo() (string, string) {
	return dsConn.dsType, dsConn.debugConnString
}

func snowflakePut(
	fileContents string,
	queryWg *sync.WaitGroup,
	transfer data.Transfer,
	dsConn DsConnection,
	stageName string,
) (
	rows *sql.Rows,
	errProperties map[string]string,
	err error,
) {
	defer queryWg.Done()
	f, _ := ioutil.TempFile("", "sqlpipe")
	// defer os.Remove(f.Name())
	_, _ = f.WriteString(
		strings.TrimSuffix(turboEndStringNilReplacer.Replace(fileContents), "\n"),
	)

	putQuery := fmt.Sprintf(`put file://%s @%s SOURCE_COMPRESSION=NONE AUTO_COMPRESS=FALSE`, f.Name(), stageName)
	return execute(dsConn, putQuery)
}

func (dsConn Snowflake) turboTransfer(
	rows *sql.Rows,
	transfer data.Transfer,
	resultSetColumnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	defer func() (
		errProperties map[string]string,
		err error,
	) {
		if panicInterface := recover(); panicInterface != nil {
			panicVal, ok := panicInterface.(map[string]string)
			if ok {
				if putErr, ok := panicVal["putError"]; ok {
					err = errors.New(putErr)
					delete(panicVal, "putError")
				}
			}
		}
		return errProperties, err
	}()

	numCols := resultSetColumnInfo.NumCols
	zeroIndexedNumCols := numCols - 1
	// targetTable := transfer.TargetTable
	colTypes := resultSetColumnInfo.ColumnIntermediateTypes

	var wg sync.WaitGroup
	var fileBuilder strings.Builder
	defer fileBuilder.Reset()

	vals := make([]interface{}, numCols)
	valPtrs := make([]interface{}, numCols)
	dataInRam := false

	// create stage and file format
	h := sha256.New()
	h.Write([]byte(transfer.TargetTable))
	h.Write([]byte(transfer.Query))
	h.Write([]byte(fmt.Sprint(time.Now())))

	stageHash := fmt.Sprintf("%x", h.Sum(nil))[:10]
	stageName := fmt.Sprintf(`public.%s%s`, transfer.TargetTable, stageHash)
	createStageQuery := fmt.Sprintf(`CREATE STAGE %s;`, stageName)
	execute(dsConn, createStageQuery)

	fileFormatQuery := `create or replace file format public.sqlpipe_csv type = csv FIELD_OPTIONALLY_ENCLOSED_BY='"' compression=NONE`
	execute(dsConn, fileFormatQuery)

	for i := 0; i < numCols; i++ {
		valPtrs[i] = &vals[i]
	}

	for i := 1; rows.Next(); i++ {
		rows.Scan(valPtrs...)

		// while in the middle of insert row, add commas at end of values
		for j := 0; j < zeroIndexedNumCols; j++ {
			dsConn.turboWriteMidVal(colTypes[j], vals[j], &fileBuilder)
		}

		// end of row doesn't need a comma at the end
		dsConn.turboWriteEndVal(colTypes[zeroIndexedNumCols], vals[zeroIndexedNumCols], &fileBuilder)
		dataInRam = true

		// each dsConn has its own limits on insert statements (either on total
		// length or number of rows)
		if dsConn.turboInsertChecker(fileBuilder.Len(), i) {
			// fileBuilder.WriteString("\n")
			wg.Add(1)
			pkg.Background(func() {
				_, putErrProperties, putError := snowflakePut(fileBuilder.String(), &wg, transfer, dsConn, stageName)
				if putError != nil {
					putErrProperties["putError"] = putError.Error()
					panic(putErrProperties)
				}
			})
			dataInRam = false
			fileBuilder.Reset()
		}
	}

	// if we still have some leftovers, add those too.
	if dataInRam {
		wg.Add(1)
		pkg.Background(func() {
			_, putErrProperties, putError := snowflakePut(fileBuilder.String(), &wg, transfer, dsConn, stageName)
			if putError != nil {
				putErrProperties["putError"] = putError.Error()
				panic(putErrProperties)
			}
		})
	}
	wg.Wait()

	copyQuery := fmt.Sprintf(`copy into %s.%s from @%s file_format = 'sqlpipe_csv'`, transfer.TargetSchema, transfer.TargetTable, stageName)
	_, errProperties, err = execute(dsConn, copyQuery)
	if err != nil {
		return errProperties, err
	}

	dropStageQuery := fmt.Sprintf(`DROP STAGE %s;`, stageName)
	_, errProperties, err = execute(dsConn, dropStageQuery)

	return errProperties, err
}

func (dsConn Snowflake) insertChecker(currentLen int, currentRow int) bool {
	if currentRow%3000 == 0 {
		return true
	} else {
		return false
	}
}

func (dsConn Snowflake) turboInsertChecker(currentLen int, currentRow int) bool {
	if currentLen%10000 == 0 {
		return true
	} else {
		return false
	}
}

func (dsConn Snowflake) dropTable(
	transfer data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {
	return dropTableIfExistsWithSchema(dsConn, transfer)
}

func (dsConn Snowflake) deleteFromTable(
	transfer data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {
	return deleteFromTableWithSchema(dsConn, transfer)
}

func (dsConn Snowflake) createTable(
	transfer data.Transfer,
	columnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	return standardCreateTable(dsConn, transfer, columnInfo)
}

func (dsConn Snowflake) getValToWriteMidRow(valType string, value interface{}) string {
	return snowflakeValWriters[valType](value, ",")
}

func (dsConn Snowflake) getValToWriteRaw(valType string, value interface{}) string {
	return snowflakeValWriters[valType](value, "")
}

func (dsConn Snowflake) getValToWriteRowEnd(valType string, value interface{}) string {
	return snowflakeValWriters[valType](value, ")")
}

func (dsConn Snowflake) turboWriteMidVal(valType string, value interface{}, builder *strings.Builder) {
	snowflakeTurboWritersMid[valType](value, builder)
}

func (dsConn Snowflake) turboWriteEndVal(valType string, value interface{}, builder *strings.Builder) {
	snowflakeTurboWritersEnd[valType](value, builder)
}

func (dsConn Snowflake) getRowStarter() string {
	return standardGetRowStarter()
}

func (dsConn Snowflake) getQueryEnder(targetTable string) string {
	return ""
}

func (dsConn Snowflake) getQueryStarter(targetTable string, columnInfo ResultSetColumnInfo) string {
	for _, colType := range columnInfo.ColumnIntermediateTypes {
		switch colType {
		case
			"PostgreSQL_JSON",
			"PostgreSQL_JSONB",
			"MySQL_JSON",
			"Snowflake_VARIANT",
			"Snowflake_OBJECT",
			"Snowflake_ARRAY":

			sep := ""
			var queryBuilder strings.Builder
			fmt.Fprintf(&queryBuilder, "INSERT INTO %s (%s) SELECT ", targetTable, strings.Join(columnInfo.ColumnNames, ", "))
			for i, colType := range columnInfo.ColumnIntermediateTypes {
				colNum := i + 1
				switch colType {
				case
					"PostgreSQL_JSON",
					"PostgreSQL_JSONB",
					"MySQL_JSON",
					"Snowflake_VARIANT",
					"Snowflake_OBJECT",
					"Snowflake_ARRAY":
					fmt.Fprintf(&queryBuilder, "%sPARSE_JSON(column%d) ", sep, colNum)
				default:
					fmt.Fprintf(&queryBuilder, "%scolumn%d ", sep, colNum)
				}
				sep = ","
			}
			queryBuilder.WriteString("FROM VALUES (")

			return queryBuilder.String()
		}
	}

	return standardGetQueryStarter(targetTable, columnInfo)
}

func (dsConn Snowflake) getCreateTableType(resultSetColInfo ResultSetColumnInfo, colNum int) (createType string) {
	scanType := resultSetColInfo.ColumnScanTypes[colNum]
	intermediateType := resultSetColInfo.ColumnIntermediateTypes[colNum]

	switch scanType.Name() {
	case "bool":
		createType = "BOOLEAN"
	case "int", "int8", "int16", "int32", "uint8", "uint16":
		createType = "INTEGER"
	case "int64", "uint32", "uint64":
		createType = "BIGINT"
	case "float32", "float64":
		createType = "FLOAT"
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
		createType = "TEXT"
	case "PostgreSQL_VARBIT":
		createType = "TEXT"
	case "PostgreSQL_BOOLEAN":
		createType = "BOOLEAN"
	case "PostgreSQL_BOX":
		createType = "TEXT"
	case "PostgreSQL_BYTEA":
		createType = "BINARY"
	case "PostgreSQL_BPCHAR":
		createType = "TEXT"
	case "PostgreSQL_CIDR":
		createType = "TEXT"
	case "PostgreSQL_CIRCLE":
		createType = "TEXT"
	case "PostgreSQL_DATE":
		createType = "DATE"
	case "PostgreSQL_FLOAT8":
		createType = "FLOAT"
	case "PostgreSQL_INET":
		createType = "TEXT"
	case "PostgreSQL_INT4":
		createType = "INTEGER"
	case "PostgreSQL_INTERVAL":
		createType = "TEXT"
	case "PostgreSQL_JSON":
		createType = "VARIANT"
	case "PostgreSQL_JSONB":
		createType = "VARIANT"
	case "PostgreSQL_LINE":
		createType = "TEXT"
	case "PostgreSQL_LSEG":
		createType = "TEXT"
	case "PostgreSQL_MACADDR":
		createType = "TEXT"
	case "PostgreSQL_MONEY":
		createType = "TEXT"
	case "PostgreSQL_PATH":
		createType = "TEXT"
	case "PostgreSQL_PG_LSN":
		createType = "TEXT"
	case "PostgreSQL_POINT":
		createType = "TEXT"
	case "PostgreSQL_POLYGON":
		createType = "TEXT"
	case "PostgreSQL_FLOAT4":
		createType = "FLOAT"
	case "PostgreSQL_INT2":
		createType = "SMALLINT"
	case "PostgreSQL_TEXT":
		createType = "TEXT"
	case "PostgreSQL_TIME":
		createType = "TIME"
	case "PostgreSQL_TIMETZ":
		createType = "TEXT"
	case "PostgreSQL_TIMESTAMP":
		createType = "TIMESTAMP"
	case "PostgreSQL_TIMESTAMPTZ":
		createType = "TIMESTAMP_TZ"
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
	case "PostgreSQL_VARCHAR":
		createType = fmt.Sprintf(
			"VARCHAR(%d)",
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

func snowflakeWriteTimestampFromTime(value interface{}, terminator string) string {
	var returnVal string

	switch v := value.(type) {
	case time.Time:
		returnVal = fmt.Sprintf("'%s'%s", v.Format("2006-01-02 15:04:05.000000"), terminator)
	default:
		return fmt.Sprintf("null%s", terminator)
	}

	return returnVal
}

func snowflakeWriteTimeFromTime(value interface{}, terminator string) string {
	var returnVal string

	switch v := value.(type) {
	case time.Time:
		returnVal = fmt.Sprintf("'%s'%s", v.Format("15:04:05.000000"), terminator)
	default:
		return fmt.Sprintf("null%s", terminator)
	}

	return returnVal
}

func snowflakeWriteBinaryfromBytes(value interface{}, terminator string) string {
	return fmt.Sprintf("to_binary('%x')%s", value, terminator)
}

var snowflakeIntermediateTypes = map[string]string{
	"FIXED":         "Snowflake_NUMBER_float64",
	"REAL":          "Snowflake_REAL_float64",
	"TEXT":          "Snowflake_TEXT_string",
	"BOOLEAN":       "Snowflake_BOOLEAN_bool",
	"DATE":          "Snowflake_DATE_time.Time",
	"TIME":          "Snowflake_TIME_time.Time",
	"TIMESTAMP_LTZ": "Snowflake_TIMESTAMP_LTZ_time.Time",
	"TIMESTAMP_NTZ": "Snowflake_TIMESTAMP_NTZ_time.Time",
	"TIMESTAMP_TZ":  "Snowflake_TIMESTAMP_TZ_time.Time",
	"VARIANT":       "Snowflake_VARIANT_string", // https://media.giphy.com/media/JYfcUkZgQxZgx1TlZM/giphy.gif
	"OBJECT":        "Snowflake_OBJECT_string",
	"ARRAY":         "Snowflake_ARRAY_string",
	"BINARY":        "Snowflake_BINARY_string",
}

var snowflakeCreateTableTypes = map[string]func(columnInfo ResultSetColumnInfo, colNum int) string{

	// Snowflake

	"Snowflake_NUMBER_float64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Snowflake_BINARY_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"Snowflake_REAL_float64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Snowflake_TEXT_string":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Snowflake_BOOLEAN_bool":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Snowflake_DATE_time.Time":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Snowflake_TIME_time.Time":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"Snowflake_TIMESTAMP_LTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP_LTZ" },
	"Snowflake_TIMESTAMP_NTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP_NTZ" },
	"Snowflake_TIMESTAMP_TZ_time.Time":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP_TZ" },
	"Snowflake_VARIANT_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARIANT" },
	"Snowflake_OBJECT_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "OBJECT" },
	"Snowflake_ARRAY_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "ARRAY" },

	// PostgreSQL

	"PostgreSQL_BIGINT_int64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"PostgreSQL_BIT_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_VARBIT_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_BOOLEAN_bool":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"PostgreSQL_BOX_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_BYTEA_[]uint8":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"PostgreSQL_BPCHAR_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_CIDR_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_CIRCLE_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_DATE_time.Time":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"PostgreSQL_FLOAT8_float64":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"PostgreSQL_INET_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_INT4_int32":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"PostgreSQL_INTERVAL_string":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_JSON":                  func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARIANT" },
	"PostgreSQL_JSONB":                 func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARIANT" },
	"PostgreSQL_LINE_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_LSEG_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_MACADDR_string":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_MONEY_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_PATH_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_PG_LSN_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_POINT_string":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_POLYGON_string":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_FLOAT4_float32":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"PostgreSQL_INT2_int16":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"PostgreSQL_TEXT_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_TIME_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"PostgreSQL_TIMETZ_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_TIMESTAMP_time.Time":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"PostgreSQL_TIMESTAMPTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP_TZ" },
	"PostgreSQL_TSQUERY_string":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_TSVECTOR_string":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_TXID_SNAPSHOT_string":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_UUID_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_XML_string":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_VARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR(%d)",
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

	"MySQL_BIT_sql.RawBytes":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MySQL_TINYINT_sql.RawBytes":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TINYINT" },
	"MySQL_SMALLINT_sql.RawBytes":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MySQL_MEDIUMINT_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_INT_sql.RawBytes":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_BIGINT_sql.NullInt64":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MySQL_FLOAT4_sql.NullFloat64": func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MySQL_FLOAT8_sql.NullFloat64": func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MySQL_DATE_sql.NullTime":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MySQL_TIME_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MySQL_DATETIME_sql.NullTime":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_TIMESTAMP_sql.NullTime": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_YEAR_sql.NullInt64":     func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_CHAR_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MySQL_VARCHAR_sql.RawBytes":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MySQL_TEXT_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MySQL_BINARY_sql.RawBytes":    func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MySQL_VARBINARY_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MySQL_BLOB_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MySQL_GEOMETRY_sql.RawBytes":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MySQL_JSON_sql.RawBytes":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "VARIANT" },
	"MySQL_DECIMAL_sql.RawBytes": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMBER(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MSSQL

	"MSSQL_BIGINT_int64":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MSSQL_BIT_bool":                 func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"MSSQL_INT_int64":                func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"MSSQL_MONEY_[]uint8":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_SMALLINT_int64":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MSSQL_SMALLMONEY_[]uint8":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_TINYINT_int64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "TINYINT" },
	"MSSQL_FLOAT_float64":            func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MSSQL_REAL_float64":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MSSQL_DATE_time.Time":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MSSQL_DATETIME2_time.Time":      func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIME_time.Time":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIMEOFFSET_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP_TZ" },
	"MSSQL_SMALLDATETIME_time.Time":  func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_TIME_time.Time":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MSSQL_TEXT_string":              func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_NTEXT_string":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_BINARY_[]uint8":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MSSQL_VARBINARY_[]uint8":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_XML_string":               func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_DECIMAL_[]uint8": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMBER(%d,%d)",
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
			"CHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_NVARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// Oracle

	"Oracle_OCIClobLocator_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Oracle_OCIBlobLocator_interface{}": func(columnInfo ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"Oracle_LONG_interface{}":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Oracle_NUMBER_interface{}":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
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
			"VARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// Redshift

	"Redshift_BIGINT_int64":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"Redshift_BOOLEAN_bool":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Redshift_CHAR_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Redshift_BPCHAR_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Redshift_DATE_time.Time":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Redshift_DOUBLE_float64":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Redshift_INT_int32":             func(columnInfo ResultSetColumnInfo, colNum int) string { return "INT" },
	"Redshift_NUMERIC_float64":       func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Redshift_REAL_float32":          func(columnInfo ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Redshift_SMALLINT_int16":        func(columnInfo ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"Redshift_TIME_string":           func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Redshift_TIMETZ_string":         func(columnInfo ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Redshift_TIMESTAMP_time.Time":   func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Redshift_TIMESTAMPTZ_time.Time": func(columnInfo ResultSetColumnInfo, colNum int) string { return "TIMESTAMP_TZ" },
	"Redshift_VARCHAR_string": func(columnInfo ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
}

var snowflakeTimeFormat = "2006-01-02 15:04:05.000000"

func snowflakeWriteTimestampFromTimeMidTurbo(value interface{}, builder *strings.Builder) {
	switch value := value.(type) {
	case time.Time:
		fmt.Fprintf(builder, `%s,`, value.Format(snowflakeTimeFormat))
	default:
		builder.WriteString(",")
	}
}

func snowflakeWriteTimestampFromTimeEndTurbo(value interface{}, builder *strings.Builder) {
	switch value := value.(type) {
	case time.Time:
		fmt.Fprintf(builder, "%s\n", value.Format(snowflakeTimeFormat))
	default:
		builder.WriteString("\n")
	}
}

func snowflakeWriteBinaryfromBytesMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, `%x,`, value)
}

func snowflakeWriteBinaryfromBytesEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%x\n", value)
}

func snowflakeWriteTimeFromTimeMidTurbo(value interface{}, builder *strings.Builder) {
	switch value := value.(type) {
	case time.Time:
		fmt.Fprintf(builder, "%s,", value.Format("15:04:05.000000"))
	default:
		builder.WriteString(",")
	}
}

func snowflakeWriteTimeFromTimeEndTurbo(value interface{}, builder *strings.Builder) {
	switch value := value.(type) {
	case time.Time:
		fmt.Fprintf(builder, "%s\n", value.Format("15:04:05.000000"))
	default:
		builder.WriteString("\n")
	}
}

var snowflakeTurboWritersMid = map[string]func(value interface{}, builder *strings.Builder){

	// Generics
	"bool":    writeBoolMidTurbo,
	"float32": writeFloatMidTurbo,
	"float64": writeFloatMidTurbo,
	"int16":   writeIntMidTurbo,
	"int32":   writeIntMidTurbo,
	"int64":   writeIntMidTurbo,
	"Time":    snowflakeWriteTimestampFromTimeMidTurbo,

	// Snowflake

	"Snowflake_NUMBER":        writeStringNoQuotesMidTurbo,
	"Snowflake_REAL":          writeStringNoQuotesMidTurbo,
	"Snowflake_TEXT":          writeEscapedQuotedStringMidTurbo,
	"Snowflake_BOOLEAN":       writeStringNoQuotesMidTurbo,
	"Snowflake_DATE":          snowflakeWriteTimestampFromTimeMidTurbo,
	"Snowflake_TIME":          snowflakeWriteTimeFromTimeMidTurbo,
	"Snowflake_TIMESTAMP_LTZ": snowflakeWriteTimestampFromTimeMidTurbo,
	"Snowflake_TIMESTAMP_NTZ": snowflakeWriteTimestampFromTimeMidTurbo,
	"Snowflake_TIMESTAMP_TZ":  snowflakeWriteTimestampFromTimeMidTurbo,
	"Snowflake_VARIANT":       writeEscapedQuotedStringMidTurbo,
	"Snowflake_OBJECT":        writeEscapedQuotedStringMidTurbo,
	"Snowflake_ARRAY":         writeEscapedQuotedStringMidTurbo,
	"Snowflake_BINARY":        snowflakeWriteBinaryfromBytesMidTurbo,

	// PostgreSQL

	"PostgreSQL_BIGINT":        writeIntMidTurbo,
	"PostgreSQL_BIT":           writeStringNoQuotesMidTurbo,
	"PostgreSQL_VARBIT":        writeStringNoQuotesMidTurbo,
	"PostgreSQL_BOOLEAN":       writeBoolMidTurbo,
	"PostgreSQL_BOX":           writeQuotedStringMidTurbo,
	"PostgreSQL_BYTEA":         snowflakeWriteBinaryfromBytesMidTurbo,
	"PostgreSQL_CIDR":          writeStringNoQuotesMidTurbo,
	"PostgreSQL_CIRCLE":        writeQuotedStringMidTurbo,
	"PostgreSQL_FLOAT8":        writeFloatMidTurbo,
	"PostgreSQL_INET":          writeStringNoQuotesMidTurbo,
	"PostgreSQL_INT4":          writeIntMidTurbo,
	"PostgreSQL_INTERVAL":      writeStringNoQuotesMidTurbo,
	"PostgreSQL_LINE":          writeQuotedStringMidTurbo,
	"PostgreSQL_LSEG":          writeQuotedStringMidTurbo,
	"PostgreSQL_MACADDR":       writeStringNoQuotesMidTurbo,
	"PostgreSQL_MONEY":         writeQuotedStringMidTurbo,
	"PostgreSQL_DECIMAL":       writeStringNoQuotesMidTurbo,
	"PostgreSQL_PATH":          writeQuotedStringMidTurbo,
	"PostgreSQL_PG_LSN":        writeStringNoQuotesMidTurbo,
	"PostgreSQL_POINT":         writeQuotedStringMidTurbo,
	"PostgreSQL_POLYGON":       writeQuotedStringMidTurbo,
	"PostgreSQL_FLOAT4":        writeFloatMidTurbo,
	"PostgreSQL_INT2":          writeIntMidTurbo,
	"PostgreSQL_TIME":          writeStringNoQuotesMidTurbo,
	"PostgreSQL_TIMETZ":        writeStringNoQuotesMidTurbo,
	"PostgreSQL_TXID_SNAPSHOT": writeStringNoQuotesMidTurbo,
	"PostgreSQL_UUID":          writeStringNoQuotesMidTurbo,
	"PostgreSQL_VARCHAR":       writeEscapedQuotedStringMidTurbo,
	"PostgreSQL_BPCHAR":        writeEscapedQuotedStringMidTurbo,
	"PostgreSQL_DATE":          snowflakeWriteTimestampFromTimeMidTurbo,
	"PostgreSQL_JSON":          writeJSONMidTurbo,
	"PostgreSQL_JSONB":         writeJSONMidTurbo,
	"PostgreSQL_TEXT":          writeEscapedQuotedStringMidTurbo,
	"PostgreSQL_TIMESTAMP":     snowflakeWriteTimestampFromTimeMidTurbo,
	"PostgreSQL_TIMESTAMPTZ":   snowflakeWriteTimestampFromTimeMidTurbo,
	"PostgreSQL_TSQUERY":       writeEscapedQuotedStringMidTurbo,
	"PostgreSQL_TSVECTOR":      writeEscapedQuotedStringMidTurbo,
	"PostgreSQL_XML":           writeStringNoQuotesMidTurbo,

	// MYSQL

	"MySQL_BIT_sql.RawBytes":       snowflakeWriteBinaryfromBytesMidTurbo,
	"MySQL_TINYINT_sql.RawBytes":   writeStringNoQuotesMidTurbo,
	"MySQL_SMALLINT_sql.RawBytes":  writeStringNoQuotesMidTurbo,
	"MySQL_MEDIUMINT_sql.RawBytes": writeStringNoQuotesMidTurbo,
	"MySQL_INT_sql.RawBytes":       writeStringNoQuotesMidTurbo,
	"MySQL_BIGINT_sql.NullInt64":   writeStringNoQuotesMidTurbo,
	"MySQL_DECIMAL_sql.RawBytes":   writeStringNoQuotesMidTurbo,
	"MySQL_FLOAT4_sql.NullFloat64": writeStringNoQuotesMidTurbo,
	"MySQL_FLOAT8_sql.NullFloat64": writeStringNoQuotesMidTurbo,
	"MySQL_DATE_sql.NullTime":      writeStringNoQuotesMidTurbo,
	"MySQL_TIME_sql.RawBytes":      writeStringNoQuotesMidTurbo,
	"MySQL_TIMESTAMP_sql.NullTime": writeStringNoQuotesMidTurbo,
	"MySQL_DATETIME_sql.NullTime":  writeStringNoQuotesMidTurbo,
	"MySQL_YEAR_sql.NullInt64":     writeStringNoQuotesMidTurbo,
	"MySQL_CHAR_sql.RawBytes":      writeEscapedQuotedStringMidTurbo,
	"MySQL_VARCHAR_sql.RawBytes":   writeEscapedQuotedStringMidTurbo,
	"MySQL_TEXT_sql.RawBytes":      writeEscapedQuotedStringMidTurbo,
	"MySQL_BINARY_sql.RawBytes":    snowflakeWriteBinaryfromBytesMidTurbo,
	"MySQL_VARBINARY_sql.RawBytes": snowflakeWriteBinaryfromBytesMidTurbo,
	"MySQL_BLOB_sql.RawBytes":      snowflakeWriteBinaryfromBytesMidTurbo,
	"MySQL_GEOMETRY_sql.RawBytes":  snowflakeWriteBinaryfromBytesMidTurbo,
	"MySQL_JSON_sql.RawBytes":      writeEscapedQuotedStringMidTurbo,

	// // MSSQL

	"MSSQL_BIGINT_int64":             writeIntMidTurbo,
	"MSSQL_BIT_bool":                 writeBoolMidTurbo,
	"MSSQL_DECIMAL_[]uint8":          writeStringNoQuotesMidTurbo,
	"MSSQL_INT_int64":                writeIntMidTurbo,
	"MSSQL_MONEY_[]uint8":            writeQuotedStringMidTurbo,
	"MSSQL_SMALLINT_int64":           writeIntMidTurbo,
	"MSSQL_SMALLMONEY_[]uint8":       writeQuotedStringMidTurbo,
	"MSSQL_TINYINT_int64":            writeIntMidTurbo,
	"MSSQL_FLOAT_float64":            writeFloatMidTurbo,
	"MSSQL_REAL_float64":             writeFloatMidTurbo,
	"MSSQL_DATE_time.Time":           snowflakeWriteTimestampFromTimeMidTurbo,
	"MSSQL_DATETIME2_time.Time":      snowflakeWriteTimestampFromTimeMidTurbo,
	"MSSQL_DATETIME_time.Time":       snowflakeWriteTimestampFromTimeMidTurbo,
	"MSSQL_DATETIMEOFFSET_time.Time": snowflakeWriteTimestampFromTimeMidTurbo,
	"MSSQL_SMALLDATETIME_time.Time":  snowflakeWriteTimestampFromTimeMidTurbo,
	"MSSQL_TIME_time.Time":           snowflakeWriteTimeFromTimeMidTurbo,
	"MSSQL_CHAR_string":              writeEscapedQuotedStringMidTurbo,
	"MSSQL_VARCHAR_string":           writeEscapedQuotedStringMidTurbo,
	"MSSQL_TEXT_string":              writeEscapedQuotedStringMidTurbo,
	"MSSQL_NCHAR_string":             writeEscapedQuotedStringMidTurbo,
	"MSSQL_NVARCHAR_string":          writeEscapedQuotedStringMidTurbo,
	"MSSQL_NTEXT_string":             writeEscapedQuotedStringMidTurbo,
	"MSSQL_BINARY_[]uint8":           snowflakeWriteBinaryfromBytesMidTurbo,
	"MSSQL_VARBINARY_[]uint8":        snowflakeWriteBinaryfromBytesMidTurbo,
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": writeMSSQLUniqueIdentifierMidTurbo,
	"MSSQL_XML_string":               writeEscapedQuotedStringMidTurbo,

	// // Oracle

	"Oracle_CHAR_interface{}":           writeEscapedQuotedStringMidTurbo,
	"Oracle_NCHAR_interface{}":          writeEscapedQuotedStringMidTurbo,
	"Oracle_OCIClobLocator_interface{}": writeEscapedQuotedStringMidTurbo,
	"Oracle_OCIBlobLocator_interface{}": snowflakeWriteBinaryfromBytesMidTurbo,
	"Oracle_LONG_interface{}":           writeEscapedQuotedStringMidTurbo,
	"Oracle_NUMBER_interface{}":         oracleWriteNumberMidTurbo,
	"Oracle_DATE_interface{}":           snowflakeWriteTimestampFromTimeMidTurbo,
	"Oracle_TimeStampDTY_interface{}":   snowflakeWriteTimestampFromTimeMidTurbo,

	// // Redshift

	"Redshift_BIGINT_int64":          writeIntMidTurbo,
	"Redshift_BOOLEAN_bool":          writeBoolMidTurbo,
	"Redshift_CHAR_string":           writeEscapedQuotedStringMidTurbo,
	"Redshift_BPCHAR_string":         writeEscapedQuotedStringMidTurbo,
	"Redshift_VARCHAR_string":        writeEscapedQuotedStringMidTurbo,
	"Redshift_DATE_time.Time":        snowflakeWriteTimestampFromTimeMidTurbo,
	"Redshift_DOUBLE_float64":        writeFloatMidTurbo,
	"Redshift_INT_int32":             writeIntMidTurbo,
	"Redshift_NUMERIC_float64":       writeStringNoQuotesMidTurbo,
	"Redshift_REAL_float32":          writeFloatMidTurbo,
	"Redshift_SMALLINT_int16":        writeIntMidTurbo,
	"Redshift_TIME_string":           writeStringNoQuotesMidTurbo,
	"Redshift_TIMETZ_string":         writeStringNoQuotesMidTurbo,
	"Redshift_TIMESTAMP_time.Time":   snowflakeWriteTimestampFromTimeMidTurbo,
	"Redshift_TIMESTAMPTZ_time.Time": snowflakeWriteTimestampFromTimeMidTurbo,
}

var snowflakeTurboWritersEnd = map[string]func(value interface{}, builder *strings.Builder){

	// Generics
	"bool":    writeBoolEndTurbo,
	"float32": writeFloatEndTurbo,
	"float64": writeFloatEndTurbo,
	"int16":   writeIntEndTurbo,
	"int32":   writeIntEndTurbo,
	"int64":   writeIntEndTurbo,
	"Time":    snowflakeWriteTimestampFromTimeEndTurbo,

	// Snowflake

	"Snowflake_NUMBER_float64":          writeStringNoQuotesEndTurbo,
	"Snowflake_REAL_float64":            writeStringNoQuotesEndTurbo,
	"Snowflake_TEXT_string":             writeEscapedQuotedStringEndTurbo,
	"Snowflake_BOOLEAN_bool":            writeStringNoQuotesEndTurbo,
	"Snowflake_DATE_time.Time":          snowflakeWriteTimestampFromTimeEndTurbo,
	"Snowflake_TIME_time.Time":          snowflakeWriteTimeFromTimeEndTurbo,
	"Snowflake_TIMESTAMP_LTZ_time.Time": snowflakeWriteTimestampFromTimeEndTurbo,
	"Snowflake_TIMESTAMP_NTZ_time.Time": snowflakeWriteTimestampFromTimeEndTurbo,
	"Snowflake_TIMESTAMP_TZ_time.Time":  snowflakeWriteTimestampFromTimeEndTurbo,
	"Snowflake_VARIANT_string":          writeEscapedQuotedStringEndTurbo,
	"Snowflake_OBJECT_string":           writeEscapedQuotedStringEndTurbo,
	"Snowflake_ARRAY_string":            writeEscapedQuotedStringEndTurbo,
	"Snowflake_BINARY_string":           snowflakeWriteBinaryfromBytesEndTurbo,

	// PostgreSQL

	"PostgreSQL_BIGINT":        writeIntEndTurbo,
	"PostgreSQL_BIT":           writeStringNoQuotesEndTurbo,
	"PostgreSQL_VARBIT":        writeStringNoQuotesEndTurbo,
	"PostgreSQL_BOOLEAN":       writeBoolEndTurbo,
	"PostgreSQL_BOX":           writeQuotedStringEndTurbo,
	"PostgreSQL_BYTEA":         snowflakeWriteBinaryfromBytesEndTurbo,
	"PostgreSQL_CIDR":          writeStringNoQuotesEndTurbo,
	"PostgreSQL_CIRCLE":        writeQuotedStringEndTurbo,
	"PostgreSQL_FLOAT8":        writeFloatEndTurbo,
	"PostgreSQL_INET":          writeStringNoQuotesEndTurbo,
	"PostgreSQL_INT4":          writeIntEndTurbo,
	"PostgreSQL_INTERVAL":      writeStringNoQuotesEndTurbo,
	"PostgreSQL_LINE":          writeQuotedStringEndTurbo,
	"PostgreSQL_LSEG":          writeQuotedStringEndTurbo,
	"PostgreSQL_MACADDR":       writeStringNoQuotesEndTurbo,
	"PostgreSQL_MONEY":         writeStringNoQuotesEndTurbo,
	"PostgreSQL_DECIMAL":       writeStringNoQuotesEndTurbo,
	"PostgreSQL_PATH":          writeQuotedStringEndTurbo,
	"PostgreSQL_PG_LSN":        writeStringNoQuotesEndTurbo,
	"PostgreSQL_POINT":         writeQuotedStringEndTurbo,
	"PostgreSQL_POLYGON":       writeQuotedStringEndTurbo,
	"PostgreSQL_FLOAT4":        writeFloatEndTurbo,
	"PostgreSQL_INT2":          writeIntEndTurbo,
	"PostgreSQL_TIME":          writeStringNoQuotesEndTurbo,
	"PostgreSQL_TIMETZ":        writeStringNoQuotesEndTurbo,
	"PostgreSQL_TXID_SNAPSHOT": writeStringNoQuotesEndTurbo,
	"PostgreSQL_UUID":          writeStringNoQuotesEndTurbo,
	"PostgreSQL_VARCHAR":       writeEscapedQuotedStringEndTurbo,
	"PostgreSQL_BPCHAR":        writeEscapedQuotedStringEndTurbo,
	"PostgreSQL_DATE":          snowflakeWriteTimestampFromTimeEndTurbo,
	"PostgreSQL_JSON":          writeJSONEndTurbo,
	"PostgreSQL_JSONB":         writeJSONEndTurbo,
	"PostgreSQL_TEXT":          writeEscapedQuotedStringEndTurbo,
	"PostgreSQL_TIMESTAMP":     snowflakeWriteTimestampFromTimeEndTurbo,
	"PostgreSQL_TIMESTAMPTZ":   snowflakeWriteTimestampFromTimeEndTurbo,
	"PostgreSQL_TSQUERY":       writeEscapedQuotedStringEndTurbo,
	"PostgreSQL_TSVECTOR":      writeEscapedQuotedStringEndTurbo,
	"PostgreSQL_XML":           writeStringNoQuotesEndTurbo,

	// MYSQL

	"MySQL_BIT_sql.RawBytes":       snowflakeWriteBinaryfromBytesEndTurbo,
	"MySQL_TINYINT_sql.RawBytes":   writeStringNoQuotesEndTurbo,
	"MySQL_SMALLINT_sql.RawBytes":  writeStringNoQuotesEndTurbo,
	"MySQL_MEDIUMINT_sql.RawBytes": writeStringNoQuotesEndTurbo,
	"MySQL_INT_sql.RawBytes":       writeStringNoQuotesEndTurbo,
	"MySQL_BIGINT_sql.NullInt64":   writeStringNoQuotesEndTurbo,
	"MySQL_DECIMAL_sql.RawBytes":   writeStringNoQuotesEndTurbo,
	"MySQL_FLOAT4_sql.NullFloat64": writeStringNoQuotesEndTurbo,
	"MySQL_FLOAT8_sql.NullFloat64": writeStringNoQuotesEndTurbo,
	"MySQL_DATE_sql.NullTime":      writeStringNoQuotesEndTurbo,
	"MySQL_TIME_sql.RawBytes":      writeStringNoQuotesEndTurbo,
	"MySQL_TIMESTAMP_sql.NullTime": writeStringNoQuotesEndTurbo,
	"MySQL_DATETIME_sql.NullTime":  writeStringNoQuotesEndTurbo,
	"MySQL_YEAR_sql.NullInt64":     writeStringNoQuotesEndTurbo,
	"MySQL_CHAR_sql.RawBytes":      writeEscapedQuotedStringEndTurbo,
	"MySQL_VARCHAR_sql.RawBytes":   writeEscapedQuotedStringEndTurbo,
	"MySQL_TEXT_sql.RawBytes":      writeEscapedQuotedStringEndTurbo,
	"MySQL_BINARY_sql.RawBytes":    snowflakeWriteBinaryfromBytesEndTurbo,
	"MySQL_VARBINARY_sql.RawBytes": snowflakeWriteBinaryfromBytesEndTurbo,
	"MySQL_BLOB_sql.RawBytes":      snowflakeWriteBinaryfromBytesEndTurbo,
	"MySQL_GEOMETRY_sql.RawBytes":  snowflakeWriteBinaryfromBytesEndTurbo,
	"MySQL_JSON_sql.RawBytes":      writeEscapedQuotedStringEndTurbo,

	// // MSSQL

	"MSSQL_BIGINT_int64":             writeIntEndTurbo,
	"MSSQL_BIT_bool":                 writeBoolEndTurbo,
	"MSSQL_DECIMAL_[]uint8":          writeStringNoQuotesEndTurbo,
	"MSSQL_INT_int64":                writeIntEndTurbo,
	"MSSQL_MONEY_[]uint8":            writeQuotedStringEndTurbo,
	"MSSQL_SMALLINT_int64":           writeIntEndTurbo,
	"MSSQL_SMALLMONEY_[]uint8":       writeQuotedStringEndTurbo,
	"MSSQL_TINYINT_int64":            writeIntEndTurbo,
	"MSSQL_FLOAT_float64":            writeFloatEndTurbo,
	"MSSQL_REAL_float64":             writeFloatEndTurbo,
	"MSSQL_DATE_time.Time":           snowflakeWriteTimestampFromTimeEndTurbo,
	"MSSQL_DATETIME2_time.Time":      snowflakeWriteTimestampFromTimeEndTurbo,
	"MSSQL_DATETIME_time.Time":       snowflakeWriteTimestampFromTimeEndTurbo,
	"MSSQL_DATETIMEOFFSET_time.Time": snowflakeWriteTimestampFromTimeEndTurbo,
	"MSSQL_SMALLDATETIME_time.Time":  snowflakeWriteTimestampFromTimeEndTurbo,
	"MSSQL_TIME_time.Time":           snowflakeWriteTimeFromTimeEndTurbo,
	"MSSQL_CHAR_string":              writeEscapedQuotedStringEndTurbo,
	"MSSQL_VARCHAR_string":           writeEscapedQuotedStringEndTurbo,
	"MSSQL_TEXT_string":              writeEscapedQuotedStringEndTurbo,
	"MSSQL_NCHAR_string":             writeEscapedQuotedStringEndTurbo,
	"MSSQL_NVARCHAR_string":          writeEscapedQuotedStringEndTurbo,
	"MSSQL_NTEXT_string":             writeEscapedQuotedStringEndTurbo,
	"MSSQL_BINARY_[]uint8":           snowflakeWriteBinaryfromBytesEndTurbo,
	"MSSQL_VARBINARY_[]uint8":        snowflakeWriteBinaryfromBytesEndTurbo,
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": writeMSSQLUniqueIdentifierEndTurbo,
	"MSSQL_XML_string":               writeEscapedQuotedStringEndTurbo,

	// // Oracle

	"Oracle_CHAR_interface{}":           writeEscapedQuotedStringEndTurbo,
	"Oracle_NCHAR_interface{}":          writeEscapedQuotedStringEndTurbo,
	"Oracle_OCIClobLocator_interface{}": writeEscapedQuotedStringEndTurbo,
	"Oracle_OCIBlobLocator_interface{}": snowflakeWriteBinaryfromBytesEndTurbo,
	"Oracle_LONG_interface{}":           writeEscapedQuotedStringEndTurbo,
	"Oracle_NUMBER_interface{}":         oracleWriteNumberEndTurbo,
	"Oracle_DATE_interface{}":           snowflakeWriteTimestampFromTimeEndTurbo,
	"Oracle_TimeStampDTY_interface{}":   snowflakeWriteTimestampFromTimeEndTurbo,

	// // Redshift

	"Redshift_BIGINT_int64":          writeIntEndTurbo,
	"Redshift_BOOLEAN_bool":          writeBoolEndTurbo,
	"Redshift_CHAR_string":           writeEscapedQuotedStringEndTurbo,
	"Redshift_BPCHAR_string":         writeEscapedQuotedStringEndTurbo,
	"Redshift_VARCHAR_string":        writeEscapedQuotedStringEndTurbo,
	"Redshift_DATE_time.Time":        snowflakeWriteTimestampFromTimeEndTurbo,
	"Redshift_DOUBLE_float64":        writeFloatEndTurbo,
	"Redshift_INT_int32":             writeIntEndTurbo,
	"Redshift_NUMERIC_float64":       writeStringNoQuotesEndTurbo,
	"Redshift_REAL_float32":          writeFloatEndTurbo,
	"Redshift_SMALLINT_int16":        writeIntEndTurbo,
	"Redshift_TIME_string":           writeStringNoQuotesEndTurbo,
	"Redshift_TIMETZ_string":         writeStringNoQuotesEndTurbo,
	"Redshift_TIMESTAMP_time.Time":   snowflakeWriteTimestampFromTimeEndTurbo,
	"Redshift_TIMESTAMPTZ_time.Time": snowflakeWriteTimestampFromTimeEndTurbo,
}

var snowflakeValWriters = map[string]func(value interface{}, terminator string) string{

	// Generics
	"bool":    writeInsertBool,
	"float32": writeInsertFloat,
	"float64": writeInsertFloat,
	"int16":   writeInsertInt,
	"int32":   writeInsertInt,
	"int64":   writeInsertInt,
	"Time":    snowflakeWriteTimestampFromTime,

	// Snowflake
	"Snowflake_NUMBER":        writeInsertRawStringNoQuotes,
	"Snowflake_REAL":          writeInsertRawStringNoQuotes,
	"Snowflake_TEXT":          writeInsertEscapedString,
	"Snowflake_BOOLEAN":       writeInsertStringNoEscape,
	"Snowflake_DATE":          snowflakeWriteTimestampFromTime,
	"Snowflake_TIME":          snowflakeWriteTimeFromTime,
	"Snowflake_TIMESTAMP_LTZ": snowflakeWriteTimestampFromTime,
	"Snowflake_TIMESTAMP_NTZ": snowflakeWriteTimestampFromTime,
	"Snowflake_TIMESTAMP_TZ":  snowflakeWriteTimestampFromTime,
	"Snowflake_VARIANT":       writeBackslashJSON,
	"Snowflake_OBJECT":        writeBackslashJSON,
	"Snowflake_ARRAY":         writeBackslashJSON,
	"Snowflake_BINARY":        snowflakeWriteBinaryfromBytes,

	// PostgreSQL

	"PostgreSQL_BIGINT":        writeInsertInt,
	"PostgreSQL_BIT":           writeInsertStringNoEscape,
	"PostgreSQL_VARBIT":        writeInsertStringNoEscape,
	"PostgreSQL_BOOLEAN":       writeInsertBool,
	"PostgreSQL_BOX":           writeInsertStringNoEscape,
	"PostgreSQL_BYTEA":         snowflakeWriteBinaryfromBytes,
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
	"PostgreSQL_DATE":          snowflakeWriteTimestampFromTime,
	"PostgreSQL_JSON":          writeBackslashJSON,
	"PostgreSQL_JSONB":         writeBackslashJSON,
	"PostgreSQL_TEXT":          writeInsertEscapedString,
	"PostgreSQL_TIMESTAMP":     snowflakeWriteTimestampFromTime,
	"PostgreSQL_TIMESTAMPTZ":   snowflakeWriteTimestampFromTime,
	"PostgreSQL_TSQUERY":       writeInsertEscapedString,
	"PostgreSQL_TSVECTOR":      writeInsertEscapedString,
	"PostgreSQL_XML":           writeInsertEscapedString,

	// MYSQL

	"MySQL_BIT_sql.RawBytes":       snowflakeWriteBinaryfromBytes,
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
	"MySQL_BINARY_sql.RawBytes":    snowflakeWriteBinaryfromBytes,
	"MySQL_VARBINARY_sql.RawBytes": snowflakeWriteBinaryfromBytes,
	"MySQL_BLOB_sql.RawBytes":      snowflakeWriteBinaryfromBytes,
	"MySQL_GEOMETRY_sql.RawBytes":  snowflakeWriteBinaryfromBytes,
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
	"MSSQL_DATE_time.Time":           snowflakeWriteTimestampFromTime,
	"MSSQL_DATETIME2_time.Time":      snowflakeWriteTimestampFromTime,
	"MSSQL_DATETIME_time.Time":       snowflakeWriteTimestampFromTime,
	"MSSQL_DATETIMEOFFSET_time.Time": snowflakeWriteTimestampFromTime,
	"MSSQL_SMALLDATETIME_time.Time":  snowflakeWriteTimestampFromTime,
	"MSSQL_TIME_time.Time":           snowflakeWriteTimeFromTime,
	"MSSQL_CHAR_string":              writeInsertEscapedString,
	"MSSQL_VARCHAR_string":           writeInsertEscapedString,
	"MSSQL_TEXT_string":              writeInsertEscapedString,
	"MSSQL_NCHAR_string":             writeInsertEscapedString,
	"MSSQL_NVARCHAR_string":          writeInsertEscapedString,
	"MSSQL_NTEXT_string":             writeInsertEscapedString,
	"MSSQL_BINARY_[]uint8":           snowflakeWriteBinaryfromBytes,
	"MSSQL_VARBINARY_[]uint8":        snowflakeWriteBinaryfromBytes,
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": writeMSSQLUniqueIdentifier,
	"MSSQL_XML_string":               writeInsertEscapedString,

	// Oracle

	"Oracle_CHAR_interface{}":           writeInsertEscapedString,
	"Oracle_NCHAR_interface{}":          writeInsertEscapedString,
	"Oracle_OCIClobLocator_interface{}": writeInsertEscapedString,
	"Oracle_OCIBlobLocator_interface{}": snowflakeWriteBinaryfromBytes,
	"Oracle_LONG_interface{}":           writeInsertEscapedString,
	"Oracle_NUMBER_interface{}":         oracleWriteNumber,
	"Oracle_DATE_interface{}":           snowflakeWriteTimestampFromTime,
	"Oracle_TimeStampDTY_interface{}":   snowflakeWriteTimestampFromTime,

	// Redshift

	"Redshift_BIGINT_int64":          writeInsertInt,
	"Redshift_BOOLEAN_bool":          writeInsertBool,
	"Redshift_CHAR_string":           writeInsertEscapedString,
	"Redshift_BPCHAR_string":         writeInsertEscapedString,
	"Redshift_VARCHAR_string":        writeInsertEscapedString,
	"Redshift_DATE_time.Time":        snowflakeWriteTimestampFromTime,
	"Redshift_DOUBLE_float64":        writeInsertFloat,
	"Redshift_INT_int32":             writeInsertInt,
	"Redshift_NUMERIC_float64":       writeInsertRawStringNoQuotes,
	"Redshift_REAL_float32":          writeInsertFloat,
	"Redshift_SMALLINT_int16":        writeInsertInt,
	"Redshift_TIME_string":           writeInsertStringNoEscape,
	"Redshift_TIMETZ_string":         writeInsertStringNoEscape,
	"Redshift_TIMESTAMP_time.Time":   snowflakeWriteTimestampFromTime,
	"Redshift_TIMESTAMPTZ_time.Time": snowflakeWriteTimestampFromTime,
}
