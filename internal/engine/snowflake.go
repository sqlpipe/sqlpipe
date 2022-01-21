//go:build allDbs
// +build allDbs

package engine

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io/ioutil"
	"sqlpipe/app/models"
	"sqlpipe/global/structs"
	"strings"
	"sync"
	"time"

	_ "github.com/snowflakedb/gosnowflake"
)

var snowflake *sql.DB
var snowflakeDsInfo structs.DsInfo

type Snowflake struct {
	dsType          string
	driverName      string `json:"-"`
	connString      string `json:"-"`
	debugConnString string
	canConnect      bool
}

func (dsConn Snowflake) GetDb() *sql.DB {
	return snowflake
}

func (dsConn Snowflake) SetCanConnect(canConnect bool) {
	dsConn.canConnect = canConnect
}

func (dsConn Snowflake) GetDsInfo() structs.DsInfo {
	return snowflakeDsInfo
}

func getNewSnowflake(dsInfo structs.DsInfo) DsConnection {

	snowflakeDsInfo = dsInfo

	connString := fmt.Sprintf(
		"%v:%v@%v/%v",
		dsInfo.Username,
		dsInfo.Password,
		dsInfo.AccountId,
		dsInfo.DbName,
	)

	var err error
	snowflake, err = sql.Open("snowflake", connString)
	snowflake.SetConnMaxLifetime(time.Minute * 1)

	if err != nil {
		panic(fmt.Sprintf("couldn't open a connection to Snowflake at host %s", dsInfo.Host))
	}

	return Snowflake{
		"snowflake",
		"snowflake",
		fmt.Sprintf(
			"%v:%v@%v/%v",
			dsInfo.Username,
			dsInfo.Password,
			dsInfo.AccountId,
			dsInfo.DbName,
		),
		fmt.Sprintf(
			"<USERNAME_MASKED>:<PASSWORD_MASKED>@%v/%v",
			dsInfo.AccountId,
			dsInfo.DbName,
		),
		false,
	}
}

func (dsConn Snowflake) getRows(transfer models.Transfer) (*sql.Rows, structs.ResultSetColumnInfo) {
	return standardGetRows(dsConn, transfer)
}

func (dsConn Snowflake) getFormattedResults(query string) structs.QueryResult {
	return standardGetFormattedResults(dsConn, queryInfo)
}

func (dsConn Snowflake) getIntermediateType(colTypeFromDriver string) string {
	return snowflakeIntermediateTypes[colTypeFromDriver]
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
	transfer models.Transfer,
	dsConn DsConnection,
	stageName string,
) {
	defer queryWg.Done()
	f, _ := ioutil.TempFile("", "sqlpipe")
	// defer os.Remove(f.Name())
	_, _ = f.WriteString(
		strings.TrimSuffix(turboEndStringNilReplacer.Replace(fileContents), "\n"),
	)

	putQuery := fmt.Sprintf(`put file://%s @%s SOURCE_COMPRESSION=NONE AUTO_COMPRESS=FALSE`, f.Name(), stageName)
	execute(dsConn, putQuery)
}

func (dsConn Snowflake) turboTransfer(
	rows *sql.Rows,
	transfer models.Transfer,
	resultSetColumnInfo structs.ResultSetColumnInfo,
) (err error) {

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
			go snowflakePut(fileBuilder.String(), &wg, transfer, dsConn, stageName)
			dataInRam = false
			fileBuilder.Reset()
		}
	}

	// if we still have some leftovers, add those too.
	if dataInRam {
		wg.Add(1)
		go snowflakePut(fileBuilder.String(), &wg, transfer, dsConn, stageName)
	}
	wg.Wait()

	copyQuery := fmt.Sprintf(`copy into %s.%s from @%s file_format = 'sqlpipe_csv'`, transfer.TargetSchema, transfer.TargetTable, stageName)
	execute(dsConn, copyQuery)

	dropStageQuery := fmt.Sprintf(`DROP STAGE %s;`, stageName)
	execute(dsConn, dropStageQuery)

	return err
}

func (db Snowflake) insertChecker(currentLen int, currentRow int) bool {
	if currentRow%3000 == 0 {
		return true
	} else {
		return false
	}
}

func (db Snowflake) turboInsertChecker(currentLen int, currentRow int) bool {
	if currentLen%10000 == 0 {
		return true
	} else {
		return false
	}
}

func (dsConn Snowflake) dropTable(transfer models.Transfer) {
	dropTableIfExistsWithSchema(dsConn, transfer)
}

func (dsConn Snowflake) deleteFromTable(transfer models.Transfer) {
	deleteFromTableWithSchema(dsConn, transfer)
}

func (dsConn Snowflake) createTable(transfer models.Transfer, columnInfo structs.ResultSetColumnInfo) string {
	return standardCreateTable(dsConn, transfer, columnInfo)
}

func (dsConn Snowflake) getValToWriteMidRow(valType string, value interface{}) string {
	return snowflakeValWriters[valType](value, ",")
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

func (dsConn Snowflake) getQueryStarter(targetTable string, columnInfo structs.ResultSetColumnInfo) string {
	for _, colType := range columnInfo.ColumnIntermediateTypes {
		switch colType {
		case
			"PostgreSQL_JSON_string",
			"PostgreSQL_JSONB_string",
			"MySQL_JSON_sql.RawBytes",
			"Snowflake_VARIANT_string",
			"Snowflake_OBJECT_string",
			"Snowflake_ARRAY_string":

			sep := ""
			var queryBuilder strings.Builder
			fmt.Fprintf(&queryBuilder, "INSERT INTO %s (%s) SELECT ", targetTable, strings.Join(columnInfo.ColumnNames, ", "))
			for i, colType := range columnInfo.ColumnIntermediateTypes {
				colNum := i + 1
				switch colType {
				case
					"PostgreSQL_JSON_string",
					"PostgreSQL_JSONB_string",
					"MySQL_JSON_sql.RawBytes",
					"Snowflake_VARIANT_string",
					"Snowflake_OBJECT_string",
					"Snowflake_ARRAY_string":
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

func (dsConn Snowflake) getCreateTableType(resultSetColInfo structs.ResultSetColumnInfo, colNum int) string {
	return snowflakeCreateTableTypes[resultSetColInfo.ColumnIntermediateTypes[colNum]](resultSetColInfo, colNum)
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

var snowflakeCreateTableTypes = map[string]func(columnInfo structs.ResultSetColumnInfo, colNum int) string{

	// Snowflake

	"Snowflake_NUMBER_float64":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Snowflake_BINARY_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"Snowflake_REAL_float64":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Snowflake_TEXT_string":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Snowflake_BOOLEAN_bool":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Snowflake_DATE_time.Time":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Snowflake_TIME_time.Time":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIME" },
	"Snowflake_TIMESTAMP_LTZ_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP_LTZ" },
	"Snowflake_TIMESTAMP_NTZ_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP_NTZ" },
	"Snowflake_TIMESTAMP_TZ_time.Time":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP_TZ" },
	"Snowflake_VARIANT_string":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARIANT" },
	"Snowflake_OBJECT_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "OBJECT" },
	"Snowflake_ARRAY_string":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "ARRAY" },

	// PostgreSQL

	"PostgreSQL_BIGINT_int64":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"PostgreSQL_BIT_string":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_VARBIT_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_BOOLEAN_bool":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"PostgreSQL_BOX_string":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_BYTEA_[]uint8":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"PostgreSQL_BPCHAR_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_CIDR_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_CIRCLE_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_DATE_time.Time":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"PostgreSQL_FLOAT8_float64":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"PostgreSQL_INET_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_INT4_int32":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INTEGER" },
	"PostgreSQL_INTERVAL_string":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_JSON_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARIANT" },
	"PostgreSQL_JSONB_string":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARIANT" },
	"PostgreSQL_LINE_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_LSEG_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_MACADDR_string":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_MONEY_string":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_PATH_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_PG_LSN_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_POINT_string":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_POLYGON_string":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_FLOAT4_float32":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"PostgreSQL_INT2_int16":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"PostgreSQL_TEXT_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_TIME_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIME" },
	"PostgreSQL_TIMETZ_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_TIMESTAMP_time.Time":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"PostgreSQL_TIMESTAMPTZ_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP_TZ" },
	"PostgreSQL_TSQUERY_string":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_TSVECTOR_string":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_TXID_SNAPSHOT_string":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_UUID_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_XML_string":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"PostgreSQL_VARCHAR_string": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"PostgreSQL_DECIMAL_string": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMBER(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MySQL

	"MySQL_BIT_sql.RawBytes":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MySQL_TINYINT_sql.RawBytes":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TINYINT" },
	"MySQL_SMALLINT_sql.RawBytes":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MySQL_MEDIUMINT_sql.RawBytes": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_INT_sql.RawBytes":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_BIGINT_sql.NullInt64":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MySQL_FLOAT4_sql.NullFloat64": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MySQL_FLOAT8_sql.NullFloat64": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MySQL_DATE_sql.NullTime":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MySQL_TIME_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MySQL_DATETIME_sql.NullTime":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_TIMESTAMP_sql.NullTime": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MySQL_YEAR_sql.NullInt64":     func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"MySQL_CHAR_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MySQL_VARCHAR_sql.RawBytes":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MySQL_TEXT_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MySQL_BINARY_sql.RawBytes":    func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MySQL_VARBINARY_sql.RawBytes": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MySQL_BLOB_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MySQL_GEOMETRY_sql.RawBytes":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MySQL_JSON_sql.RawBytes":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "VARIANT" },
	"MySQL_DECIMAL_sql.RawBytes": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMBER(%d,%d)",
			columnInfo.ColumnPrecisions[colNum],
			columnInfo.ColumnScales[colNum],
		)
	},

	// MSSQL

	"MSSQL_BIGINT_int64":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"MSSQL_BIT_bool":                 func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"MSSQL_INT_int64":                func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"MSSQL_MONEY_[]uint8":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_SMALLINT_int64":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"MSSQL_SMALLMONEY_[]uint8":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_TINYINT_int64":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TINYINT" },
	"MSSQL_FLOAT_float64":            func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MSSQL_REAL_float64":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"MSSQL_DATE_time.Time":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"MSSQL_DATETIME2_time.Time":      func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIME_time.Time":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_DATETIMEOFFSET_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP_TZ" },
	"MSSQL_SMALLDATETIME_time.Time":  func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"MSSQL_TIME_time.Time":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIME" },
	"MSSQL_TEXT_string":              func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_NTEXT_string":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_BINARY_[]uint8":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MSSQL_VARBINARY_[]uint8":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"MSSQL_UNIQUEIDENTIFIER_[]uint8": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_XML_string":               func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"MSSQL_DECIMAL_[]uint8": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"NUMBER(%d,%d)",
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
			"CHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},
	"MSSQL_NVARCHAR_string": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
		return fmt.Sprintf(
			"VARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// Oracle

	"Oracle_OCIClobLocator_interface{}": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Oracle_OCIBlobLocator_interface{}": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BINARY" },
	"Oracle_LONG_interface{}":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Oracle_NUMBER_interface{}":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
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
			"VARCHAR(%d)",
			columnInfo.ColumnLengths[colNum],
		)
	},

	// Redshift

	"Redshift_BIGINT_int64":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BIGINT" },
	"Redshift_BOOLEAN_bool":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "BOOLEAN" },
	"Redshift_CHAR_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Redshift_BPCHAR_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Redshift_DATE_time.Time":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "DATE" },
	"Redshift_DOUBLE_float64":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Redshift_INT_int32":             func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "INT" },
	"Redshift_NUMERIC_float64":       func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Redshift_REAL_float32":          func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "FLOAT" },
	"Redshift_SMALLINT_int16":        func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "SMALLINT" },
	"Redshift_TIME_string":           func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Redshift_TIMETZ_string":         func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TEXT" },
	"Redshift_TIMESTAMP_time.Time":   func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP" },
	"Redshift_TIMESTAMPTZ_time.Time": func(columnInfo structs.ResultSetColumnInfo, colNum int) string { return "TIMESTAMP_TZ" },
	"Redshift_VARCHAR_string": func(columnInfo structs.ResultSetColumnInfo, colNum int) string {
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

	// Snowflake

	"Snowflake_NUMBER_float64":          writeStringNoQuotesMidTurbo,
	"Snowflake_REAL_float64":            writeStringNoQuotesMidTurbo,
	"Snowflake_TEXT_string":             writeEscapedQuotedStringMidTurbo,
	"Snowflake_BOOLEAN_bool":            writeStringNoQuotesMidTurbo,
	"Snowflake_DATE_time.Time":          snowflakeWriteTimestampFromTimeMidTurbo,
	"Snowflake_TIME_time.Time":          snowflakeWriteTimeFromTimeMidTurbo,
	"Snowflake_TIMESTAMP_LTZ_time.Time": snowflakeWriteTimestampFromTimeMidTurbo,
	"Snowflake_TIMESTAMP_NTZ_time.Time": snowflakeWriteTimestampFromTimeMidTurbo,
	"Snowflake_TIMESTAMP_TZ_time.Time":  snowflakeWriteTimestampFromTimeMidTurbo,
	"Snowflake_VARIANT_string":          writeEscapedQuotedStringMidTurbo,
	"Snowflake_OBJECT_string":           writeEscapedQuotedStringMidTurbo,
	"Snowflake_ARRAY_string":            writeEscapedQuotedStringMidTurbo,
	"Snowflake_BINARY_string":           snowflakeWriteBinaryfromBytesMidTurbo,

	// PostgreSQL

	"PostgreSQL_BIGINT_int64":          writeIntMidTurbo,
	"PostgreSQL_BIT_string":            writeStringNoQuotesMidTurbo,
	"PostgreSQL_VARBIT_string":         writeStringNoQuotesMidTurbo,
	"PostgreSQL_BOOLEAN_bool":          writeBoolMidTurbo,
	"PostgreSQL_BOX_string":            writeQuotedStringMidTurbo,
	"PostgreSQL_BYTEA_[]uint8":         snowflakeWriteBinaryfromBytesMidTurbo,
	"PostgreSQL_CIDR_string":           writeStringNoQuotesMidTurbo,
	"PostgreSQL_CIRCLE_string":         writeQuotedStringMidTurbo,
	"PostgreSQL_FLOAT8_float64":        writeFloatMidTurbo,
	"PostgreSQL_INET_string":           writeStringNoQuotesMidTurbo,
	"PostgreSQL_INT4_int32":            writeIntMidTurbo,
	"PostgreSQL_INTERVAL_string":       writeStringNoQuotesMidTurbo,
	"PostgreSQL_LINE_string":           writeQuotedStringMidTurbo,
	"PostgreSQL_LSEG_string":           writeQuotedStringMidTurbo,
	"PostgreSQL_MACADDR_string":        writeStringNoQuotesMidTurbo,
	"PostgreSQL_MONEY_string":          writeQuotedStringMidTurbo,
	"PostgreSQL_DECIMAL_string":        writeStringNoQuotesMidTurbo,
	"PostgreSQL_PATH_string":           writeQuotedStringMidTurbo,
	"PostgreSQL_PG_LSN_string":         writeStringNoQuotesMidTurbo,
	"PostgreSQL_POINT_string":          writeQuotedStringMidTurbo,
	"PostgreSQL_POLYGON_string":        writeQuotedStringMidTurbo,
	"PostgreSQL_FLOAT4_float32":        writeFloatMidTurbo,
	"PostgreSQL_INT2_int16":            writeIntMidTurbo,
	"PostgreSQL_TIME_string":           writeStringNoQuotesMidTurbo,
	"PostgreSQL_TIMETZ_string":         writeStringNoQuotesMidTurbo,
	"PostgreSQL_TXID_SNAPSHOT_string":  writeStringNoQuotesMidTurbo,
	"PostgreSQL_UUID_string":           writeStringNoQuotesMidTurbo,
	"PostgreSQL_VARCHAR_string":        writeEscapedQuotedStringMidTurbo,
	"PostgreSQL_BPCHAR_string":         writeEscapedQuotedStringMidTurbo,
	"PostgreSQL_DATE_time.Time":        snowflakeWriteTimestampFromTimeMidTurbo,
	"PostgreSQL_JSON_string":           writeJSONMidTurbo,
	"PostgreSQL_JSONB_string":          writeJSONMidTurbo,
	"PostgreSQL_TEXT_string":           writeEscapedQuotedStringMidTurbo,
	"PostgreSQL_TIMESTAMP_time.Time":   snowflakeWriteTimestampFromTimeMidTurbo,
	"PostgreSQL_TIMESTAMPTZ_time.Time": snowflakeWriteTimestampFromTimeMidTurbo,
	"PostgreSQL_TSQUERY_string":        writeEscapedQuotedStringMidTurbo,
	"PostgreSQL_TSVECTOR_string":       writeEscapedQuotedStringMidTurbo,
	"PostgreSQL_XML_string":            writeStringNoQuotesMidTurbo,

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

	"PostgreSQL_BIGINT_int64":          writeIntEndTurbo,
	"PostgreSQL_BIT_string":            writeStringNoQuotesEndTurbo,
	"PostgreSQL_VARBIT_string":         writeStringNoQuotesEndTurbo,
	"PostgreSQL_BOOLEAN_bool":          writeBoolEndTurbo,
	"PostgreSQL_BOX_string":            writeQuotedStringEndTurbo,
	"PostgreSQL_BYTEA_[]uint8":         snowflakeWriteBinaryfromBytesEndTurbo,
	"PostgreSQL_CIDR_string":           writeStringNoQuotesEndTurbo,
	"PostgreSQL_CIRCLE_string":         writeQuotedStringEndTurbo,
	"PostgreSQL_FLOAT8_float64":        writeFloatEndTurbo,
	"PostgreSQL_INET_string":           writeStringNoQuotesEndTurbo,
	"PostgreSQL_INT4_int32":            writeIntEndTurbo,
	"PostgreSQL_INTERVAL_string":       writeStringNoQuotesEndTurbo,
	"PostgreSQL_LINE_string":           writeQuotedStringEndTurbo,
	"PostgreSQL_LSEG_string":           writeQuotedStringEndTurbo,
	"PostgreSQL_MACADDR_string":        writeStringNoQuotesEndTurbo,
	"PostgreSQL_MONEY_string":          writeStringNoQuotesEndTurbo,
	"PostgreSQL_DECIMAL_string":        writeStringNoQuotesEndTurbo,
	"PostgreSQL_PATH_string":           writeQuotedStringEndTurbo,
	"PostgreSQL_PG_LSN_string":         writeStringNoQuotesEndTurbo,
	"PostgreSQL_POINT_string":          writeQuotedStringEndTurbo,
	"PostgreSQL_POLYGON_string":        writeQuotedStringEndTurbo,
	"PostgreSQL_FLOAT4_float32":        writeFloatEndTurbo,
	"PostgreSQL_INT2_int16":            writeIntEndTurbo,
	"PostgreSQL_TIME_string":           writeStringNoQuotesEndTurbo,
	"PostgreSQL_TIMETZ_string":         writeStringNoQuotesEndTurbo,
	"PostgreSQL_TXID_SNAPSHOT_string":  writeStringNoQuotesEndTurbo,
	"PostgreSQL_UUID_string":           writeStringNoQuotesEndTurbo,
	"PostgreSQL_VARCHAR_string":        writeEscapedQuotedStringEndTurbo,
	"PostgreSQL_BPCHAR_string":         writeEscapedQuotedStringEndTurbo,
	"PostgreSQL_DATE_time.Time":        snowflakeWriteTimestampFromTimeEndTurbo,
	"PostgreSQL_JSON_string":           writeJSONEndTurbo,
	"PostgreSQL_JSONB_string":          writeJSONEndTurbo,
	"PostgreSQL_TEXT_string":           writeEscapedQuotedStringEndTurbo,
	"PostgreSQL_TIMESTAMP_time.Time":   snowflakeWriteTimestampFromTimeEndTurbo,
	"PostgreSQL_TIMESTAMPTZ_time.Time": snowflakeWriteTimestampFromTimeEndTurbo,
	"PostgreSQL_TSQUERY_string":        writeEscapedQuotedStringEndTurbo,
	"PostgreSQL_TSVECTOR_string":       writeEscapedQuotedStringEndTurbo,
	"PostgreSQL_XML_string":            writeStringNoQuotesEndTurbo,

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

	// Snowflake

	"Snowflake_NUMBER_float64":          writeInsertRawStringNoQuotes,
	"Snowflake_REAL_float64":            writeInsertRawStringNoQuotes,
	"Snowflake_TEXT_string":             writeInsertEscapedString,
	"Snowflake_BOOLEAN_bool":            writeInsertStringNoEscape,
	"Snowflake_DATE_time.Time":          snowflakeWriteTimestampFromTime,
	"Snowflake_TIME_time.Time":          snowflakeWriteTimeFromTime,
	"Snowflake_TIMESTAMP_LTZ_time.Time": snowflakeWriteTimestampFromTime,
	"Snowflake_TIMESTAMP_NTZ_time.Time": snowflakeWriteTimestampFromTime,
	"Snowflake_TIMESTAMP_TZ_time.Time":  snowflakeWriteTimestampFromTime,
	"Snowflake_VARIANT_string":          writeBackslashJSON,
	"Snowflake_OBJECT_string":           writeBackslashJSON,
	"Snowflake_ARRAY_string":            writeBackslashJSON,
	"Snowflake_BINARY_string":           snowflakeWriteBinaryfromBytes,

	// PostgreSQL

	"PostgreSQL_BIGINT_int64":          writeInsertInt,
	"PostgreSQL_BIT_string":            writeInsertStringNoEscape,
	"PostgreSQL_VARBIT_string":         writeInsertStringNoEscape,
	"PostgreSQL_BOOLEAN_bool":          writeInsertBool,
	"PostgreSQL_BOX_string":            writeInsertStringNoEscape,
	"PostgreSQL_BYTEA_[]uint8":         snowflakeWriteBinaryfromBytes,
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
	"PostgreSQL_DATE_time.Time":        snowflakeWriteTimestampFromTime,
	"PostgreSQL_JSON_string":           writeBackslashJSON,
	"PostgreSQL_JSONB_string":          writeBackslashJSON,
	"PostgreSQL_TEXT_string":           writeInsertEscapedString,
	"PostgreSQL_TIMESTAMP_time.Time":   snowflakeWriteTimestampFromTime,
	"PostgreSQL_TIMESTAMPTZ_time.Time": snowflakeWriteTimestampFromTime,
	"PostgreSQL_TSQUERY_string":        writeInsertEscapedString,
	"PostgreSQL_TSVECTOR_string":       writeInsertEscapedString,
	"PostgreSQL_XML_string":            writeInsertEscapedString,

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
