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
	db              *sql.DB
}

func (dsConn Snowflake) writeSyncInsert(
	row []string,
	relation relation,
	rowsColumnInfo RowsColumnInfo,
) (
	query string,
) {
	return standardWriteSyncInsert(dsConn, row, relation, rowsColumnInfo)
}

func (dsConn Snowflake) execute(query string) (rows *sql.Rows, errProperties map[string]string, err error) {
	return standardExecute(query, dsConn.dsType, dsConn.db)
}

func (dsConn Snowflake) closeDb() {
	dsConn.db.Close()
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
		snowflake,
	}

	return dsConn, errProperties, err
}

func (dsConn Snowflake) getRows(
	transfer data.Transfer,
) (
	rows *sql.Rows,
	rowColumnInfo RowsColumnInfo,
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
		intermediateType = "Snowflake_NUMBER"
	case "REAL":
		intermediateType = "Snowflake_REAL"
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
	rows, errProperties, err = dsConn.execute(putQuery)
	if err != nil {
		return rows, errProperties, err
	}
	defer rows.Close()
	return rows, errProperties, err
}

func (dsConn Snowflake) turboTransfer(
	rows *sql.Rows,
	transfer data.Transfer,
	rowColumnInfo RowsColumnInfo,
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

	numCols := rowColumnInfo.NumCols
	zeroIndexedNumCols := numCols - 1
	// targetTable := transfer.TargetTable
	colTypes := rowColumnInfo.ColumnIntermediateTypes

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
	createRows, errProperties, err := dsConn.execute(createStageQuery)
	if err != nil {
		return errProperties, err
	}
	defer createRows.Close()

	fileFormatQuery := `create or replace file format public.sqlpipe_csv type = csv FIELD_OPTIONALLY_ENCLOSED_BY='"' compression=NONE`
	fileFormatRows, errProperties, err := dsConn.execute(fileFormatQuery)
	if err != nil {
		return errProperties, err
	}
	defer fileFormatRows.Close()

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
	rows, errProperties, err = dsConn.execute(copyQuery)
	if err != nil {
		return errProperties, err
	}
	defer rows.Close()
	if err != nil {
		return errProperties, err
	}

	dropStageQuery := fmt.Sprintf(`DROP STAGE %s;`, stageName)
	rows, errProperties, err = dsConn.execute(dropStageQuery)
	if err != nil {
		return errProperties, err
	}
	defer rows.Close()

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
	columnInfo RowsColumnInfo,
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

func (dsConn Snowflake) getQueryStarter(targetTable string, columnInfo RowsColumnInfo) string {
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

	return standardGetQueryStarter(targetTable, columnInfo.ColumnNames)
}

func (dsConn Snowflake) getCreateTableType(resultSetColInfo RowsColumnInfo, colNum int) (createType string) {
	scanType := resultSetColInfo.ColumnScanTypes[colNum]
	intermediateType := resultSetColInfo.ColumnIntermediateTypes[colNum]

	switch scanType.Name() {
	// Generics
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
	// Generics
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

	// MySQL
	case "MySQL_BIT":
		createType = "BINARY"
	case "MySQL_TINYINT":
		createType = "TINYINT"
	case "MySQL_SMALLINT":
		createType = "SMALLINT"
	case "MySQL_MEDIUMINT":
		createType = "INT"
	case "MySQL_INT":
		createType = "INT"
	case "MySQL_BIGINT":
		createType = "BIGINT"
	case "MySQL_FLOAT4":
		createType = "FLOAT"
	case "MySQL_FLOAT8":
		createType = "FLOAT"
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
		createType = "TEXT"
	case "MySQL_VARCHAR":
		createType = "TEXT"
	case "MySQL_TEXT":
		createType = "TEXT"
	case "MySQL_BINARY":
		createType = "BINARY"
	case "MySQL_VARBINARY":
		createType = "BINARY"
	case "MySQL_BLOB":
		createType = "BINARY"
	case "MySQL_GEOMETRY":
		createType = "BINARY"
	case "MySQL_JSON":
		createType = "VARIANT"
	case "MySQL_DECIMAL":
		createType = fmt.Sprintf(
			"NUMBER(%d,%d)",
			resultSetColInfo.ColumnPrecisions[colNum],
			resultSetColInfo.ColumnScales[colNum],
		)

	case "Snowflake_NUMBER":
		createType = "FLOAT"
	case "Snowflake_BINARY":
		createType = "BINARY"
	case "Snowflake_REAL":
		createType = "FLOAT"
	case "Snowflake_TEXT":
		createType = "TEXT"
	case "Snowflake_BOOLEAN":
		createType = "BOOLEAN"
	case "Snowflake_DATE":
		createType = "DATE"
	case "Snowflake_TIME":
		createType = "TIME"
	case "Snowflake_TIMESTAMP_LTZ":
		createType = "TIMESTAMP_LTZ"
	case "Snowflake_TIMESTAMP_NTZ":
		createType = "TIMESTAMP_NTZ"
	case "Snowflake_TIMESTAMP_TZ":
		createType = "TIMESTAMP_TZ"
	case "Snowflake_VARIANT":
		createType = "VARIANT"
	case "Snowflake_OBJECT":
		createType = "OBJECT"
	case "Snowflake_ARRAY":
		createType = "ARRAY"

	case "MSSQL_BIGINT":
		createType = "BIGINT"
	case "MSSQL_BIT":
		createType = "BOOLEAN"
	case "MSSQL_INT":
		createType = "INT"
	case "MSSQL_MONEY":
		createType = "TEXT"
	case "MSSQL_SMALLINT":
		createType = "SMALLINT"
	case "MSSQL_SMALLMONEY":
		createType = "TEXT"
	case "MSSQL_TINYINT":
		createType = "TINYINT"
	case "MSSQL_FLOAT":
		createType = "FLOAT"
	case "MSSQL_REAL":
		createType = "FLOAT"
	case "MSSQL_DATE":
		createType = "DATE"
	case "MSSQL_DATETIME2":
		createType = "TIMESTAMP"
	case "MSSQL_DATETIME":
		createType = "TIMESTAMP"
	case "MSSQL_DATETIMEOFFSET":
		createType = "TIMESTAMP_TZ"
	case "MSSQL_SMALLDATETIME":
		createType = "TIMESTAMP"
	case "MSSQL_TIME":
		createType = "TIMESTAMP"
	case "MSSQL_TEXT":
		createType = "TEXT"
	case "MSSQL_NTEXT":
		createType = "TEXT"
	case "MSSQL_BINARY":
		createType = "BINARY"
	case "MSSQL_VARBINARY":
		createType = "BINARY"
	case "MSSQL_UNIQUEIDENTIFIER":
		createType = "TEXT"
	case "MSSQL_XML":
		createType = "TEXT"
	case "MSSQL_DECIMAL":
		createType = fmt.Sprintf(
			"NUMBER(%d,%d)",
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
			"CHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	case "MSSQL_NVARCHAR":
		createType = fmt.Sprintf(
			"VARCHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)

	// Oracle

	case "Oracle_OCIClobLocator":
		createType = "TEXT"
	case "Oracle_OCIBlobLocator":
		createType = "BINARY"
	case "Oracle_LONG":
		createType = "TEXT"
	case "Oracle_NUMBER":
		createType = "FLOAT"
	case "Oracle_DATE":
		createType = "DATE"
	case "Oracle_TimeStampDTY":
		createType = "TIMESTAMP"
	case "Oracle_CHAR":
		createType = "TEXT"
	case "Oracle_NCHAR":
		createType = "TEXT"

	// Redshift

	case "Redshift_BIGINT":
		createType = "BIGINT"
	case "Redshift_BOOLEAN":
		createType = "BOOLEAN"
	case "Redshift_CHAR":
		createType = "TEXT"
	case "Redshift_BPCHAR":
		createType = "TEXT"
	case "Redshift_DATE":
		createType = "DATE"
	case "Redshift_DOUBLE":
		createType = "FLOAT"
	case "Redshift_INT":
		createType = "INT"
	case "Redshift_NUMERIC":
		createType = "FLOAT"
	case "Redshift_REAL":
		createType = "FLOAT"
	case "Redshift_SMALLINT":
		createType = "SMALLINT"
	case "Redshift_TIME":
		createType = "TEXT"
	case "Redshift_TIMETZ":
		createType = "TEXT"
	case "Redshift_TIMESTAMP":
		createType = "TIMESTAMP"
	case "Redshift_TIMESTAMPTZ":
		createType = "TIMESTAMP_TZ"
	case "Redshift_VARCHAR":
		createType = fmt.Sprintf(
			"VARCHAR(%d)",
			resultSetColInfo.ColumnLengths[colNum],
		)
	default:
		createType = "TEXT"
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

	"MySQL_BIT":       snowflakeWriteBinaryfromBytesMidTurbo,
	"MySQL_TINYINT":   writeStringNoQuotesMidTurbo,
	"MySQL_SMALLINT":  writeStringNoQuotesMidTurbo,
	"MySQL_MEDIUMINT": writeStringNoQuotesMidTurbo,
	"MySQL_INT":       writeStringNoQuotesMidTurbo,
	"MySQL_BIGINT":    writeStringNoQuotesMidTurbo,
	"MySQL_DECIMAL":   writeStringNoQuotesMidTurbo,
	"MySQL_FLOAT4":    writeStringNoQuotesMidTurbo,
	"MySQL_FLOAT8":    writeStringNoQuotesMidTurbo,
	"MySQL_DATE":      writeStringNoQuotesMidTurbo,
	"MySQL_TIME":      writeStringNoQuotesMidTurbo,
	"MySQL_TIMESTAMP": writeStringNoQuotesMidTurbo,
	"MySQL_DATETIME":  writeStringNoQuotesMidTurbo,
	"MySQL_YEAR":      writeStringNoQuotesMidTurbo,
	"MySQL_CHAR":      writeEscapedQuotedStringMidTurbo,
	"MySQL_VARCHAR":   writeEscapedQuotedStringMidTurbo,
	"MySQL_TEXT":      writeEscapedQuotedStringMidTurbo,
	"MySQL_BINARY":    snowflakeWriteBinaryfromBytesMidTurbo,
	"MySQL_VARBINARY": snowflakeWriteBinaryfromBytesMidTurbo,
	"MySQL_BLOB":      snowflakeWriteBinaryfromBytesMidTurbo,
	"MySQL_GEOMETRY":  snowflakeWriteBinaryfromBytesMidTurbo,
	"MySQL_JSON":      writeEscapedQuotedStringMidTurbo,

	// MSSQL

	"MSSQL_BIGINT":           writeIntMidTurbo,
	"MSSQL_BIT":              writeBoolMidTurbo,
	"MSSQL_DECIMAL":          writeStringNoQuotesMidTurbo,
	"MSSQL_INT":              writeIntMidTurbo,
	"MSSQL_MONEY":            writeQuotedStringMidTurbo,
	"MSSQL_SMALLINT":         writeIntMidTurbo,
	"MSSQL_SMALLMONEY":       writeQuotedStringMidTurbo,
	"MSSQL_TINYINT":          writeIntMidTurbo,
	"MSSQL_FLOAT":            writeFloatMidTurbo,
	"MSSQL_REAL":             writeFloatMidTurbo,
	"MSSQL_DATE":             snowflakeWriteTimestampFromTimeMidTurbo,
	"MSSQL_DATETIME2":        snowflakeWriteTimestampFromTimeMidTurbo,
	"MSSQL_DATETIME":         snowflakeWriteTimestampFromTimeMidTurbo,
	"MSSQL_DATETIMEOFFSET":   snowflakeWriteTimestampFromTimeMidTurbo,
	"MSSQL_SMALLDATETIME":    snowflakeWriteTimestampFromTimeMidTurbo,
	"MSSQL_TIME":             snowflakeWriteTimestampFromTimeMidTurbo,
	"MSSQL_CHAR":             writeEscapedQuotedStringMidTurbo,
	"MSSQL_VARCHAR":          writeEscapedQuotedStringMidTurbo,
	"MSSQL_TEXT":             writeEscapedQuotedStringMidTurbo,
	"MSSQL_NCHAR":            writeEscapedQuotedStringMidTurbo,
	"MSSQL_NVARCHAR":         writeEscapedQuotedStringMidTurbo,
	"MSSQL_NTEXT":            writeEscapedQuotedStringMidTurbo,
	"MSSQL_BINARY":           snowflakeWriteBinaryfromBytesMidTurbo,
	"MSSQL_VARBINARY":        snowflakeWriteBinaryfromBytesMidTurbo,
	"MSSQL_UNIQUEIDENTIFIER": writeMSSQLUniqueIdentifierMidTurbo,
	"MSSQL_XML":              writeEscapedQuotedStringMidTurbo,

	// // Oracle

	"Oracle_CHAR":           writeEscapedQuotedStringMidTurbo,
	"Oracle_NCHAR":          writeEscapedQuotedStringMidTurbo,
	"Oracle_OCIClobLocator": writeEscapedQuotedStringMidTurbo,
	"Oracle_OCIBlobLocator": snowflakeWriteBinaryfromBytesMidTurbo,
	"Oracle_LONG":           writeEscapedQuotedStringMidTurbo,
	"Oracle_NUMBER":         oracleWriteNumberMidTurbo,
	"Oracle_DATE":           snowflakeWriteTimestampFromTimeMidTurbo,
	"Oracle_TimeStampDTY":   snowflakeWriteTimestampFromTimeMidTurbo,

	// // Redshift

	"Redshift_BIGINT":      writeIntMidTurbo,
	"Redshift_BOOLEAN":     writeBoolMidTurbo,
	"Redshift_CHAR":        writeEscapedQuotedStringMidTurbo,
	"Redshift_BPCHAR":      writeEscapedQuotedStringMidTurbo,
	"Redshift_VARCHAR":     writeEscapedQuotedStringMidTurbo,
	"Redshift_DATE":        snowflakeWriteTimestampFromTimeMidTurbo,
	"Redshift_DOUBLE":      writeFloatMidTurbo,
	"Redshift_INT":         writeIntMidTurbo,
	"Redshift_NUMERIC":     writeStringNoQuotesMidTurbo,
	"Redshift_REAL":        writeFloatMidTurbo,
	"Redshift_SMALLINT":    writeIntMidTurbo,
	"Redshift_TIME":        writeStringNoQuotesMidTurbo,
	"Redshift_TIMETZ":      writeStringNoQuotesMidTurbo,
	"Redshift_TIMESTAMP":   snowflakeWriteTimestampFromTimeMidTurbo,
	"Redshift_TIMESTAMPTZ": snowflakeWriteTimestampFromTimeMidTurbo,
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

	"Snowflake_NUMBER":        writeStringNoQuotesEndTurbo,
	"Snowflake_REAL":          writeStringNoQuotesEndTurbo,
	"Snowflake_TEXT":          writeEscapedQuotedStringEndTurbo,
	"Snowflake_BOOLEAN":       writeStringNoQuotesEndTurbo,
	"Snowflake_DATE":          snowflakeWriteTimestampFromTimeEndTurbo,
	"Snowflake_TIME":          snowflakeWriteTimeFromTimeEndTurbo,
	"Snowflake_TIMESTAMP_LTZ": snowflakeWriteTimestampFromTimeEndTurbo,
	"Snowflake_TIMESTAMP_NTZ": snowflakeWriteTimestampFromTimeEndTurbo,
	"Snowflake_TIMESTAMP_TZ":  snowflakeWriteTimestampFromTimeEndTurbo,
	"Snowflake_VARIANT":       writeEscapedQuotedStringEndTurbo,
	"Snowflake_OBJECT":        writeEscapedQuotedStringEndTurbo,
	"Snowflake_ARRAY":         writeEscapedQuotedStringEndTurbo,
	"Snowflake_BINARY":        snowflakeWriteBinaryfromBytesEndTurbo,

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

	"MySQL_BIT":       snowflakeWriteBinaryfromBytesEndTurbo,
	"MySQL_TINYINT":   writeStringNoQuotesEndTurbo,
	"MySQL_SMALLINT":  writeStringNoQuotesEndTurbo,
	"MySQL_MEDIUMINT": writeStringNoQuotesEndTurbo,
	"MySQL_INT":       writeStringNoQuotesEndTurbo,
	"MySQL_BIGINT":    writeStringNoQuotesEndTurbo,
	"MySQL_DECIMAL":   writeStringNoQuotesEndTurbo,
	"MySQL_FLOAT4":    writeStringNoQuotesEndTurbo,
	"MySQL_FLOAT8":    writeStringNoQuotesEndTurbo,
	"MySQL_DATE":      writeStringNoQuotesEndTurbo,
	"MySQL_TIME":      writeStringNoQuotesEndTurbo,
	"MySQL_TIMESTAMP": writeStringNoQuotesEndTurbo,
	"MySQL_DATETIME":  writeStringNoQuotesEndTurbo,
	"MySQL_YEAR":      writeStringNoQuotesEndTurbo,
	"MySQL_CHAR":      writeEscapedQuotedStringEndTurbo,
	"MySQL_VARCHAR":   writeEscapedQuotedStringEndTurbo,
	"MySQL_TEXT":      writeEscapedQuotedStringEndTurbo,
	"MySQL_BINARY":    snowflakeWriteBinaryfromBytesEndTurbo,
	"MySQL_VARBINARY": snowflakeWriteBinaryfromBytesEndTurbo,
	"MySQL_BLOB":      snowflakeWriteBinaryfromBytesEndTurbo,
	"MySQL_GEOMETRY":  snowflakeWriteBinaryfromBytesEndTurbo,
	"MySQL_JSON":      writeEscapedQuotedStringEndTurbo,

	// MSSQL

	"MSSQL_BIGINT":           writeIntEndTurbo,
	"MSSQL_BIT":              writeBoolEndTurbo,
	"MSSQL_DECIMAL":          writeStringNoQuotesEndTurbo,
	"MSSQL_INT":              writeIntEndTurbo,
	"MSSQL_MONEY":            writeQuotedStringEndTurbo,
	"MSSQL_SMALLINT":         writeIntEndTurbo,
	"MSSQL_SMALLMONEY":       writeQuotedStringEndTurbo,
	"MSSQL_TINYINT":          writeIntEndTurbo,
	"MSSQL_FLOAT":            writeFloatEndTurbo,
	"MSSQL_REAL":             writeFloatEndTurbo,
	"MSSQL_DATE":             snowflakeWriteTimestampFromTimeEndTurbo,
	"MSSQL_DATETIME2":        snowflakeWriteTimestampFromTimeEndTurbo,
	"MSSQL_DATETIME":         snowflakeWriteTimestampFromTimeEndTurbo,
	"MSSQL_DATETIMEOFFSET":   snowflakeWriteTimestampFromTimeEndTurbo,
	"MSSQL_SMALLDATETIME":    snowflakeWriteTimestampFromTimeEndTurbo,
	"MSSQL_TIME":             snowflakeWriteTimestampFromTimeEndTurbo,
	"MSSQL_CHAR":             writeEscapedQuotedStringEndTurbo,
	"MSSQL_VARCHAR":          writeEscapedQuotedStringEndTurbo,
	"MSSQL_TEXT":             writeEscapedQuotedStringEndTurbo,
	"MSSQL_NCHAR":            writeEscapedQuotedStringEndTurbo,
	"MSSQL_NVARCHAR":         writeEscapedQuotedStringEndTurbo,
	"MSSQL_NTEXT":            writeEscapedQuotedStringEndTurbo,
	"MSSQL_BINARY":           snowflakeWriteBinaryfromBytesEndTurbo,
	"MSSQL_VARBINARY":        snowflakeWriteBinaryfromBytesEndTurbo,
	"MSSQL_UNIQUEIDENTIFIER": writeMSSQLUniqueIdentifierEndTurbo,
	"MSSQL_XML":              writeEscapedQuotedStringEndTurbo,

	// // Oracle

	"Oracle_CHAR":           writeEscapedQuotedStringEndTurbo,
	"Oracle_NCHAR":          writeEscapedQuotedStringEndTurbo,
	"Oracle_OCIClobLocator": writeEscapedQuotedStringEndTurbo,
	"Oracle_OCIBlobLocator": snowflakeWriteBinaryfromBytesEndTurbo,
	"Oracle_LONG":           writeEscapedQuotedStringEndTurbo,
	"Oracle_NUMBER":         oracleWriteNumberEndTurbo,
	"Oracle_DATE":           snowflakeWriteTimestampFromTimeEndTurbo,
	"Oracle_TimeStampDTY":   snowflakeWriteTimestampFromTimeEndTurbo,

	// // Redshift

	"Redshift_BIGINT":      writeIntEndTurbo,
	"Redshift_BOOLEAN":     writeBoolEndTurbo,
	"Redshift_CHAR":        writeEscapedQuotedStringEndTurbo,
	"Redshift_BPCHAR":      writeEscapedQuotedStringEndTurbo,
	"Redshift_VARCHAR":     writeEscapedQuotedStringEndTurbo,
	"Redshift_DATE":        snowflakeWriteTimestampFromTimeEndTurbo,
	"Redshift_DOUBLE":      writeFloatEndTurbo,
	"Redshift_INT":         writeIntEndTurbo,
	"Redshift_NUMERIC":     writeStringNoQuotesEndTurbo,
	"Redshift_REAL":        writeFloatEndTurbo,
	"Redshift_SMALLINT":    writeIntEndTurbo,
	"Redshift_TIME":        writeStringNoQuotesEndTurbo,
	"Redshift_TIMETZ":      writeStringNoQuotesEndTurbo,
	"Redshift_TIMESTAMP":   snowflakeWriteTimestampFromTimeEndTurbo,
	"Redshift_TIMESTAMPTZ": snowflakeWriteTimestampFromTimeEndTurbo,
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
	// Syncs
	"PostgreSQL_BIGINT_SYNC":      writeInsertRawStringNoQuotes,
	"PostgreSQL_BOOL_SYNC":        writeInsertStringNoEscape,
	"PostgreSQL_DATE_SYNC":        writeInsertStringNoEscape,
	"PostgreSQL_DOUBLE_SYNC":      writeInsertRawStringNoQuotes,
	"PostgreSQL_INT_SYNC":         writeInsertRawStringNoQuotes,
	"PostgreSQL_FLOAT_SYNC":       writeInsertRawStringNoQuotes,
	"PostgreSQL_SMALLINT_SYNC":    writeInsertRawStringNoQuotes,
	"PostgreSQL_TIMESTAMP_SYNC":   writeInsertStringNoEscape,
	"PostgreSQL_TIMESTAMPTZ_SYNC": writeInsertStringNoEscape,
	"NIL":                         postgresqlWriteNone,

	// MYSQL

	"MySQL_BIT":       snowflakeWriteBinaryfromBytes,
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
	"MySQL_BINARY":    snowflakeWriteBinaryfromBytes,
	"MySQL_VARBINARY": snowflakeWriteBinaryfromBytes,
	"MySQL_BLOB":      snowflakeWriteBinaryfromBytes,
	"MySQL_GEOMETRY":  snowflakeWriteBinaryfromBytes,
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
	"MSSQL_DATE":             snowflakeWriteTimestampFromTime,
	"MSSQL_DATETIME2":        snowflakeWriteTimestampFromTime,
	"MSSQL_DATETIME":         snowflakeWriteTimestampFromTime,
	"MSSQL_DATETIMEOFFSET":   snowflakeWriteTimestampFromTime,
	"MSSQL_SMALLDATETIME":    snowflakeWriteTimestampFromTime,
	"MSSQL_TIME":             snowflakeWriteTimestampFromTime,
	"MSSQL_CHAR":             writeInsertEscapedString,
	"MSSQL_VARCHAR":          writeInsertEscapedString,
	"MSSQL_TEXT":             writeInsertEscapedString,
	"MSSQL_NCHAR":            writeInsertEscapedString,
	"MSSQL_NVARCHAR":         writeInsertEscapedString,
	"MSSQL_NTEXT":            writeInsertEscapedString,
	"MSSQL_BINARY":           snowflakeWriteBinaryfromBytes,
	"MSSQL_VARBINARY":        snowflakeWriteBinaryfromBytes,
	"MSSQL_UNIQUEIDENTIFIER": writeMSSQLUniqueIdentifier,
	"MSSQL_XML":              writeInsertEscapedString,

	// Oracle

	"Oracle_CHAR":           writeInsertEscapedString,
	"Oracle_NCHAR":          writeInsertEscapedString,
	"Oracle_OCIClobLocator": writeInsertEscapedString,
	"Oracle_OCIBlobLocator": snowflakeWriteBinaryfromBytes,
	"Oracle_LONG":           writeInsertEscapedString,
	"Oracle_NUMBER":         oracleWriteNumber,
	"Oracle_DATE":           snowflakeWriteTimestampFromTime,
	"Oracle_TimeStampDTY":   snowflakeWriteTimestampFromTime,

	// Redshift

	"Redshift_BIGINT":      writeInsertInt,
	"Redshift_BOOLEAN":     writeInsertBool,
	"Redshift_CHAR":        writeInsertEscapedString,
	"Redshift_BPCHAR":      writeInsertEscapedString,
	"Redshift_VARCHAR":     writeInsertEscapedString,
	"Redshift_DATE":        snowflakeWriteTimestampFromTime,
	"Redshift_DOUBLE":      writeInsertFloat,
	"Redshift_INT":         writeInsertInt,
	"Redshift_NUMERIC":     writeInsertRawStringNoQuotes,
	"Redshift_REAL":        writeInsertFloat,
	"Redshift_SMALLINT":    writeInsertInt,
	"Redshift_TIME":        writeInsertStringNoEscape,
	"Redshift_TIMETZ":      writeInsertStringNoEscape,
	"Redshift_TIMESTAMP":   snowflakeWriteTimestampFromTime,
	"Redshift_TIMESTAMPTZ": snowflakeWriteTimestampFromTime,
}
