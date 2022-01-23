package engine

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/pkg"
)

type DsConnection interface {

	// Gets information needed to connect to the DB represented by the DsConnection
	getConnectionInfo() (dsType string, driverName string, connString string)

	// Returns true if the result set is getting close to the DBs insertion size
	insertChecker(currentLen int, currentRow int) (limitReached bool)

	// Runs "delete from <transfer.TargetTable>"
	deleteFromTable(transfer data.Transfer) (errProperties map[string]string, err error)

	// Drops <transfer.TargetTable>
	dropTable(transferInfo data.Transfer) (errProperties map[string]string, err error)

	// Creates a table to match the result set of <transfer.Query>
	createTable(transfer data.Transfer, columnInfo ResultSetColumnInfo) (errProperties map[string]string, err error)

	// Translates a single value from the source into an intermediate type,
	// which will then be translated by one of the writer functions below
	getIntermediateType(colTypeFromDriver string) (intermediateType string, errProperties map[string]string, err error)

	// Takes a value, converts it into the proper format
	// for insertion into a specific DB type, and returns the value
	getValToWriteMidRow(valType string, value interface{}) (valToWrite string)

	// Takes a value, converts it into the proper format
	// for insertion into a specific DB type, and returns the value
	getValToWriteRowEnd(valType string, value interface{}) (valToWrite string)

	// Takes a value, converts it into the proper format
	// for insertion into a specific DB type, and returns the value
	getValToWriteRaw(valType string, value interface{}) (valToWrite string)

	// Takes a value, converts it into the proper format
	// for insertion into a specific DB type, and writes it to a string builder
	turboWriteMidVal(valType string, value interface{}, builder *strings.Builder) (errProperties map[string]string, err error)

	// Takes a value, converts it into the proper format
	// for insertion into a specific DB type, and writes it to a string builder
	turboWriteEndVal(valType string, value interface{}, builder *strings.Builder) (errProperties map[string]string, err error)

	// Generates the first few characters of a row for
	// a given insertion query for a given data system type
	getRowStarter() (rowStarted string)

	// Generates the start of an insertion query for a given data system type
	getQueryStarter(targetTable string, columnInfo ResultSetColumnInfo) (queryStarter string)

	// Generates the end of an insertion query for a given data system type
	getQueryEnder(targetTable string) (queryEnder string)

	// Returns the data system type, and a debug conn string, which has
	// the username and password scrubbed
	GetDebugInfo() (dsType string, debugConnString string)

	// Gets the result of a given query, which will later be inserted
	getRows(transferInfo data.Transfer) (resultSet *sql.Rows, resultSetColumnInfo ResultSetColumnInfo, errProperties map[string]string, err error)

	// Gets the result of a given query, which will not be inserted
	getFormattedResults(query string) (resultSet QueryResult, errProperties map[string]string, err error)

	// Takes result set info, and returns a list of types to create a new table in another DB based
	// on those types.
	getCreateTableType(resultSetColInfo ResultSetColumnInfo, colNum int) (createTableTypes string)

	// Runs a turbo transfer
	turboTransfer(rows *sql.Rows, transferInfo data.Transfer, resultSetColumnInfo ResultSetColumnInfo) (errProperties map[string]string, err error)
}

func TestConnection(
	connection *data.Connection,
) (
	*data.Connection,
	map[string]string,
	error,
) {
	dsConn, errProperties, err := GetDs(*connection)
	if err != nil {
		return connection, errProperties, err
	}

	_, driverName, connString := dsConn.getConnectionInfo()

	db, err := sql.Open(driverName, connString)

	if err != nil {
		return connection, errProperties, err
	}

	if err := db.Ping(); err != nil {
		return connection, errProperties, err
	}

	connection.CanConnect = true

	return connection, errProperties, err
}

func TestConnections(connections []*data.Connection) (
	[]*data.Connection,
	map[string]string,
	error,
) {

	var wg sync.WaitGroup
	var errProperties map[string]string
	var err error
	numConns := len(connections)

	for i := 0; i < numConns; i++ {
		connection := connections[i]
		wg.Add(1)
		pkg.Background(func() {
			defer wg.Done()
			connection, errProperties, err = TestConnection(connection)
		})
	}
	wg.Wait()

	return connections, errProperties, err
}

func GetDs(connection data.Connection) (
	dsConn DsConnection,
	errProperties map[string]string,
	err error,
) {

	switch connection.DsType {
	case "postgresql":
		dsConn, errProperties, err = getNewPostgreSQL(connection)
	case "mysql":
		dsConn, errProperties, err = getNewMySQL(connection)
	case "mssql":
		dsConn, errProperties, err = getNewMSSQL(connection)
	// case "redshift":
	// 	dsConn = getNewRedshift(connection)
	// case "snowflake":
	// 	dsConn = getNewSnowflake(connection)
	// case "oracle":
	// 	dsConn = getNewOracle(connection)
	default:
		panic("Unknown DsType")
	}

	return dsConn, errProperties, err
}

func RunTransfer(
	transfer *data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {

	sourceConnection := transfer.Source

	targetConnection := transfer.Target

	sourceSystem, errProperties, err := GetDs(sourceConnection)
	if err != nil {
		return errProperties, err
	}

	rows, resultSetColumnInfo, errProperties, err := sourceSystem.getRows(*transfer)
	if err != nil {
		return errProperties, err
	}
	defer rows.Close()

	targetSystem, errProperties, err := GetDs(targetConnection)
	if err != nil {
		return errProperties, err
	}
	errProperties, err = Insert(targetSystem, rows, *transfer, resultSetColumnInfo)

	return errProperties, err
}

func RunQuery(query *data.Query) (
	errProperties map[string]string,
	err error,
) {
	dsConn, errProperties, err := GetDs(query.Connection)
	if err != nil {
		return errProperties, err
	}
	_, errProperties, err = execute(dsConn, query.Query)
	return errProperties, err
}

func standardGetRows(
	dsConn DsConnection,
	transferInfo data.Transfer,
) (
	rows *sql.Rows,
	resultSetColumnInfo ResultSetColumnInfo,
	errProperties map[string]string,
	err error,
) {

	rows, errProperties, err = execute(dsConn, transferInfo.Query)
	if err != nil {
		return rows, resultSetColumnInfo, errProperties, err
	}
	resultSetColumnInfo, errProperties, err = getResultSetColumnInfo(dsConn, rows)
	return rows, resultSetColumnInfo, errProperties, err
}

func sqlInsert(
	dsConn DsConnection,
	rows *sql.Rows,
	transfer data.Transfer,
	resultSetColumnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	numCols := resultSetColumnInfo.NumCols
	zeroIndexedNumCols := numCols - 1
	targetTable := transfer.TargetTable
	colTypes := resultSetColumnInfo.ColumnIntermediateTypes

	var wg sync.WaitGroup
	var queryBuilder strings.Builder

	values := make([]interface{}, numCols)
	valuePtrs := make([]interface{}, numCols)
	isFirst := true

	// set the pointer in valueptrs to corresponding values
	for i := 0; i < numCols; i++ {
		valuePtrs[i] = &values[i]
	}

	var insertError error

	for i := 1; rows.Next(); i++ {

		// scan incoming values into valueptrs, which in turn points to values
		rows.Scan(valuePtrs...)

		if isFirst {
			queryBuilder.WriteString(dsConn.getQueryStarter(targetTable, resultSetColumnInfo))
			isFirst = false
		} else {
			queryBuilder.WriteString(dsConn.getRowStarter())
		}

		// while in the middle of insert row, add commas at end of values
		for j := 0; j < zeroIndexedNumCols; j++ {
			queryBuilder.WriteString(dsConn.getValToWriteMidRow(colTypes[j], values[j]))
		}

		// end of row doesn't need a comma at the end
		queryBuilder.WriteString(dsConn.getValToWriteRowEnd(colTypes[zeroIndexedNumCols], values[zeroIndexedNumCols]))

		// each dsConn has its own limits on insert statements (either on total
		// length or number of rows)
		if dsConn.insertChecker(queryBuilder.Len(), i) {
			noUnionAll := strings.TrimSuffix(queryBuilder.String(), " UNION ALL ")
			withQueryEnder := fmt.Sprintf("%s%s", noUnionAll, dsConn.getQueryEnder(targetTable))
			queryString := sqlEndStringNilReplacer.Replace(withQueryEnder)
			wg.Wait()
			if insertError != nil {
				return errProperties, insertError
			}
			wg.Add(1)
			pkg.Background(func() {
				defer wg.Done()
				_, errProperties, insertError = execute(dsConn, queryString)
			})
			isFirst = true
			queryBuilder.Reset()
		}

	}
	// if we still have some leftovers, add those too.
	if !isFirst {
		noUnionAll := strings.TrimSuffix(queryBuilder.String(), " UNION ALL ")
		withQueryEnder := fmt.Sprintf("%s%s", noUnionAll, dsConn.getQueryEnder(targetTable))
		queryString := sqlEndStringNilReplacer.Replace(withQueryEnder)
		wg.Wait()
		if insertError != nil {
			return errProperties, insertError
		}
		wg.Add(1)
		pkg.Background(func() {
			defer wg.Done()
			_, errProperties, insertError = execute(dsConn, queryString)
		})
	}
	wg.Wait()

	return nil, nil
}

func turboTransfer(
	dsConn DsConnection,
	rows *sql.Rows,
	transferInfo data.Transfer,
	resultSetColumnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	return dsConn.turboTransfer(rows, transferInfo, resultSetColumnInfo)
}

func Insert(
	dsConn DsConnection,
	rows *sql.Rows,
	transfer data.Transfer,
	resultSetColumnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {

	if transfer.Overwrite {
		errProperties, err = dsConn.dropTable(transfer)
		if err != nil {
			return errProperties, err
		}
		errProperties, err = dsConn.createTable(transfer, resultSetColumnInfo)
		if err != nil {
			return errProperties, err
		}
	}

	return sqlInsert(dsConn, rows, transfer, resultSetColumnInfo)
}

func standardGetFormattedResults(
	dsConn DsConnection,
	query string,
) (
	queryResult QueryResult,
	errProperties map[string]string,
	err error,
) {

	rows, errProperties, err := execute(dsConn, query)
	if err != nil {
		return queryResult, errProperties, err
	}

	resultSetColumnInfo, errProperties, err := getResultSetColumnInfo(dsConn, rows)

	queryResult.ColumnTypes = map[string]string{}
	queryResult.Rows = []interface{}{}
	for i, colType := range resultSetColumnInfo.ColumnDbTypes {
		queryResult.ColumnTypes[resultSetColumnInfo.ColumnNames[i]] = colType
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

// This is the base level function, where queries actually get executed.
func execute(
	dsConn DsConnection,
	query string,
) (rows *sql.Rows, errProperties map[string]string, err error) {

	_, driverName, connString := dsConn.getConnectionInfo()
	dsType, debugConnString := dsConn.GetDebugInfo()

	db, err := sql.Open(driverName, connString)
	if err != nil {
		msg := errors.New("error while running sql.Open()")
		errProperties := map[string]string{
			"debugConnString": debugConnString,
			"error":           err.Error(),
		}
		return rows, errProperties, msg
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		msg := errors.New("error while running db.Ping()")
		errProperties := map[string]string{
			"debugConnString": debugConnString,
			"error":           err.Error(),
		}
		return rows, errProperties, msg
	}

	rows, err = db.Query(query)
	if err != nil {
		if len(query) > 1000 {
			query = fmt.Sprintf("%v ... (Rest of query truncated)", query[:1000])
		}

		msg := errors.New("db.Query() threw an error")
		errProperties := map[string]string{
			"error":  err.Error(),
			"dsType": dsType,
			"query":  query,
		}

		return rows, errProperties, msg
	}

	return rows, nil, nil
}

func getResultSetColumnInfo(
	dsConn DsConnection,
	rows *sql.Rows,
) (
	resultSetColumnInfo ResultSetColumnInfo,
	errProperties map[string]string,
	err error,
) {

	var colNames []string
	var colTypes []string
	var scanTypes []reflect.Type
	var intermediateTypes []string
	var colLens []int64
	var lenOks []bool
	var precisions []int64
	var scales []int64
	var precisionScaleOks []bool
	var nullables []bool
	var nullableOks []bool
	var colNamesToTypes = map[string]string{}

	colTypesFromDriver, err := rows.ColumnTypes()
	if err != nil {
		return resultSetColumnInfo, errProperties, err
	}

	for _, colType := range colTypesFromDriver {
		colNames = append(colNames, colType.Name())
		colTypes = append(colTypes, colType.DatabaseTypeName())
		scanTypes = append(scanTypes, colType.ScanType())

		intermediateType, errProperties, err := dsConn.getIntermediateType(colType.DatabaseTypeName())
		if err != nil {
			return resultSetColumnInfo, errProperties, err
		}
		intermediateTypes = append(intermediateTypes, intermediateType)

		colLen, lenOk := colType.Length()
		colLens = append(colLens, colLen)
		lenOks = append(lenOks, lenOk)

		precision, scale, precisionScaleOk := colType.DecimalSize()
		precisions = append(precisions, precision)
		scales = append(scales, scale)
		precisionScaleOks = append(precisionScaleOks, precisionScaleOk)

		nullable, nullableOk := colType.Nullable()
		nullables = append(nullables, nullable)
		nullableOks = append(nullableOks, nullableOk)
		colNamesToTypes[colType.Name()] = colType.DatabaseTypeName()
	}

	columnInfo := ResultSetColumnInfo{
		ColumnNames:             colNames,
		ColumnDbTypes:           colTypes,
		ColumnScanTypes:         scanTypes,
		ColumnIntermediateTypes: intermediateTypes,
		ColumnLengths:           colLens,
		LengthOks:               lenOks,
		ColumnPrecisions:        precisions,
		ColumnScales:            scales,
		PrecisionScaleOks:       precisionScaleOks,
		ColumnNullables:         nullables,
		NullableOks:             nullableOks,
		NumCols:                 len(colNames),
	}

	return columnInfo, errProperties, err
}

func standardGetRowStarter() string {
	return ",("
}

func standardGetQueryStarter(targetTable string, columnInfo ResultSetColumnInfo) string {
	return fmt.Sprintf("INSERT INTO %s ("+strings.Join(columnInfo.ColumnNames, ", ")+") VALUES (", targetTable)
}

func dropTableIfExistsWithSchema(
	dsConn DsConnection,
	transferInfo data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {
	dropQuery := fmt.Sprintf(
		"DROP TABLE IF EXISTS %v.%v",
		transferInfo.TargetSchema,
		transferInfo.TargetTable,
	)

	_, errProperties, err = execute(dsConn, dropQuery)

	return errProperties, err
}

func dropTableIfExistsNoSchema(dsConn DsConnection, transferInfo data.Transfer) (errProperties map[string]string, err error) {
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %v", transferInfo.TargetTable)
	_, errProperties, err = execute(dsConn, dropQuery)
	return errProperties, err
}

func dropTableNoSchema(dsConn DsConnection, transferInfo data.Transfer) (errProperties map[string]string, err error) {
	dropQuery := fmt.Sprintf("DROP TABLE %v", transferInfo.TargetTable)
	_, errProperties, err = execute(dsConn, dropQuery)
	return errProperties, err
}

func deleteFromTableWithSchema(
	dsConn DsConnection,
	transferInfo data.Transfer,
) (
	errProperties map[string]string,
	err error,
) {

	query := fmt.Sprintf(
		"DELETE FROM %v.%v",
		transferInfo.TargetSchema,
		transferInfo.TargetTable,
	)

	_, errProperties, err = execute(dsConn, query)

	return errProperties, err
}

func deleteFromTableNoSchema(dsConn DsConnection, transferInfo data.Transfer) (
	errProperties map[string]string,
	err error,
) {
	query := fmt.Sprintf(
		"DELETE FROM %v",
		transferInfo.TargetTable,
	)
	_, errProperties, err = execute(dsConn, query)

	return errProperties, err
}

func standardCreateTable(
	dsConn DsConnection,
	transferInfo data.Transfer,
	columnInfo ResultSetColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	var queryBuilder strings.Builder
	errProperties = map[string]string{}

	if transferInfo.TargetSchema == "" {
		_, err = fmt.Fprintf(
			&queryBuilder,
			"CREATE TABLE %v (", transferInfo.TargetTable,
		)
	} else {
		_, err = fmt.Fprintf(
			&queryBuilder,
			"CREATE TABLE %v.%v (", transferInfo.TargetSchema, transferInfo.TargetTable,
		)
	}

	if err != nil {
		errProperties["error"] = err.Error()
		return errProperties, errors.New("error building create table query")
	}

	var colNamesAndTypesSlice []string

	for i, colName := range columnInfo.ColumnNames {
		colType := dsConn.getCreateTableType(columnInfo, i)
		colNamesAndTypesSlice = append(colNamesAndTypesSlice, fmt.Sprintf("%v %v", colName, colType))
	}

	fmt.Fprintf(&queryBuilder, "%v)", strings.Join(colNamesAndTypesSlice, ", "))

	if err != nil {
		errProperties["error"] = err.Error()
		return errProperties, errors.New("error building create table query")
	}

	query := queryBuilder.String()

	_, errProperties, err = execute(dsConn, query)

	return errProperties, err
}
