package engine

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/jsonLog"
	"github.com/calmitchell617/sqlpipe/pkg"
)

var logger = jsonLog.New(os.Stdout, jsonLog.LevelInfo)

type DsConnection interface {

	// Gets information needed to connect to the DB represented by the DsConnection
	GetConnectionInfo() (dsType string, driverName string, connString string)

	// Returns true if the result set is getting close to the DBs insertion size
	insertChecker(currentLen int, currentRow int) (limitReached bool)

	// Runs "delete from <transfer.TargetTable>"
	deleteFromTable(transfer data.Transfer) (err error)

	// Drops <transfer.TargetTable>
	dropTable(transferInfo data.Transfer) (err error)

	// Creates a table to match the result set of <transfer.Query>
	createTable(transfer data.Transfer, columnInfo ResultSetColumnInfo) (err error)

	// Translates a single value from the source into an intermediate type,
	// which will then be translated by one of the writer functions below
	getIntermediateType(colTypeFromDriver string) (intermediateType string, err error)

	// Takes a value, converts it into the proper format
	// for insertion into a specific DB type, and returns the value
	getValToWriteMidRow(valType string, value interface{}) (valToWrite string)

	// Takes a value, converts it into the proper format
	// for insertion into a specific DB type, and returns the value
	getValToWriteRowEnd(valType string, value interface{}) (valToWrite string)

	// Takes a value, converts it into the proper format
	// for insertion into a specific DB type, and writes it to a string builder
	turboWriteMidVal(valType string, value interface{}, builder *strings.Builder) (err error)

	// Takes a value, converts it into the proper format
	// for insertion into a specific DB type, and writes it to a string builder
	turboWriteEndVal(valType string, value interface{}, builder *strings.Builder) (err error)

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
	getRows(transferInfo data.Transfer) (resultSet *sql.Rows, resultSetColumnInfo ResultSetColumnInfo, err error)

	// Gets the result of a given query, which will not be inserted
	getFormattedResults(query string) (resultSet QueryResult, err error)

	// Takes result set info, and returns a list of types to create a new table in another DB based
	// on those types.
	getCreateTableType(resultSetColInfo ResultSetColumnInfo, colNum int) (createTableTypes string)

	// Runs a turbo transfer
	turboTransfer(rows *sql.Rows, transferInfo data.Transfer, resultSetColumnInfo ResultSetColumnInfo) (err error)
}

type Work struct {
	Query     data.Query
	Transfer  data.Transfer
	CreatedAt time.Time
}

func GetDs(connection data.Connection) (dsConn DsConnection) {

	switch connection.DsType {
	case "postgresql":
		dsConn = getNewPostgreSQL(connection)
		// case "redshift":
		// 	dsConn = getNewRedshift(connection)
		// case "snowflake":
		// 	dsConn = getNewSnowflake(connection)
		// case "mysql":
		// 	dsConn = getNewMySQL(connection)
		// case "mssql":
		// 	dsConn = getNewMSSQL(connection)
		// case "oracle":
		// 	dsConn = getNewOracle(connection)
	}

	return dsConn
}

func RunTransfer(transfer *data.Transfer) error {

	sourceConnection := transfer.Source

	targetConnection := transfer.Target

	sourceSystem := GetDs(sourceConnection)

	rows, resultSetColumnInfo, err := sourceSystem.getRows(*transfer)
	if err != nil {
		return err
	}
	defer rows.Close()

	targetSystem := GetDs(targetConnection)
	err = Insert(targetSystem, rows, *transfer, resultSetColumnInfo)
	if err != nil {
		return err
	}

	return nil
}

// func RunQuery(query data.Query) (resultSet QueryResult, err error) {

// 	db := helpers.GetDb()
// 	query.Status = "In progress"
// 	db.Save(&query)

// 	var connection data.Connection
// 	db.First(&connection, query.ConnectionFk)

// 	dsConn := GetDs(connection)
// 	resultSet, err = dsConn.getFormattedResults(query.Query)

// 	if err != nil {
// 		query.Status = "Failed"
// 		query.StoppedAt = time.Now()
// 		query.Error = err.Error()
// 		db.Save(&query)
// 		return resultSet, err
// 	}

// 	query.Status = "Complete"
// 	query.StoppedAt = time.Now()
// 	db.Save(&query)
// 	return resultSet, err
// }

func standardGetRows(
	dsConn DsConnection,
	transferInfo data.Transfer,
) (
	rows *sql.Rows,
	resultSetColumnInfo ResultSetColumnInfo,
	err error,
) {

	rows, err = execute(dsConn, transferInfo.Query)
	if err != nil {
		return rows, resultSetColumnInfo, err
	}
	resultSetColumnInfo, err = getResultSetColumnInfo(dsConn, rows)
	if err != nil {
		return rows, resultSetColumnInfo, err
	}

	return rows, resultSetColumnInfo, err
}

// func GetRows(dsConn DsConnection, transferInfo data.Transfer) (*sql.Rows, ResultSetColumnInfo, error) {
// 	return dsConn.getRows(transferInfo)
// }

func sqlInsert(
	dsConn DsConnection,
	rows *sql.Rows,
	transfer data.Transfer,
	resultSetColumnInfo ResultSetColumnInfo,
) error {
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
				logger.PrintError(insertError, nil)
				return insertError
			}
			wg.Add(1)
			pkg.Background(func() {
				defer wg.Done()
				_, insertError = execute(dsConn, queryString)
			})
			fmt.Printf("**SQLPipe status update\nNow inserting at row %d of result set from query:\n%s\n\n", i, transfer.Query)
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
			logger.PrintError(insertError, nil)
			return insertError
		}
		wg.Add(1)
		pkg.Background(func() {
			defer wg.Done()
			_, insertError = execute(dsConn, queryString)
		})
	}
	wg.Wait()

	return nil
}

func turboTransfer(
	dsConn DsConnection,
	rows *sql.Rows,
	transferInfo data.Transfer,
	resultSetColumnInfo ResultSetColumnInfo,
) error {
	return dsConn.turboTransfer(rows, transferInfo, resultSetColumnInfo)
}

func Insert(
	dsConn DsConnection,
	rows *sql.Rows,
	transfer data.Transfer,
	resultSetColumnInfo ResultSetColumnInfo,
) (
	err error,
) {

	if transfer.Overwrite {
		err = dsConn.dropTable(transfer)
		if err != nil {
			return err
		}
		err = dsConn.createTable(transfer, resultSetColumnInfo)
		if err != nil {
			return err
		}
	}

	return sqlInsert(dsConn, rows, transfer, resultSetColumnInfo)
}

func standardGetFormattedResults(
	dsConn DsConnection,
	query string,
) (
	queryResult QueryResult,
	err error,
) {

	rows, err := execute(dsConn, query)
	if err != nil {
		return queryResult, err
	}

	resultSetColumnInfo, err := getResultSetColumnInfo(dsConn, rows)

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
		queryResult.Rows = append(
			queryResult.Rows,
			map[string]interface{}{},
		)

		for j := 0; j < numCols; j++ {
			queryResult.Rows = append(queryResult.Rows, dsConn.getValToWriteMidRow(colTypes[j], values[j]))
		}
	}

	return queryResult, err
}

// This is the base level function, where queries actually get executed.
func execute(
	dsConn DsConnection,
	query string,
) (rows *sql.Rows, err error) {

	_, driverName, connString := dsConn.GetConnectionInfo()
	dsType, debugConnString := dsConn.GetDebugInfo()

	db, err := sql.Open(driverName, connString)
	if err != nil {
		prettyError := fmt.Errorf(
			"sql.Open() threw the following error with connString %v:\n\n%v",
			debugConnString,
			err,
		)
		log.Println(prettyError)
		return rows, prettyError
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		prettyError := fmt.Errorf(
			"db.Ping() threw the following error with connString %v:\n\n%v",
			debugConnString,
			err,
		)
		log.Println(prettyError)
		return rows, prettyError
	}

	rows, err = db.Query(query)
	if err != nil {
		cutoff := pkg.Min(len(query), 1000)

		var truncatedMessage string
		if cutoff == 1000 {
			truncatedMessage = " ... (rest of query truncated)"
		}

		prettyError := fmt.Errorf(
			"db.Query() threw the following error:\n\n%v\n\nWhile running query on DsType %v:\n\n%v%v",
			err,
			dsType,
			query[:cutoff],
			truncatedMessage,
		)
		log.Println(prettyError)
		return rows, prettyError
	}

	return rows, err
}

// Used for async inserts.
// func executeAsync(
// 	dsConn DsConnection,
// 	query string,
// 	wg *sync.WaitGroup,
// 	transfer data.Transfer,
// ) {
// 	defer wg.Done()

// 	_, driverName, connString := dsConn.GetConnectionInfo()
// 	dsType, debugConnString := dsConn.GetDebugInfo()

// 	db, err := sql.Open(driverName, connString)
// 	if err != nil {
// 		backendDb := helpers.GetDb()
// 		transfer.Error = err.Error()
// 		transfer.StoppedAt = time.Now()
// 		transfer.Status = "Failed"
// 		backendDb.Save(&transfer)

// 		prettyError := fmt.Errorf(
// 			"async sql.Open() threw the following error with connString %v:\n\n%v",
// 			debugConnString,
// 			err,
// 		)
// 		log.Println(prettyError)
// 	}
// 	defer db.Close()

// 	err = db.Ping()
// 	if err != nil {
// 		backendDb := helpers.GetDb()
// 		transfer.Error = err.Error()
// 		transfer.StoppedAt = time.Now()
// 		transfer.Status = "Failed"
// 		backendDb.Save(&transfer)
// 		prettyError := fmt.Errorf(
// 			"async db.Ping() threw the following error with connString %v:\n\n%v",
// 			debugConnString,
// 			err,
// 		)
// 		log.Println(prettyError)
// 	}

// 	_, err = db.Query(query)
// 	if err != nil {
// 		backendDb := helpers.GetDb()
// 		transfer.Error = err.Error()
// 		transfer.StoppedAt = time.Now()
// 		transfer.Status = "Failed"
// 		backendDb.Save(&transfer)
// 		cutoff := helpers.Min(len(query), 1000)

// 		var truncatedMessage string
// 		if cutoff == 1000 {
// 			truncatedMessage = " ... (rest of query truncated)"
// 		}

// 		prettyError := fmt.Errorf(
// 			"async db.Query() threw the following error:\n\n%v\n\nWhile running query on DsType %v:\n\n%v%v",
// 			err,
// 			dsType,
// 			query[:cutoff],
// 			truncatedMessage,
// 		)
// 		log.Println(prettyError)
// 	}
// }

func getResultSetColumnInfo(
	dsConn DsConnection,
	rows *sql.Rows,
) (
	resultSetColumnInfo ResultSetColumnInfo,
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
		return resultSetColumnInfo, err
	}

	for _, colType := range colTypesFromDriver {
		colNames = append(colNames, colType.Name())
		colTypes = append(colTypes, colType.DatabaseTypeName())
		scanTypes = append(scanTypes, colType.ScanType())

		intermediateType, err := dsConn.getIntermediateType(colType.DatabaseTypeName())
		if err != nil {
			log.Println(err)
			return resultSetColumnInfo, err
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

	return columnInfo, err
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
	err error,
) {
	dropQuery := fmt.Sprintf(
		"DROP TABLE IF EXISTS %v.%v",
		transferInfo.TargetSchema,
		transferInfo.TargetTable,
	)

	_, err = execute(dsConn, dropQuery)

	return err
}

func dropTableIfExistsNoSchema(dsConn DsConnection, transferInfo data.Transfer) {
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %v", transferInfo.TargetTable)
	execute(dsConn, dropQuery)
}

func dropTableNoSchema(dsConn DsConnection, transferInfo data.Transfer) {
	dropQuery := fmt.Sprintf("DROP TABLE %v", transferInfo.TargetTable)
	execute(dsConn, dropQuery)
}

func deleteFromTableWithSchema(
	dsConn DsConnection,
	transferInfo data.Transfer,
) (
	err error,
) {

	query := fmt.Sprintf(
		"DELETE FROM %v.%v",
		transferInfo.TargetSchema,
		transferInfo.TargetTable,
	)

	_, err = execute(dsConn, query)

	return err
}

func deleteFromTableNoSchema(dsConn DsConnection, transferInfo data.Transfer) {
	query := fmt.Sprintf(
		"DELETE FROM %v",
		transferInfo.TargetTable,
	)
	execute(dsConn, query)
}

func standardCreateTable(
	dsConn DsConnection,
	transferInfo data.Transfer,
	columnInfo ResultSetColumnInfo,
) (
	err error,
) {
	var queryBuilder strings.Builder

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
		prettyError := fmt.Errorf(
			"following error encountered while building a create table string:\n\n%s",
			err,
		)
		log.Println(prettyError)
		return err
	}

	var colNamesAndTypesSlice []string

	for i, colName := range columnInfo.ColumnNames {
		colType := dsConn.getCreateTableType(columnInfo, i)
		colNamesAndTypesSlice = append(colNamesAndTypesSlice, fmt.Sprintf("%v %v", colName, colType))
	}

	fmt.Fprintf(&queryBuilder, "%v)", strings.Join(colNamesAndTypesSlice, ", "))

	if err != nil {
		prettyError := fmt.Errorf(
			"following error encountered while building a create table string:\n\n%s",
			err,
		)
		log.Println(prettyError)
		return err
	}

	query := queryBuilder.String()

	_, err = execute(dsConn, query)

	return err
}
