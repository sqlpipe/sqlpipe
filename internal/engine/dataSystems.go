package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/jsonLog"
	"github.com/calmitchell617/sqlpipe/pkg"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
)

type column struct {
	name    string
	isKey   bool
	colType uint32
	length  int
}

type relation struct {
	columns   []column
	name      string
	namespace string
}

type transaction struct {
	queries []string
}

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
	createTable(transfer data.Transfer, columnInfo RowsColumnInfo) (errProperties map[string]string, err error)

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
	turboWriteMidVal(valType string, value interface{}, builder *strings.Builder)

	// Takes a value, converts it into the proper format
	// for insertion into a specific DB type, and writes it to a string builder
	turboWriteEndVal(valType string, value interface{}, builder *strings.Builder)

	// Generates the first few characters of a row for
	// a given insertion query for a given data system type
	getRowStarter() (rowStarted string)

	// Generates the start of an insertion query for a given data system type
	getQueryStarter(targetTable string, columnInfo RowsColumnInfo) (queryStarter string)

	// Generates the end of an insertion query for a given data system type
	getQueryEnder(targetTable string) (queryEnder string)

	// Returns the data system type, and a debug conn string, which has
	// the username and password scrubbed
	GetDebugInfo() (dsType string, debugConnString string)

	// Gets the result of a given query, which will later be inserted
	getRows(transferInfo data.Transfer) (resultSet *sql.Rows, rowColumnInfo RowsColumnInfo, errProperties map[string]string, err error)

	// Gets the result of a given query, which will not be inserted
	getFormattedResults(query string) (resultSet QueryResult, errProperties map[string]string, err error)

	// Takes result set info, and returns a list of types to create a new table in another DB based
	// on those types.
	getCreateTableType(resultSetColInfo RowsColumnInfo, colNum int) (createTableTypes string)

	// Runs a turbo transfer
	turboTransfer(rows *sql.Rows, transferInfo data.Transfer, rowColumnInfo RowsColumnInfo) (errProperties map[string]string, err error)

	// Bottom level func where queries actually get run
	execute(query string) (rows *sql.Rows, errProperties map[string]string, err error)

	// Replication insert writer
	writeSyncInsert(row []string, relation relation, rowsColumnInfo RowsColumnInfo) (query string)

	closeDb()
}

func TestConnection(
	connection *data.Connection,
) (
	*data.Connection,
	map[string]string,
	error,
) {
	dsConn, errProperties, err := GetDs(*connection)
	defer dsConn.closeDb()
	if err != nil {
		return connection, errProperties, err
	}

	_, driverName, connString := dsConn.getConnectionInfo()

	db, err := sql.Open(driverName, connString)
	if err != nil {
		return connection, errProperties, err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		errProperties = map[string]string{
			"dsType":    connection.DsType,
			"host":      connection.Hostname,
			"port":      fmt.Sprint(connection.Port),
			"accountId": connection.AccountId,
			"username":  connection.Username,
			"dbName":    connection.DbName,
			"err":       fmt.Sprint(err),
		}
		err = errors.New("couldn't connect to DB")
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
	case "oracle":
		dsConn, errProperties, err = getNewOracle(connection)
	case "redshift":
		dsConn, errProperties, err = getNewRedshift(connection)
	case "snowflake":
		dsConn, errProperties, err = getNewSnowflake(connection)
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
	defer sourceSystem.closeDb()
	if err != nil {
		return errProperties, err
	}

	rows, rowColumnInfo, errProperties, err := sourceSystem.getRows(*transfer)
	if err != nil {
		return errProperties, err
	}

	targetSystem, errProperties, err := GetDs(targetConnection)
	defer targetSystem.closeDb()
	if err != nil {
		return errProperties, err
	}
	errProperties, err = Insert(targetSystem, rows, *transfer, rowColumnInfo)

	return errProperties, err
}

func RunQuery(query *data.Query) (
	errProperties map[string]string,
	err error,
) {
	dsConn, errProperties, err := GetDs(query.Connection)
	defer dsConn.closeDb()
	if err != nil {
		return errProperties, err
	}
	rows, errProperties, err := dsConn.execute(query.Query)
	if err != nil {
		return errProperties, err
	}
	defer rows.Close()
	return errProperties, err
}

func RunSync(sync *data.Sync) (
	errProperties map[string]string,
	err error,
) {

	sourceSystem, errProperties, err := GetDs(sync.Source)
	defer sourceSystem.closeDb()
	if err != nil {
		return errProperties, err
	}

	targetSystem, errProperties, err := GetDs(sync.Target)
	defer targetSystem.closeDb()
	if err != nil {
		return errProperties, err
	}

	rows, errProperties, err := sourceSystem.execute("show wal_level")
	if err != nil {
		return errProperties, err
	}

	for rows.Next() {
		var wal_level string
		rows.Scan(&wal_level)
		if wal_level != "logical" {
			err = errors.New(`wal_level must be set to "logical". to make this change, run "ALTER SYSTEM SET wal_level=logical;". alternatively, alter your config file. then, restart the DB. more info at https://www.postgresql.org/docs/9.6/runtime-config-wal.html`)
			errProperties = map[string]string{"error": "wal_level != logical"}
			return
		}
	}

	logger := jsonLog.New(os.Stdout, jsonLog.LevelInfo)
	_, _, sourceConnString := sourceSystem.getConnectionInfo()
	sourceReplicationConnString := sourceConnString + "?replication=database"

	sourceConn, err := pgconn.Connect(context.Background(), sourceReplicationConnString)
	if err != nil {
		return map[string]string{"error": err.Error()}, errors.New("failed to connect to PostgreSQL server")
	}
	defer sourceConn.Close(context.Background())

	result := sourceConn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS sqlpipe_publication;")
	_, err = result.ReadAll()
	if err != nil {
		return map[string]string{"error": err.Error()}, errors.New("drop publication if exists error")
	}

	result = sourceConn.Exec(
		context.Background(),
		fmt.Sprintf("CREATE PUBLICATION sqlpipe_publication FOR TABLE %v;", strings.Join(sync.Tables, ", ")),
	)
	_, err = result.ReadAll()
	if err != nil {
		return map[string]string{"error": err.Error()}, errors.New("create publication error")
	}

	pluginArguments := []string{"proto_version '1'", "publication_names 'sqlpipe_publication'"}

	sysident, err := pglogrepl.IdentifySystem(context.Background(), sourceConn)
	if err != nil {
		return map[string]string{"error": err.Error()}, errors.New("IdentifySystem failed")
	}

	createReplicationSlotResult, err := pglogrepl.CreateReplicationSlot(context.Background(), sourceConn, sync.ReplicationSlot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		return map[string]string{"error": err.Error()}, errors.New("CreateReplicationSlot failed")
	}
	logger.PrintInfo(fmt.Sprintf("Created temporary replication slot: %v", sync.ReplicationSlot), map[string]string{})

	conn, err := sql.Open("pgx", sourceConnString)
	if err != nil {
		errProperties = map[string]string{"error": err.Error()}
		return errProperties, errors.New("unable to connect to source system")
	}
	defer conn.Close()

	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		errProperties = map[string]string{"error": err.Error()}
		return errProperties, errors.New("unable to begin data copy transaction")
	}

	// var lsnString string
	_, err = tx.Exec("SET TRANSACTION ISOLATION LEVEL repeatable read;")
	if err != nil {
		errProperties = map[string]string{"error": err.Error()}
		return errProperties, errors.New("unable to set transaction isolation level")
	}
	_, err = tx.Exec(fmt.Sprintf("set transaction snapshot '%v';", createReplicationSlotResult.SnapshotName))
	if err != nil {
		errProperties = map[string]string{"error": err.Error()}
		return errProperties, errors.New("unable to set transaction isolation level")
	}

	lsn, err := pglogrepl.ParseLSN(createReplicationSlotResult.ConsistentPoint)
	if err != nil {
		errProperties = map[string]string{"error": err.Error()}
		return errProperties, errors.New("unable to parse lsn")
	}

	for _, table := range sync.Tables {

		tableAndSchema := pkg.GetTableAndSchema(table)

		transfer := data.Transfer{
			Query:        fmt.Sprintf("select * from %v", table),
			Overwrite:    true,
			TargetSchema: sync.TargetSchema,
			TargetTable:  tableAndSchema.TableName,
			Source:       sync.Source,
			Target:       sync.Target,
		}

		rows, err := tx.Query(fmt.Sprintf("select * from %v", table))
		if err != nil {
			errProperties = map[string]string{"error": err.Error()}
			return errProperties, errors.New("unable to run data copy")
		}

		rowColumnInfo, errProperties, err := getRowsColumnInfoFromRows(sourceSystem, rows)
		if err != nil {
			return errProperties, err
		}

		errProperties, err = Insert(targetSystem, rows, transfer, rowColumnInfo)
		if err != nil {
			return errProperties, err
		}
		rows.Close()
	}

	err = tx.Commit()
	if err != nil {
		errProperties = map[string]string{"error": err.Error()}
		return errProperties, fmt.Errorf("error committing")
	}

	err = pglogrepl.StartReplication(context.Background(), sourceConn, sync.ReplicationSlot, lsn, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return map[string]string{"error": err.Error()}, errors.New("StartReplication failed")
	}
	logger.PrintInfo(fmt.Sprintf("Logical replication started on slot: %v, at location: %v", sync.ReplicationSlot, createReplicationSlotResult.ConsistentPoint), map[string]string{})

	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	txn := transaction{}
	lastRelation := relation{}

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), sourceConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return map[string]string{"error": err.Error()}, errors.New("SendStandbyStatusUpdate failed")
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err := sourceConn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return map[string]string{"error": err.Error()}, errors.New("ReceiveMessage failed")
		}

		switch msg := msg.(type) {

		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return map[string]string{"error": err.Error()}, errors.New("ParsePrimaryKeepaliveMessage failed")
				}

				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return map[string]string{"error": err.Error()}, errors.New("ParseXLogData failed")
				}

				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return map[string]string{"error": err.Error()}, errors.New("parse logical replication message failed")
				}

				switch logicalMsg.Type().String() {
				case "Begin":
					msg := logicalMsg.(*pglogrepl.BeginMessage)
					fmt.Printf(
						"Begin msg... FinalLSN: %v, CommitTime: %v, Xid: %v",
						msg.FinalLSN,
						msg.CommitTime,
						msg.Xid,
					)
					txn = transaction{}
				case "Commit":
					msg := logicalMsg.(*pglogrepl.CommitMessage)
					fmt.Printf(
						"\nCommit msg... Flags: %v, CommitLSN: %v, TransactionEndLSN: %v, CommitTime: %v\n",
						msg.Flags,
						msg.CommitLSN,
						msg.TransactionEndLSN,
						msg.CommitTime,
					)

					for _, query := range txn.queries {
						rows, errProperties, err := targetSystem.execute(query)
						if err != nil {
							fmt.Println(err, errProperties)
						}
						rows.Close()
					}
					txn = transaction{}
				case "Origin":
					msg := logicalMsg.(*pglogrepl.OriginMessage)
					fmt.Printf(
						"\nOrigin msg... CommitLSN: %v, Name: %v",
						msg.CommitLSN,
						msg.Name,
					)
				case "Relation":
					msg := logicalMsg.(*pglogrepl.RelationMessage)
					fmt.Printf(
						"\nRelation msg... RelationID: %v, Namespace: %v, RelationName: %v, ReplicaIdentity: %v, ColumnNum: %v",
						msg.RelationID,
						msg.Namespace,
						msg.RelationName,
						msg.ReplicaIdentity,
						msg.ColumnNum,
					)

					lastRelation.namespace = msg.Namespace
					lastRelation.name = msg.RelationName
					lastRelation.columns = []column{}

					for _, col := range msg.Columns {
						lastRelation.columns = append(
							lastRelation.columns,
							column{
								col.Name, !(col.Flags == 0),
								col.DataType,
								0,
							},
						)
					}

				case "Type":
					msg := logicalMsg.(*pglogrepl.TypeMessage)
					fmt.Printf(
						"\nType msg... DataType: %v, Namespace: %v, Name: %v",
						msg.DataType,
						msg.Namespace,
						msg.Name,
					)
				case "Insert":
					msg := logicalMsg.(*pglogrepl.InsertMessage)

					fmt.Printf(
						"\nInsert msg... RelationID: %v, Tuple.ColumnNum: %v",
						msg.RelationID,
						msg.Tuple.ColumnNum,
					)

					rowVals := []string{}

					for i, col := range msg.Tuple.Columns {
						var val string
						var length int = -1
						switch string(col.DataType) {
						case "t":
							val = string(col.Data)
							length = int(col.Length)
						default:
							val = string(col.Data)
						}

						rowVals = append(rowVals, val)

						lastRelation.columns[i].length = length
					}

					rowColumnInfo, errProperties, err := getRowsColumnInfoFromSync(sourceSystem, lastRelation)
					if err != nil {
						return errProperties, err
					}

					query := targetSystem.writeSyncInsert(rowVals, lastRelation, rowColumnInfo)

					txn.queries = append(txn.queries, query)

				case "Update":
					msg := logicalMsg.(*pglogrepl.UpdateMessage)

					oldTupleType := string(msg.OldTupleType)
					switch oldTupleType {
					case "K":
						fmt.Printf(
							"\nUpdate msg...\n    OldTupleType: %v, OldTuple.ColumnNum: %v",
							oldTupleType,
							msg.OldTuple.ColumnNum,
						)

						for _, col := range msg.OldTuple.Columns {
							tupleType := string(col.DataType)
							if tupleType == "t" {
								fmt.Printf("\n        Type: %v, Length: %v, Data: %v", tupleType, col.Length, string(col.Data))
							} else {
								fmt.Printf("\n        Type: %v", tupleType)
							}
						}
					case "O":
						fmt.Printf(
							"\nUpdate msg...\n    OldTupleType: %v, OldTuple.ColumnNum: %v",
							oldTupleType,
							msg.OldTuple.ColumnNum,
						)

						for _, col := range msg.OldTuple.Columns {
							tupleType := string(col.DataType)
							if tupleType == "t" {
								fmt.Printf("\n        Type: %v, Length: %v, Data: %v", tupleType, col.Length, string(col.Data))
							} else {
								fmt.Printf("\n        Type: %v", tupleType)
							}
						}
					default:
						fmt.Printf(
							"\nUpdate msg...\n    OldTupleType: %v",
							oldTupleType,
						)
					}

					fmt.Printf(
						"\n    NewTuple.ColumnNum: %v",
						msg.NewTuple.ColumnNum,
					)

					for _, col := range msg.NewTuple.Columns {
						tupleType := string(col.DataType)
						if tupleType == "t" {
							fmt.Printf("\n        Type: %v, Length: %v, Data: %v", tupleType, col.Length, string(col.Data))
						} else {
							fmt.Printf("\n        Type: %v", tupleType)
						}
					}

				case "Delete":
					msg := logicalMsg.(*pglogrepl.DeleteMessage)
					fmt.Printf("\n    Delete msg... RelationID: %v, OldTuple.ColumnNum: %v", msg.RelationID, msg.OldTuple.ColumnNum)

					oldTupleType := string(msg.OldTupleType)
					switch oldTupleType {
					case "K":
						for _, col := range msg.OldTuple.Columns {
							tupleType := string(col.DataType)
							if tupleType == "t" {
								fmt.Printf("\n        Type: %v, Length: %v, Data: %v", tupleType, col.Length, string(col.Data))
							} else {
								fmt.Printf("\n        Type: %v", tupleType)
							}
						}
					case "O":
						for _, col := range msg.OldTuple.Columns {
							tupleType := string(col.DataType)
							if tupleType == "t" {
								fmt.Printf("\n        Type: %v, Length: %v, Data: %v", tupleType, col.Length, string(col.Data))
							} else {
								fmt.Printf("\n        Type: %v", tupleType)
							}
						}
					}

				case "Truncate":
					msg := logicalMsg.(*pglogrepl.TruncateMessage)
					fmt.Printf(
						"\nTruncate msg... RelationNum: %v, Option: %v, relationIDs: %v",
						msg.RelationNum,
						msg.Option,
						msg.RelationIDs,
					)

				}

				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			return map[string]string{"message": err.Error()}, errors.New("received unexpected message")
			// log.Printf("Received unexpected message: %#v\n", msg)
		}
	}
}

func standardGetRows(
	dsConn DsConnection,
	transferInfo data.Transfer,
) (
	rows *sql.Rows,
	rowColumnInfo RowsColumnInfo,
	errProperties map[string]string,
	err error,
) {

	rows, errProperties, err = dsConn.execute(transferInfo.Query)
	if err != nil {
		return rows, rowColumnInfo, errProperties, err
	}
	rowColumnInfo, errProperties, err = getRowsColumnInfoFromRows(dsConn, rows)
	return rows, rowColumnInfo, errProperties, err
}

func standardWriteSyncInsert(
	dsConn DsConnection,
	rowVals []string,
	relation relation,
	rowColumnInfo RowsColumnInfo,
) (
	query string,
) {

	var queryBuilder strings.Builder
	queryBuilder.WriteString(dsConn.getQueryStarter(relation.name, rowColumnInfo))

	numCols := rowColumnInfo.NumCols
	zeroIndexedNumCols := numCols - 1
	colTypes := rowColumnInfo.ColumnIntermediateTypes

	// while in the middle of insert row, add commas at end of values
	for j := 0; j < zeroIndexedNumCols; j++ {
		switch rowVals[j] {
		case "":
			queryBuilder.WriteString(dsConn.getValToWriteMidRow("NIL", rowVals[j]))
		default:
			queryBuilder.WriteString(dsConn.getValToWriteMidRow(colTypes[j], rowVals[j]))
		}
	}

	queryBuilder.WriteString(dsConn.getValToWriteRowEnd(colTypes[zeroIndexedNumCols], rowVals[zeroIndexedNumCols]))

	return queryBuilder.String()
}

func rowsInsert(
	dsConn DsConnection,
	rows *sql.Rows,
	transfer data.Transfer,
	rowColumnInfo RowsColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	numCols := rowColumnInfo.NumCols
	zeroIndexedNumCols := numCols - 1
	targetTable := transfer.TargetTable
	colTypes := rowColumnInfo.ColumnIntermediateTypes

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
	var insertErrProperties map[string]string
	var insertRows *sql.Rows

	for i := 1; rows.Next(); i++ {
		// scan incoming values into valueptrs, which in turn points to values
		rows.Scan(valuePtrs...)

		if isFirst {
			queryBuilder.WriteString(dsConn.getQueryStarter(targetTable, rowColumnInfo))
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
			queryBuilder.Reset()
			withQueryEnder := fmt.Sprintf("%s%s", noUnionAll, dsConn.getQueryEnder(targetTable))
			queryString := sqlEndStringNilReplacer.Replace(withQueryEnder)
			wg.Wait()
			if insertError != nil {
				return insertErrProperties, insertError
			}
			wg.Add(1)
			pkg.Background(func() {
				defer wg.Done()
				insertRows, insertErrProperties, insertError = dsConn.execute(queryString)
				if insertError != nil {
					return
				}
				defer insertRows.Close()
			})
			isFirst = true
		}
	}
	// if we still have some leftovers, add those too.
	if !isFirst {
		noUnionAll := strings.TrimSuffix(queryBuilder.String(), " UNION ALL ")
		withQueryEnder := fmt.Sprintf("%s%s", noUnionAll, dsConn.getQueryEnder(targetTable))
		queryString := sqlEndStringNilReplacer.Replace(withQueryEnder)
		wg.Wait()
		if insertError != nil {
			return insertErrProperties, insertError
		}
		wg.Add(1)
		pkg.Background(func() {
			defer wg.Done()
			insertRows, insertErrProperties, insertError = dsConn.execute(queryString)
			if insertError != nil {
				return
			}
			defer insertRows.Close()
		})
	}
	wg.Wait()
	if insertError != nil {
		return insertErrProperties, insertError
	}

	return nil, nil
}

func turboTransfer(
	dsConn DsConnection,
	rows *sql.Rows,
	transferInfo data.Transfer,
	rowColumnInfo RowsColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	return dsConn.turboTransfer(rows, transferInfo, rowColumnInfo)
}

func Insert(
	dsConn DsConnection,
	rows *sql.Rows,
	transfer data.Transfer,
	rowColumnInfo RowsColumnInfo,
) (
	errProperties map[string]string,
	err error,
) {
	defer rows.Close()

	if transfer.Overwrite {
		errProperties, err = dsConn.dropTable(transfer)
		if err != nil {
			return errProperties, err
		}
		errProperties, err = dsConn.createTable(transfer, rowColumnInfo)
		if err != nil {
			return errProperties, err
		}
	}

	return rowsInsert(dsConn, rows, transfer, rowColumnInfo)
}

func standardGetFormattedResults(
	dsConn DsConnection,
	query string,
) (
	queryResult QueryResult,
	errProperties map[string]string,
	err error,
) {

	rows, errProperties, err := dsConn.execute(query)
	if err != nil {
		return queryResult, errProperties, err
	}

	rowColumnInfo, errProperties, err := getRowsColumnInfoFromRows(dsConn, rows)

	queryResult.ColumnTypes = map[string]string{}
	queryResult.Rows = []interface{}{}
	for i, colType := range rowColumnInfo.ColumnDbTypes {
		queryResult.ColumnTypes[rowColumnInfo.ColumnNames[i]] = colType
	}

	numCols := rowColumnInfo.NumCols
	colTypes := rowColumnInfo.ColumnIntermediateTypes

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

func standardExecute(query string, dsType string, db *sql.DB) (rows *sql.Rows, errProperties map[string]string, err error) {
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

func getRowsColumnInfoFromRows(
	dsConn DsConnection,
	rows *sql.Rows,
) (
	rowColumnInfo RowsColumnInfo,
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
		return rowColumnInfo, errProperties, err
	}

	for _, colType := range colTypesFromDriver {
		colNames = append(colNames, colType.Name())
		colTypes = append(colTypes, colType.DatabaseTypeName())
		scanTypes = append(scanTypes, colType.ScanType())

		intermediateType, errProperties, err := dsConn.getIntermediateType(colType.DatabaseTypeName())
		if err != nil {
			return rowColumnInfo, errProperties, err
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

	columnInfo := RowsColumnInfo{
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

func getRowsColumnInfoFromSync(
	dsConn DsConnection,
	relation relation,
) (
	rowColumnInfo RowsColumnInfo,
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

	for _, col := range relation.columns {
		colType := fmt.Sprint(col.colType)
		colNames = append(colNames, col.name)
		colTypes = append(colTypes, colType)
		scanTypes = append(scanTypes, reflect.TypeOf(""))

		intermediateType, errProperties, err := dsConn.getIntermediateType(colType)
		if err != nil {
			return rowColumnInfo, errProperties, err
		}
		intermediateTypes = append(intermediateTypes, intermediateType)

		colLen := col.length
		lenOk := false
		if colLen > -1 {
			lenOk = true
		}
		colLens = append(colLens, int64(colLen))
		lenOks = append(lenOks, lenOk)

		precisionScaleOk := false
		precisions = append(precisions, 0)
		scales = append(scales, 0)
		precisionScaleOks = append(precisionScaleOks, precisionScaleOk)

		nullables = append(nullables, false)
		nullableOks = append(nullableOks, false)

		colNamesToTypes[col.name] = colType
	}

	columnInfo := RowsColumnInfo{
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

func standardGetQueryStarter(targetTable string, colNames []string) string {
	return fmt.Sprintf("INSERT INTO %s ("+strings.Join(colNames, ", ")+") VALUES (", targetTable)
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

	rows, errProperties, err := dsConn.execute(dropQuery)
	if err != nil {
		return errProperties, err
	}
	defer rows.Close()

	return errProperties, err
}

func dropTableIfExistsNoSchema(dsConn DsConnection, transferInfo data.Transfer) (errProperties map[string]string, err error) {
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %v", transferInfo.TargetTable)
	rows, errProperties, err := dsConn.execute(dropQuery)
	if err != nil {
		return errProperties, err
	}
	defer rows.Close()
	return errProperties, err
}

func dropTableNoSchema(dsConn DsConnection, transferInfo data.Transfer) (errProperties map[string]string, err error) {
	dropQuery := fmt.Sprintf("DROP TABLE %v", transferInfo.TargetTable)
	rows, errProperties, err := dsConn.execute(dropQuery)
	if err != nil {
		return errProperties, err
	}
	defer rows.Close()
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

	rows, errProperties, err := dsConn.execute(query)
	if err != nil {
		return errProperties, err
	}
	defer rows.Close()

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
	rows, errProperties, err := dsConn.execute(query)
	if err != nil {
		return errProperties, err
	}
	defer rows.Close()

	return errProperties, err
}

func standardCreateTable(
	dsConn DsConnection,
	transferInfo data.Transfer,
	columnInfo RowsColumnInfo,
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

	rows, errProperties, err := dsConn.execute(query)
	if err != nil {
		return errProperties, err
	}
	defer rows.Close()

	return errProperties, err
}
