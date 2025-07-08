package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/time/rate"
)

type Postgresql struct {
	db              *sql.DB
	replConn        *pgconn.PgConn
	app             *application
	systemInfo      SystemInfo
	receiveFieldMap map[string]map[string]map[string]Location
	pushFieldMap    map[string]map[string]map[string]Location
	limiter         *rate.Limiter
}

func (app *application) newPostgresql(systemInfo SystemInfo) (postgresql Postgresql, err error) {
	db, err := openConnectionPool(systemInfo.Name, systemInfo.ConnectionString, DriverPostgreSQL)
	if err != nil {
		return postgresql, fmt.Errorf("error opening postgresql db :: %v", err)
	}

	// Create replication connection
	replConn, err := pgconn.Connect(context.Background(), systemInfo.ReplicationDsn)
	if err != nil {
		return postgresql, fmt.Errorf("error opening postgresql replication connection :: %v", err)
	}

	postgresql.db = db
	postgresql.replConn = replConn
	postgresql.app = app
	postgresql.systemInfo = systemInfo
	postgresql.limiter = rate.NewLimiter(rate.Limit(systemInfo.RateLimit), systemInfo.RateBucketSize)
	postgresql.receiveFieldMap = app.receiveFieldMap[systemInfo.Name]
	postgresql.pushFieldMap = app.pushFieldMap[systemInfo.Name]

	// fmt.Println("push field map in create new", app.pushFieldMap)
	// fmt.Println("receive field map in create new", app.receiveFieldMap)

	var index int64 = 0
	app.storageEngine.setSafeIndexMap(systemInfo.Name, index)

	go postgresql.watchQueue()
	go postgresql.watchCDC()

	return postgresql, nil
}

func (p Postgresql) watchQueue() {
	var index int64
	for {
		// Get the last safe object index for this system
		var exists bool
		index, exists = p.app.storageEngine.getSafeIndexMap(p.systemInfo.Name)
		if !exists {
			panic(fmt.Sprintf("safe index not found for system %s", p.systemInfo.Name))
		}

		// Wait for rate limiter
		err := p.limiter.Wait(context.Background())
		if err != nil {
			// Optionally log or handle error, then break or continue
			continue
		}

		// Query safeObjects after lastIndex
		objects := p.app.storageEngine.getSafeObjectsFromIndex(index)
		if len(objects) > 0 {
			// Process new objects as needed
			index += int64(len(objects))
			// Example: log or handle objects
			// fmt.Printf("New safeObjects: %v\n", objects)
		}

		// newObjs := make(map[string]interface{})

		for _, obj := range objects {
			searchFields := []string{}
			object := obj.(map[string]interface{})

			// ensure there is only 1 key. that is the object type
			if len(object) != 1 {
				p.app.logger.Error("object does not have exactly one key", "object", obj)
				continue
			}

			// Get the object type (the only key in the map)
			var objectType string
			for key := range object {
				objectType = key
			}

			objectVal := object[objectType].(map[string]interface{})

			newObj := map[string]interface{}{}
			for locationInSystem, fieldMap := range p.pushFieldMap[objectType] {
				for keyInSchema, desiredKey := range fieldMap {
					if _, ok := objectVal[keyInSchema]; ok {
						newObj[desiredKey.Field] = objectVal[keyInSchema]
						if fieldMap[keyInSchema].SearchKey {
							searchFields = append(searchFields, desiredKey.Field)
						}
					}
				}

				// newObjs[locationInSystem] = newObj
				err = p.upsertJSON(newObj, searchFields, locationInSystem)
				if err != nil {
					p.app.logger.Error("error upserting JSON to PostgreSQL", "error", err, "objectType", objectType, "locationInSystem", locationInSystem, "data", newObj)
				}
			}
		}

		// Update the safe index map for this system
		p.app.storageEngine.setSafeIndexMap(p.systemInfo.Name, index)
	}
}

func (p Postgresql) handleWebhook(w http.ResponseWriter, r *http.Request) {
	p.app.logger.Error("PostgreSQL does not support webhooks", "system", p.systemInfo.Name)
}

func (p Postgresql) upsertJSON(data map[string]interface{}, searchFields []string, locationInSystem string) error {
	// Find the first available search field in data
	// fmt.Println("upserting:", data, "searchFields:", searchFields, "locationInSystem:", locationInSystem)
	var conflictFields []string
	for _, field := range searchFields {
		if _, ok := data[field]; ok {
			conflictFields = append(conflictFields, field)
		}
	}
	if len(conflictFields) == 0 {
		return fmt.Errorf("none of the search fields found in data")
	}

	// Collect column names and values
	columns := make([]string, 0, len(data))
	placeholders := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))

	idx := 1
	for k, v := range data {
		columns = append(columns, k)
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
		values = append(values, v)
		idx++
	}

	// Build ON CONFLICT SET clause, excluding conflict fields
	updates := make([]string, 0, len(columns))
	conflictFieldSet := make(map[string]struct{}, len(conflictFields))
	for _, f := range conflictFields {
		conflictFieldSet[f] = struct{}{}
	}
	for _, col := range columns {
		if _, isConflict := conflictFieldSet[col]; !isConflict {
			updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}

	// Assemble SQL
	query := fmt.Sprintf(`
		INSERT INTO %s (%s)
		VALUES (%s)
		ON CONFLICT (%s) DO UPDATE SET %s
	`,
		locationInSystem,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(conflictFields, ", "),
		strings.Join(updates, ", "),
	)

	// Execute
	_, err := p.db.Exec(query, values...)
	return err
}

// Start CDC for all tables in publication
func (p *Postgresql) watchCDC() {
	slotName := "sqlpipe_slot"
	outputPlugin := "wal2json"

	replConn, err := pgconn.Connect(context.Background(), p.systemInfo.ReplicationDsn)
	if err != nil {
		p.app.logger.Error("failed to connect", "error", err)
		os.Exit(1)
	}
	defer replConn.Close(context.Background())

	sysident, err := pglogrepl.IdentifySystem(context.Background(), replConn)
	if err != nil {
		p.app.logger.Error("IdentifySystem failed", "error", err)
		os.Exit(1)
	}

	_, err = pglogrepl.CreateReplicationSlot(context.Background(), replConn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: false, Mode: pglogrepl.LogicalReplication})
	if err != nil {
		// If the error is "already exists", it's OK, otherwise fail
		if !strings.Contains(err.Error(), "already exists") {
			p.app.logger.Error("CreateReplicationSlot failed", "error", err)
			os.Exit(1)
		}
	}

	pluginArguments := []string{"\"pretty-print\" 'true'"}
	err = pglogrepl.StartReplication(context.Background(), replConn, slotName, sysident.XLogPos,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArguments,
		})
	if err != nil {
		p.app.logger.Error("StartReplication failed", "error", err)
		os.Exit(1)
	}

	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	// relations := map[uint32]*pglogrepl.RelationMessage{}
	// typeMap := pgtype.NewMap()

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			// log.Printf("Sent Standby status message at %s\n", clientXLogPos.String())
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		rawMsg, err := replConn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalln("ReceiveMessage failed:", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Fatalf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message: %T\n", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
			}
			// log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Fatalln("ParseXLogData failed:", err)
			}

			if outputPlugin == "wal2json" {
				log.Printf("wal2json data: %s\n", string(xld.WALData))
			}
			// else {
			// log.Printf("XLogData => WALStart %s ServerWALEnd %s ServerTime %s WALData:\n", xld.WALStart, xld.ServerWALEnd, xld.ServerTime)
			// if v2 {
			// 	processV2(xld.WALData, relationsV2, typeMap, &inStream)
			// } else {
			// processV1(xld.WALData, relations, typeMap)
			// }
			// }

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
		default:
		}
	}
}

// func processV2(walData []byte, relations map[uint32]*pglogrepl.RelationMessageV2, typeMap *pgtype.Map, inStream *bool) {
// 	logicalMsg, err := pglogrepl.ParseV2(walData, *inStream)
// 	if err != nil {
// 		log.Fatalf("Parse logical replication message: %s", err)
// 	}
// 	log.Printf("Receive a logical replication message: %s", logicalMsg.Type())
// 	switch logicalMsg := logicalMsg.(type) {
// 	case *pglogrepl.RelationMessageV2:
// 		relations[logicalMsg.RelationID] = logicalMsg

// 	case *pglogrepl.BeginMessage:
// 		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

// 	case *pglogrepl.CommitMessage:

// 	case *pglogrepl.InsertMessageV2:
// 		rel, ok := relations[logicalMsg.RelationID]
// 		if !ok {
// 			log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
// 		}
// 		values := map[string]interface{}{}
// 		for idx, col := range logicalMsg.Tuple.Columns {
// 			colName := rel.Columns[idx].Name
// 			switch col.DataType {
// 			case 'n': // null
// 				values[colName] = nil
// 			case 'u': // unchanged toast
// 				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
// 			case 't': //text
// 				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
// 				if err != nil {
// 					log.Fatalln("error decoding column data:", err)
// 				}
// 				values[colName] = val
// 			}
// 		}
// 		log.Printf("insert for xid %d\n", logicalMsg.Xid)
// 		log.Printf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)

// 	case *pglogrepl.UpdateMessageV2:
// 		log.Printf("update for xid %d\n", logicalMsg.Xid)
// 		// ...
// 	case *pglogrepl.DeleteMessageV2:
// 		log.Printf("delete for xid %d\n", logicalMsg.Xid)
// 		// ...
// 	case *pglogrepl.TruncateMessageV2:
// 		log.Printf("truncate for xid %d\n", logicalMsg.Xid)
// 		// ...

// 	case *pglogrepl.TypeMessageV2:
// 	case *pglogrepl.OriginMessage:

// 	case *pglogrepl.LogicalDecodingMessageV2:
// 		log.Printf("Logical decoding message: %q, %q, %d", logicalMsg.Prefix, logicalMsg.Content, logicalMsg.Xid)

// 	case *pglogrepl.StreamStartMessageV2:
// 		*inStream = true
// 		log.Printf("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
// 	case *pglogrepl.StreamStopMessageV2:
// 		*inStream = false
// 		log.Printf("Stream stop message")
// 	case *pglogrepl.StreamCommitMessageV2:
// 		log.Printf("Stream commit message: xid %d", logicalMsg.Xid)
// 	case *pglogrepl.StreamAbortMessageV2:
// 		log.Printf("Stream abort message: xid %d", logicalMsg.Xid)
// 	default:
// 		log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
// 	}
// }

func processV1(walData []byte, relations map[uint32]*pglogrepl.RelationMessage, typeMap *pgtype.Map) {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		log.Fatalf("Parse logical replication message: %s", err)
	}
	log.Printf("Receive a logical replication message: %s", logicalMsg.Type())
	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

	case *pglogrepl.CommitMessage:

	case *pglogrepl.InsertMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
		}
		values := map[string]interface{}{}
		for idx, col := range logicalMsg.Tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Fatalln("error decoding column data:", err)
				}
				values[colName] = val
			}
		}
		log.Printf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)

	case *pglogrepl.UpdateMessage:
		// ...
	case *pglogrepl.DeleteMessage:
		// ...
	case *pglogrepl.TruncateMessage:
		// ...

	case *pglogrepl.TypeMessage:
	case *pglogrepl.OriginMessage:

	case *pglogrepl.LogicalDecodingMessage:
		log.Printf("Logical decoding message: %q, %q", logicalMsg.Prefix, logicalMsg.Content)

	case *pglogrepl.StreamStartMessageV2:
		log.Printf("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
	case *pglogrepl.StreamStopMessageV2:
		log.Printf("Stream stop message")
	case *pglogrepl.StreamCommitMessageV2:
		log.Printf("Stream commit message: xid %d", logicalMsg.Xid)
	case *pglogrepl.StreamAbortMessageV2:
		log.Printf("Stream abort message: xid %d", logicalMsg.Xid)
	default:
		log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
