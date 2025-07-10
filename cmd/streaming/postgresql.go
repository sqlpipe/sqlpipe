package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/time/rate"
)

type ExpiringObject struct {
	Object Object    `json:"object"`
	Expiry time.Time `json:"expiry"`
}

func newExpiringObject(object Object, timeFromNow time.Duration) ExpiringObject {
	return ExpiringObject{
		Object: object,
		Expiry: time.Now().Add(timeFromNow),
	}
}

type Postgresql struct {
	db               *sql.DB
	replConn         *pgconn.PgConn
	app              *application
	systemInfo       SystemInfo
	limiter          *rate.Limiter
	duplicateChecker map[string][]ExpiringObject
}

func (app *application) newPostgresql(systemInfo SystemInfo, duplicateChecker map[string][]ExpiringObject) (postgresql Postgresql, err error) {
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
	postgresql.duplicateChecker = duplicateChecker

	app.storageEngine.setSafeIndexMap(systemInfo.Name, 0)

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
		}

		for _, object := range objects {
			searchFields := []string{}

			prettyObj, err := json.MarshalIndent(object, "", "  ")
			if err != nil {
				p.app.logger.Error("error pretty printing object", "error", err)
			} else {
				fmt.Printf("Postgresql got object from queue:\n%s\n", string(prettyObj))
			}

			for locationInSystem, fields := range p.systemInfo.PushRouter[object.Type] {
				newObj := map[string]any{}
				for keyInSchema, location := range fields {
					if _, ok := object.Payload[keyInSchema]; ok {
						newObj[location.Field] = object.Payload[keyInSchema]

						if fields[keyInSchema].SearchKey {
							searchFields = append(searchFields, location.Field)
						}
					}
				}

				var objectIsDuplicate bool
				foundDuplicate := false
				for i, expiringObj := range p.duplicateChecker[object.Type] {

					objectIsDuplicate = true

					for k, v := range newObj {
						if _, ok := expiringObj.Object.Payload[k]; !ok {
							objectIsDuplicate = false
							break
						}
						if v != expiringObj.Object.Payload[k] {
							objectIsDuplicate = false
							break
						}
					}

					if objectIsDuplicate {
						fmt.Println("Postgresql found duplicate object in duplicate checker while watching queue:", expiringObj.Object)
						// If we found a duplicate, we can remove it from the duplicate checker
						p.duplicateChecker[object.Type] = append(p.duplicateChecker[object.Type][:i], p.duplicateChecker[object.Type][i+1:]...)
						foundDuplicate = true
						break
					}
				}

				if !foundDuplicate {
					fmt.Println("No duplicate found for object, upserting to PostgreSQL", object)
					fmt.Printf("PostgreSQL is upserting object: %v\n", newObj)

					switch object.Operation {
					case "upsert":
						err = p.upsertJSON(object.Payload, searchFields, locationInSystem, object.Type)
						if err != nil {
							p.app.logger.Error("error upserting JSON to PostgreSQL", "error", err, "objectType", object.Type, "locationInSystem", locationInSystem, "data", newObj)
						}
					case "delete":
						err = p.deleteFromPostgresql(object.Payload, searchFields, locationInSystem)
						if err != nil {
							p.app.logger.Error("error deleting from PostgreSQL", "error", err, "objectType", object.Type, "locationInSystem", locationInSystem, "data", newObj)
						}
					}
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

func (p Postgresql) upsertJSON(data map[string]any, searchFields []string, locationInSystem string, objectType string) error {
	var foundMatch bool
	var conflictField string
	var conflictValue any

	for _, field := range searchFields {
		if v, ok := data[field]; ok {
			// Check if a row exists with this search field
			query := fmt.Sprintf("SELECT 1 FROM %s WHERE %s = $1 LIMIT 1", locationInSystem, field)
			row := p.db.QueryRow(query, v)
			var dummy int
			err := row.Scan(&dummy)
			if err == nil {
				foundMatch = true
				conflictField = field
				conflictValue = v
				break
			}
			if err != sql.ErrNoRows && err != nil {
				p.app.logger.Error("error checking for existing row", "error", err, "query", query, "value", v)
				return fmt.Errorf("error checking for existing row: %v", err)
			}
		}
	}

	if foundMatch {
		// Prepare UPDATE: set all columns except the conflict field
		setCols := make([]string, 0, len(data))
		values := make([]any, 0, len(data))
		idx := 1
		for k, v := range data {
			if k != conflictField {
				setCols = append(setCols, fmt.Sprintf("%s = $%d", k, idx))
				values = append(values, v)
				idx++
			}
		}
		// Add WHERE for the conflict field
		whereClause := fmt.Sprintf("%s = $%d", conflictField, idx)
		values = append(values, conflictValue)

		updateQuery := fmt.Sprintf(
			"UPDATE %s SET %s WHERE %s",
			locationInSystem,
			strings.Join(setCols, ", "),
			whereClause,
		)

		_, err := p.db.Exec(updateQuery, values...)
		if err != nil {
			p.app.logger.Error("error executing update query", "error", err, "query", updateQuery, "values", values)
			return fmt.Errorf("error executing update query: %v", err)
		}
	} else {
		// Build INSERT
		columns := make([]string, 0, len(data))
		placeholders := make([]string, 0, len(data))
		insertValues := make([]any, 0, len(data))

		idx := 1
		for k, v := range data {
			columns = append(columns, k)
			placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
			insertValues = append(insertValues, v)
			idx++
		}
		insertQuery := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			locationInSystem,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
		)
		_, err := p.db.Exec(insertQuery, insertValues...)
		if err != nil {
			p.app.logger.Error("error executing insert query", "error", err, "query", insertQuery, "values", insertValues)
			return fmt.Errorf("error executing insert query: %v", err)
		}
	}

	object := Object{
		Type:    objectType,
		Payload: data,
	}

	expiringObj := newExpiringObject(object, p.app.config.keepDuplicatesFor)
	p.duplicateChecker[objectType] = append(p.duplicateChecker[objectType], expiringObj)

	return nil
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

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
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

			// if outputPlugin == "wal2json" {
			// 	log.Printf("wal2json data: %s\n", string(xld.WALData))
			// }

			err = p.handleCdcEvent(string(xld.WALData))
			if err != nil {
				p.app.logger.Error("error handling CDC event", "error", err, "data", string(xld.WALData))
				return
			}

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
		default:
		}
	}
}

type OldKeys struct {
	KeyNames  []string `json:"keynames"`
	KeyValues []any    `json:"keyvalues,omitempty"` // Optional, if not provided, the old keys are not included
}

type CdcChange struct {
	Kind         string   `json:"kind"`
	Schema       string   `json:"schema"`
	Table        string   `json:"table"`
	ColumnNames  []string `json:"columnnames"`
	ColumnTypes  []string `json:"columntypes"`
	ColumnValues []any    `json:"columnvalues"`
	OldKeys      OldKeys  `json:"oldkeys,omitempty"` // Optional, if not provided, the old keys are not included
}

type CdcEvent struct {
	Change []CdcChange `json:"change"`
}

func (p Postgresql) handleCdcEvent(jsonString string) error {

	// fmt.Println("receive field map: ", p.receiveFieldMap)

	var event CdcEvent
	err := json.Unmarshal([]byte(jsonString), &event)
	if err != nil {
		return fmt.Errorf("error unmarshalling CDC event: %v", err)
	}

	// objs := []map[string]any{}

	for _, change := range event.Change {
		pullLocation := change.Schema + "." + change.Table
		operationType := change.Kind

		obj := map[string]any{}
		newObjs := make(map[string]map[string]any)

		switch operationType {
		case "insert", "update":
			operationType = "upsert"
			for i, colName := range change.ColumnNames {
				if change.ColumnValues[i] != nil {
					obj[colName] = change.ColumnValues[i]
				}
			}
		case "delete":
			operationType = "delete"
			for i, colName := range change.OldKeys.KeyNames {
				if change.OldKeys.KeyValues != nil {
					obj[colName] = change.OldKeys.KeyValues[i]
				}
			}
		default:
			return fmt.Errorf("unknown operation type: %s", operationType)
		}

		fmt.Println("Received cdc data from PostgreSQL:", jsonString)

		for objectType, pullObject := range p.systemInfo.ReceiveRouter[pullLocation] {
			newObj := map[string]any{}

			for keyInObj, fields := range pullObject {
				newObj[fields.Field] = obj[keyInObj]
			}

			newObjs[objectType] = newObj
		}

		for schemaName, obj := range newObjs {

			for k, v := range obj {
				if v == nil {
					delete(obj, k)
				}
			}

			fmt.Println("Postgresql validating / dupe scanning obj:", obj)

			schema, inMap := p.app.schemaMap[schemaName]
			if !inMap {
				return fmt.Errorf("no schema found for pull location: %s", pullLocation)
			}

			err = schema.Validate(obj)
			if err != nil {
				return fmt.Errorf("object failed postgresql schema validation for '%s': %v", pullLocation, err)
			}

			var objectIsDuplicate bool
			foundDuplicate := false
			for i, expiringObj := range p.duplicateChecker[schemaName] {

				objectIsDuplicate = true

				for k, v := range obj {
					if v != expiringObj.Object.Payload[k] {
						objectIsDuplicate = false
						break
					}
				}

				if objectIsDuplicate {
					fmt.Println("Postgresql found duplicate object in duplicate checker while handling cdc event:", expiringObj.Object)
					// If we found a duplicate, we can remove it from the duplicate checker
					p.duplicateChecker[schemaName] = append(p.duplicateChecker[schemaName][:i], p.duplicateChecker[schemaName][i+1:]...)
					foundDuplicate = true
					break
				}
			}

			if !foundDuplicate {

				object := Object{
					Operation: operationType,
					Type:      schemaName,
					Payload:   obj,
				}

				// also add to storage engine
				fmt.Println("PostgreSQL no duplicate found. Storing object in queue", obj)
				p.app.storageEngine.addSafeObject(object)

				fmt.Println("PostgreSQL no duplicate found for object, adding to duplicate checker", obj)
				expiringObj := newExpiringObject(object, p.app.config.keepDuplicatesFor)
				p.duplicateChecker[schemaName] = append(p.duplicateChecker[schemaName], expiringObj)
			}
		}
	}

	return nil
}

// deleteFromPostgresql deletes a row from PostgreSQL based on the searchFields and payload.
func (p Postgresql) deleteFromPostgresql(payload map[string]any, searchFields []string, locationInSystem string) error {
	if len(searchFields) == 0 {
		return fmt.Errorf("no search fields provided for delete operation")
	}

	whereClauses := make([]string, 0, len(searchFields))
	values := make([]any, 0, len(searchFields))
	idx := 1
	for _, field := range searchFields {
		val, ok := payload[field]
		if !ok {
			return fmt.Errorf("search field '%s' not found in payload", field)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", field, idx))
		values = append(values, val)
		idx++
	}

	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s", locationInSystem, strings.Join(whereClauses, " AND "))
	_, err := p.db.Exec(deleteQuery, values...)
	if err != nil {
		p.app.logger.Error("error executing delete query", "error", err, "query", deleteQuery, "values", values)
		return fmt.Errorf("error executing delete query: %v", err)
	}
	return nil
}
