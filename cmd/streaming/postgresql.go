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

type ExpiringMapAny struct {
	Object map[string]interface{} `json:"object"`
	Expiry time.Time              `json:"expiry"`
}

func newExpiringMapAny(object map[string]any, timeFromNow time.Duration) ExpiringMapAny {
	return ExpiringMapAny{
		Object: object,
		Expiry: time.Now().Add(timeFromNow),
	}
}

type Postgresql struct {
	db               *sql.DB
	replConn         *pgconn.PgConn
	app              *application
	systemInfo       SystemInfo
	receiveFieldMap  map[string]map[string]map[string]Location
	pushFieldMap     map[string]map[string]map[string]Location
	limiter          *rate.Limiter
	duplicateChecker map[string][]ExpiringMapAny
}

func (app *application) newPostgresql(systemInfo SystemInfo, duplicateChecker map[string][]ExpiringMapAny) (postgresql Postgresql, err error) {
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

		for _, obj := range objects {
			searchFields := []string{}
			object := obj.(map[string]interface{})

			prettyObj, err := json.MarshalIndent(object, "", "  ")
			if err != nil {
				p.app.logger.Error("error pretty printing object", "error", err)
			} else {
				fmt.Printf("Postgresql got object from queue:\n%s\n", string(prettyObj))
			}

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

			var objectIsDuplicate bool
			foundDuplicate := false
			for i, expiringObj := range p.duplicateChecker[objectType] {

				objectIsDuplicate = true

				for k, v := range expiringObj.Object {
					if v != objectVal[k] {
						objectIsDuplicate = false
						break
					}
				}

				if objectIsDuplicate {
					fmt.Println("Postgresql found duplicate object in duplicate checker while watching queue:", expiringObj.Object)
					// If we found a duplicate, we can remove it from the duplicate checker
					p.duplicateChecker[objectType] = append(p.duplicateChecker[objectType][:i], p.duplicateChecker[objectType][i+1:]...)
					foundDuplicate = true
					break
				}
			}

			if !foundDuplicate {
				fmt.Println("No duplicate found for object, upserting to PostgreSQL", obj)
				for locationInSystem, fieldMap := range p.pushFieldMap[objectType] {
					newObj := map[string]interface{}{}
					for keyInSchema, location := range fieldMap {
						if _, ok := objectVal[keyInSchema]; ok {
							if location.Push || location.SearchKey {
								newObj[location.Field] = objectVal[keyInSchema]
							}

							if fieldMap[keyInSchema].SearchKey {
								searchFields = append(searchFields, location.Field)
							}
						}
					}

					fmt.Printf("PostgreSQL is upserting object: %v\n", newObj)

					err = p.upsertJSON(objectVal, searchFields, locationInSystem, objectType)
					if err != nil {
						p.app.logger.Error("error upserting JSON to PostgreSQL", "error", err, "objectType", objectType, "locationInSystem", locationInSystem, "data", newObj)
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

func (p Postgresql) upsertJSON(data map[string]interface{}, searchFields []string, locationInSystem string, objectType string) error {
	var foundMatch bool
	var conflictField string
	var conflictValue interface{}

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
		values := make([]interface{}, 0, len(data))
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
		insertValues := make([]interface{}, 0, len(data))

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

	expiringObj := newExpiringMapAny(data, p.app.config.keepDuplicatesFor)
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

type CdcChange struct {
	Kind         string        `json:"kind"`
	Schema       string        `json:"schema"`
	Table        string        `json:"table"`
	ColumnNames  []string      `json:"columnnames"`
	ColumnTypes  []string      `json:"columntypes"`
	ColumnValues []interface{} `json:"columnvalues"`
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

	// objs := []map[string]interface{}{}

	for _, change := range event.Change {
		objectName := change.Schema + "." + change.Table

		// fmt.Println("Received CDC event", objectName)

		fmt.Println("Raw cdc data:", jsonString)

		obj := map[string]interface{}{}
		newObjs := make(map[string]map[string]interface{})

		for i, colName := range change.ColumnNames {
			if change.ColumnValues[i] != nil {
				obj[colName] = change.ColumnValues[i]
			}
		}

		for schemaName, fieldMap := range p.receiveFieldMap[objectName] {
			newObj := map[string]interface{}{}

			for keyInObj, location := range fieldMap {
				if location.Pull || location.SearchKey {
					newObj[location.Field] = obj[keyInObj]
				}
			}

			newObjs[schemaName] = newObj
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
				return fmt.Errorf("no schema found for object: %s", objectName)
			}

			err = schema.Validate(obj)
			if err != nil {
				return fmt.Errorf("object failed postgresql schema validation for '%s': %v", objectName, err)
			}

			var objectIsDuplicate bool
			foundDuplicate := false
			for i, expiringObj := range p.duplicateChecker[schemaName] {

				objectIsDuplicate = true

				for k, v := range obj {
					if v != expiringObj.Object[k] {
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

				// also add to storage engine
				fmt.Println("PostgreSQL is storing object in queue", obj)
				p.app.storageEngine.addSafeObject(obj, schemaName)

				fmt.Println("PostgreSQL no duplicate found for object, adding to duplicate checker", obj)
				expiringObj := newExpiringMapAny(obj, p.app.config.keepDuplicatesFor)
				p.duplicateChecker[schemaName] = append(p.duplicateChecker[schemaName], expiringObj)
			}
		}
	}

	// p.app.storageEngine.printAllContents()

	return nil
}
