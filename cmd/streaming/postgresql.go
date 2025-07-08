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
		}

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

				err = p.upsertJSON(newObj, searchFields, locationInSystem, objectType)
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

func (p Postgresql) upsertJSON(data map[string]interface{}, searchFields []string, locationInSystem string, objectType string) error {
	// Find the first available search field in data
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
	if err != nil {
		p.app.logger.Error("error executing upsert query", "error", err, "query", query, "values", values)
		return fmt.Errorf("error executing upsert query: %v", err)
	}

	expiringObj := newExpiringMapAny(data, p.app.config.keepDuplicatesFor)
	// Add the expiring object to the duplicate checker
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

	fmt.Println("receive field map: ", p.receiveFieldMap)

	var event CdcEvent
	err := json.Unmarshal([]byte(jsonString), &event)
	if err != nil {
		return fmt.Errorf("error unmarshalling CDC event: %v", err)
	}

	// objs := []map[string]interface{}{}

	for _, change := range event.Change {
		objectName := change.Schema + "." + change.Table

		// fmt.Println("Received CDC event", objectName)

		// fmt.Println("Raw cdc data:", jsonString)

		obj := map[string]interface{}{}

		for i, colName := range change.ColumnNames {
			if change.ColumnValues[i] != nil {
				obj[colName] = change.ColumnValues[i]
			}
		}

		// objs = append(objs, obj)

		newObjs := make(map[string]map[string]interface{})

		for schemaName, fieldMap := range p.receiveFieldMap[objectName] {
			newModel := map[string]interface{}{}

			for keyInObj, desiredKey := range fieldMap {
				newModel[desiredKey.Field] = obj[keyInObj]
			}

			newObjs[schemaName] = newModel
		}

		for schemaName, obj := range newObjs {

			schema, inMap := p.app.schemaMap[schemaName]
			if !inMap {
				return fmt.Errorf("no schema found for object: %s", objectName)
			}

			err = schema.Validate(obj)
			if err != nil {
				return fmt.Errorf("object failed schema validation for '%s': %v", objectName, err)
			}

			for k, v := range obj {
				if v == nil {
					delete(obj, k)
				}
			}

			fmt.Println("Adding object to storage engine", obj)

			var objectIsDuplicate bool
			foundDuplicate := false
			for i, expiringObj := range p.duplicateChecker[schemaName] {

				objectIsDuplicate = true

				for k, v := range expiringObj.Object {
					if v != obj[k] {
						objectIsDuplicate = false
						break
					}
				}

				if objectIsDuplicate {
					// If we found a duplicate, we can remove it from the duplicate checker
					p.duplicateChecker[schemaName] = append(p.duplicateChecker[schemaName][:i], p.duplicateChecker[schemaName][i+1:]...)
					foundDuplicate = true
					break
				}
			}

			if !foundDuplicate {
				// If we didn't find a duplicate, add the object to the duplicate checker
				expiringObj := newExpiringMapAny(obj, p.app.config.keepDuplicatesFor)
				p.duplicateChecker[schemaName] = append(p.duplicateChecker[schemaName], expiringObj)

				// also add to storage engine
				p.app.storageEngine.addSafeObject(obj, schemaName)
			}
		}
	}

	p.app.storageEngine.printAllContents()

	return nil
}
