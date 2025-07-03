package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type envelope map[string]any

func (app *application) writeJSON(w http.ResponseWriter, status int, data envelope, headers http.Header) error {
	js, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		return err
	}

	js = append(js, '\n')

	for key, value := range headers {
		w.Header()[key] = value
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(js)

	return nil
}

func (app *application) healthcheckHandler(w http.ResponseWriter, r *http.Request) {
	env := envelope{
		"status": "available",
		"system_info": map[string]string{
			"version": version,
		},
	}

	err := app.writeJSON(w, http.StatusOK, env, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

func openConnectionPool(name, connectionString, driverName string) (connectionPool *sql.DB, err error) {

	connectionPool, err = sql.Open(driverName, connectionString)
	if err != nil {
		return nil, fmt.Errorf("error opening connection to %v :: %v", name, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = connectionPool.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("error pinging %v :: %v", name, err)
	}

	return connectionPool, nil
}

// ValidateWithSchema looks up schemas[name] and runs Validate on your payload.
// Returns a *jsonschema.ValidationError if it fails.
func ValidateSchema(
	name string,
	rawPayload []byte,
	compiledSchemas map[string]*jsonschema.Schema,
) error {
	sch, ok := compiledSchemas[name]
	if !ok {
		return fmt.Errorf("no schema named %q loaded", name)
	}

	var v interface{}
	if err := json.Unmarshal(rawPayload, &v); err != nil {
		return fmt.Errorf("invalid JSON payload: %w", err)
	}
	return sch.Validate(v)
}
