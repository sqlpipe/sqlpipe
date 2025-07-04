package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
)

type Postgresql struct {
	Connection *sql.DB
}

func newPostgresql(systemInfo SystemInfo) (postgresql Postgresql, err error) {
	db, err := openConnectionPool(systemInfo.Name, systemInfo.ConnectionString, DriverPostgreSQL)
	if err != nil {
		return postgresql, fmt.Errorf("error opening postgresql db :: %v", err)
	}

	postgresql.Connection = db

	return postgresql, nil
}

func (p Postgresql) handleWebhook(w http.ResponseWriter, r *http.Request) {
	// PostgreSQL will not send us webhooks, so this is a no-op
}

func (p Postgresql) mapProperties(obj map[string]interface{}) (map[string]interface{}, error) {
	return nil, errors.New("not implemented for postgresql yet")
}
