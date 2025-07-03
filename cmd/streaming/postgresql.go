package main

import (
	"database/sql"
	"fmt"
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
