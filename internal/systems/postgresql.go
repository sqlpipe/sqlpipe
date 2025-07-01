package systems

import (
	"database/sql"
	"fmt"
)

type Postgresql struct {
	Connection *sql.DB
}

func newPostgresql(connectionInfo ConnectionInfo) (postgresql Postgresql, err error) {
	db, err := openConnectionPool(connectionInfo.Name, connectionInfo.ConnectionString, DriverPostgreSQL)
	if err != nil {
		return postgresql, fmt.Errorf("error opening postgresql db :: %v", err)
	}

	postgresql.Connection = db

	return postgresql, nil
}
