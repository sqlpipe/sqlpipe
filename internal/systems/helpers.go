package systems

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

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
