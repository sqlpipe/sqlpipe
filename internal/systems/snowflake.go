package systems

import (
	"database/sql"
	"fmt"

	_ "github.com/snowflakedb/gosnowflake"
)

type Snowflake struct {
	Connection *sql.DB
}

func newSnowflake(systemInfo SystemInfo) (snowflake Snowflake, err error) {
	db, err := openConnectionPool(systemInfo.Name, systemInfo.ConnectionString, DriverSnowflake)
	if err != nil {
		return snowflake, fmt.Errorf("error opening snowflake db :: %v", err)
	}

	snowflake.Connection = db

	return snowflake, nil
}
