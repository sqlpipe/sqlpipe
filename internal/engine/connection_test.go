package engine

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/sqlpipe/odbc"

	"github.com/sqlpipe/sqlpipe/internal/data"
)

type connectionTest struct {
	name   string
	source data.Source
}

var connectionTests = []connectionTest{
	{
		name:   "postgresql connection test",
		source: postgresqlTestSource,
	},
	{
		name:   "mssql connection test",
		source: mssqlTestSource,
	},
	{
		name:   "mysql connection test",
		source: mysqlTestSource,
	},
}

func TestConnections(t *testing.T) {
	var err error
	for _, tt := range connectionTests {
		t.Run(tt.name, func(t *testing.T) {
			// t.Parallel()
			tt.source.Db, err = sql.Open(
				"odbc",
				tt.source.OdbcDsn,
			)
			if err != nil {
				t.Fatalf(fmt.Sprintf("error runing sql.Open: %v", err.Error()))
			}

			err = tt.source.Db.Ping()
			if err != nil {
				t.Fatalf(fmt.Sprintf("error pinging DB: %v", err.Error()))
			}
		})
	}
}
