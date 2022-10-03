package engine

import (
	"database/sql"
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
}

func TestConnections(t *testing.T) {
	var err error
	for _, tt := range connectionTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.source.Db, err = sql.Open(
				"odbc",
				tt.source.OdbcDsn,
			)
			if err != nil {
				t.Fatalf(err.Error())
			}

			err = tt.source.Db.Ping()
			if err != nil {
				t.Fatalf(err.Error())
			}
		})
	}
}
