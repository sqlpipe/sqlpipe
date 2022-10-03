package engine

import (
	"database/sql"
	"testing"

	_ "github.com/sqlpipe/odbc"

	"github.com/sqlpipe/sqlpipe/internal/data"
)

var (
	// Sources
	postgresqlTestSource = data.Source{
		OdbcDsn: "Driver=PostgreSQL;Server=localhost;Port=5432;Database=postgres;Uid=postgres;Pwd=Mypass123;",
	}
	mssqlTestSource = data.Source{
		OdbcDsn: "DRIVER=MSSQL;SERVER=localhost;PORT=1433;Database=master;UID=sa;PWD=Mypass123;TDS_Version=7.0",
	}
	postgresqlTestTarget = data.Target{
		SystemType: "postgresql",
		OdbcDsn:    "Driver=PostgreSQL;Server=localhost;Port=5432;Database=postgres;Uid=postgres;Pwd=Mypass123;",
		Schema:     "public",
	}
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
