package engine

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/shomali11/xsql"
	_ "github.com/sqlpipe/odbc"
)

var dropTests = []setupTest{
	// PostgreSQL
	{
		name:        "postgresql wide_table drop",
		source:      postgresqlTestSource,
		testQuery:   `drop table if exists wide_table;`,
		expectedErr: "Stmt did not create a result set",
	},
	// MSSQL
	{
		name:        "mssql wide_table drop",
		source:      mssqlTestSource,
		testQuery:   `drop table if exists wide_table;`,
		expectedErr: "Stmt did not create a result set",
	},
}

func TestDrop(t *testing.T) {
	ctx := context.Background()
	var err error

	for _, tt := range dropTests {
		tt.source.Db, err = sql.Open(
			"odbc",
			tt.source.OdbcDsn,
		)
		if err != nil {
			t.Fatalf("unable to create db, err: %v\n", err)
		}

		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := tt.source.Db.QueryContext(ctx, tt.testQuery)
			if err != nil && err.Error() != tt.expectedErr {
				t.Fatalf("unable to run test query, err: %v\n", err)
			}

			if tt.checkQuery != "" {
				rows, err := tt.source.Db.QueryContext(ctx, tt.checkQuery)
				if err != nil && err.Error() != tt.expectedErr {
					t.Fatalf("\nwanted error:\n%#v\n\ngot error:\n%#v\n", tt.expectedErr, err.Error())
				}
				defer rows.Close()
				result, err := xsql.Pretty(rows)
				if err != nil {
					t.Fatalf("unable to format query results, err: %v\n", err)
				}

				if !reflect.DeepEqual(result, tt.checkResult) {
					t.Fatalf("\n\nWanted:\n%#v\n\nGot:\n%#v", tt.checkResult, result)
				}
			}
		})
	}
}
