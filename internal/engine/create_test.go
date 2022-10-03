package engine

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/sqlpipe/odbc"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine/queries"
)

type setupTest struct {
	name        string      // name of test
	source      data.Source //source to run query on
	testQuery   string      // query
	checkQuery  string      // query to test if the testQuery was successful
	checkResult string      // expected result of checkQuery
	expectedErr string      // if an error is expected, this will be the expected error
}

var createTests = []setupTest{
	// PostgreSQL
	{
		name:        "postgresql wide_table create",
		source:      postgresqlTestSource,
		testQuery:   `create table wide_table(mybigint bigint, mybit bit(5), mybitvarying varbit, myboolean boolean, mybox box, mybytea bytea, mychar char(3), myvarchar varchar(100), mycidr cidr, mycircle circle, mydate date, mydoubleprecision double precision, myinet inet, myinteger integer, myinterval interval, myjson json, myjsonb jsonb, myline line, mylseg lseg, mymacaddr macaddr, mymoney money, mynumeric numeric(10,5), mypath path, mypg_lsn pg_lsn, mypoint point, mypolygon polygon, myreal real, mysmallint smallint, mytext text, mytime time, mytimetz timetz, mytimestamp timestamp, mytimestamptz timestamptz, mytsquery tsquery, mytsvector tsvector, myuuid uuid, myxml xml);`,
		checkQuery:  "select * from wide_table",
		checkResult: " mybigint | mybit | mybitvarying | myboolean | mybox | mybytea | mychar | myvarchar | mycidr | mycircle | mydate | mydoubleprecision | myinet | myinteger | myinterval | myjson | myjsonb | myline | mylseg | mymacaddr | mymoney | mynumeric | mypath | mypg_lsn | mypoint | mypolygon | myreal | mysmallint | mytext | mytime | mytimetz | mytimestamp | mytimestamptz | mytsquery | mytsvector | myuuid | myxml \n----------+-------+--------------+-----------+-------+---------+--------+-----------+--------+----------+--------+-------------------+--------+-----------+------------+--------+---------+--------+--------+-----------+---------+-----------+--------+----------+---------+-----------+--------+------------+--------+--------+----------+-------------+---------------+-----------+------------+--------+-------\n(0 rows)",
	},
	// MSSQL
	{
		name:        "mssqlwide_table create",
		source:      mssqlTestSource,
		testQuery:   `create table wide_table (mybigint bigint, mybit bit, mydecimal decimal(10,5), myint int, mymoney money, mynumeric numeric(11,7), mysmallint smallint, mysmallmoney smallmoney, mytinyint tinyint, myfloat float, myreal real, mydate date, mydatetime2 datetime2, mydatetime datetime, mydatetimeoffset datetimeoffset, mysmalldatetime smalldatetime, mytime time, mychar char(3), myvarchar varchar(20), mytext text, mynchar nchar(3), mynvarchar nvarchar(20), myntext ntext, mybinary binary(3), myvarbinary varbinary(30), myuniqueidentifier uniqueidentifier, myxml xml);`,
		checkQuery:  "select * from wide_table",
		checkResult: " mybigint | mybit | mydecimal | myint | mymoney | mynumeric | mysmallint | mysmallmoney | mytinyint | myfloat | myreal | mydate | mydatetime2 | mydatetime | mydatetimeoffset | mysmalldatetime | mytime | mychar | myvarchar | mytext | mynchar | mynvarchar | myntext | mybinary | myvarbinary | myuniqueidentifier | myxml \n----------+-------+-----------+-------+---------+-----------+------------+--------------+-----------+---------+--------+--------+-------------+------------+------------------+-----------------+--------+--------+-----------+--------+---------+------------+---------+----------+-------------+--------------------+-------\n(0 rows)",
	},
}

func TestCreate(t *testing.T) {
	ctx := context.Background()
	var err error

	for _, tt := range createTests {
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
			_, _, err = queries.RunQuery(ctx, data.Query{Source: tt.source, Query: tt.testQuery})

			if err != nil && err.Error() != tt.expectedErr {
				t.Fatalf("unable to run test query, err: %v\n", err)
			}

			if tt.checkQuery != "" {
				result, _, err := queries.RunQuery(ctx, data.Query{Source: tt.source, Query: tt.checkQuery})

				if err != nil && err.Error() != tt.expectedErr {
					t.Fatalf("\nwanted error:\n%#v\n\ngot error:\n%#v\n", tt.expectedErr, err.Error())
				}

				if !reflect.DeepEqual(result, tt.checkResult) {
					t.Fatalf("\n\nWanted:\n%#v\n\nGot:\n%#v", tt.checkResult, result)
				}
			}
		})
	}
}
