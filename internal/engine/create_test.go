package engine

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/shomali11/xsql"
	_ "github.com/sqlpipe/odbc"
)

var createTests = []setupTest{
	// PostgreSQL
	{
		name:        "postgresql wide_table create",
		source:      postgresqlTestSource,
		testQuery:   `create table wide_table(mybigint bigint, mybit bit(5), mybitvarying varbit, myboolean boolean, mybox box, mybytea bytea, mychar char(3), myvarchar varchar(100), mycidr cidr, mycircle circle, mydate date, mydoubleprecision double precision, myinet inet, myinteger integer, myinterval interval, myjson json, myjsonb jsonb, myline line, mylseg lseg, mymacaddr macaddr, mymoney money, mynumeric numeric(10,5), mypath path, mypg_lsn pg_lsn, mypoint point, mypolygon polygon, myreal real, mysmallint smallint, mytext text, mytime time, mytimetz timetz, mytimestamp timestamp, mytimestamptz timestamptz, mytsquery tsquery, mytsvector tsvector, myuuid uuid, myxml xml);`,
		expectedErr: "Stmt did not create a result set",
		checkQuery:  "select * from wide_table",
		checkResult: " mybigint | mybit | mybitvarying | myboolean | mybox | mybytea | mychar | myvarchar | mycidr | mycircle | mydate | mydoubleprecision | myinet | myinteger | myinterval | myjson | myjsonb | myline | mylseg | mymacaddr | mymoney | mynumeric | mypath | mypg_lsn | mypoint | mypolygon | myreal | mysmallint | mytext | mytime | mytimetz | mytimestamp | mytimestamptz | mytsquery | mytsvector | myuuid | myxml \n----------+-------+--------------+-----------+-------+---------+--------+-----------+--------+----------+--------+-------------------+--------+-----------+------------+--------+---------+--------+--------+-----------+---------+-----------+--------+----------+---------+-----------+--------+------------+--------+--------+----------+-------------+---------------+-----------+------------+--------+-------\n(0 rows)",
	},
	// MSSQL
	{
		name:        "mssql wide_table create",
		source:      mssqlTestSource,
		testQuery:   `create table wide_table (mybigint bigint, mybit bit, mydecimal decimal(10,5), myint int, mymoney money, mynumeric numeric(11,7), mysmallint smallint, mysmallmoney smallmoney, mytinyint tinyint, myfloat float, myreal real, mydate date, mydatetime2 datetime2, mydatetime datetime, mydatetimeoffset datetimeoffset, mysmalldatetime smalldatetime, mytime time, mychar char(3), myvarchar varchar(20), mytext text, mynchar nchar(3), mynvarchar nvarchar(20), myntext ntext, mybinary binary(3), myvarbinary varbinary(30), myuniqueidentifier uniqueidentifier, myxml xml);`,
		expectedErr: "Stmt did not create a result set",
		checkQuery:  "select * from wide_table",
		checkResult: " mybigint | mybit | mydecimal | myint | mymoney | mynumeric | mysmallint | mysmallmoney | mytinyint | myfloat | myreal | mydate | mydatetime2 | mydatetime | mydatetimeoffset | mysmalldatetime | mytime | mychar | myvarchar | mytext | mynchar | mynvarchar | myntext | mybinary | myvarbinary | myuniqueidentifier | myxml \n----------+-------+-----------+-------+---------+-----------+------------+--------------+-----------+---------+--------+--------+-------------+------------+------------------+-----------------+--------+--------+-----------+--------+---------+------------+---------+----------+-------------+--------------------+-------\n(0 rows)",
	},
	// MySQL
	{
		name:        "mysql wide_table create",
		source:      mysqlTestSource,
		testQuery:   `create table wide_table(myserial serial, mybit bit, mybit5 bit(5), mybit64 bit(64), mytinyint tinyint, mysmallint smallint, mymediumint mediumint, myint int, mybigint bigint, mydecimal decimal(10, 5), myfloat float, mydouble double, mydate date, mytime time, mydatetime datetime, mytimestamp timestamp, myyear year, mychar char(3), myvarchar varchar(200), mynchar nchar(3), mynvarchar nvarchar(200), mybinary binary(3), myvarbinary varbinary(200), mytinyblob tinyblob, mymediumblob mediumblob, myblob blob, mylongblob longblob, mytinytext tinytext, mytext text, mymediumtext mediumtext, mylongtext longtext, myenum ENUM('enumval1', 'enumval2'), myset SET('setval1', 'setval2'), myjson json);`,
		expectedErr: "Stmt did not create a result set",
		checkQuery:  "select * from wide_table",
		checkResult: " myserial | mybit | mybit5 | mybit64 | mytinyint | mysmallint | mymediumint | myint | mybigint | mydecimal | myfloat | mydouble | mydate | mytime | mydatetime | mytimestamp | myyear | mychar | myvarchar | mynchar | mynvarchar | mybinary | myvarbinary | mytinyblob | mymediumblob | myblob | mylongblob | mytinytext | mytext | mymediumtext | mylongtext | myenum | myset | myjson \n----------+-------+--------+---------+-----------+------------+-------------+-------+----------+-----------+---------+----------+--------+--------+------------+-------------+--------+--------+-----------+---------+------------+----------+-------------+------------+--------------+--------+------------+------------+--------+--------------+------------+--------+-------+--------\n(0 rows)",
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

			_, err := tt.source.Db.QueryContext(ctx, tt.testQuery)
			if err != nil && err.Error() != tt.expectedErr {
				t.Fatalf("unable to run test query, err: %v\n", err)
			}

			if tt.checkQuery != "" {
				rows, err := tt.source.Db.QueryContext(ctx, tt.checkQuery)
				if err != nil && err.Error() != tt.expectedCheckErr {
					t.Fatalf("\nwanted error:\n%#v\n\ngot error:\n%#v\n", tt.expectedCheckErr, err.Error())
				}

				if err == nil {
					defer rows.Close()
					result, err := xsql.Pretty(rows)
					if err != nil {
						t.Fatalf("unable to format query results, err: %v\n", err)
					}

					if !reflect.DeepEqual(result, tt.checkResult) {
						t.Fatalf("\n\nWanted:\n%#v\n\nGot:\n%#v", tt.checkResult, result)
					}
				}
			}
		})
	}
}
