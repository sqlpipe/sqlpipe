package engine

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/sqlpipe/odbc"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine/queries"
	"github.com/sqlpipe/sqlpipe/internal/engine/transfers"
)

// Test sources
var (
	postgresqlTestSource = data.Source{
		OdbcDsn: "Driver=PostgreSQL;Server=localhost;Port=5432;Database=postgres;Uid=postgres;Pwd=Mypass123;",
	}
)

type queryTest struct {
	name        string      // name of test
	source      data.Source //source to run query on
	testQuery   string      // query
	checkQuery  string      // query to test if the testQuery was successful
	checkResult string      // expected result of checkQuery
	expectedErr string      // if an error is expected, this will be the expected error
}

type transferTest struct {
	name        string
	transfer    data.Transfer
	checkQuery  string
	checkResult interface{}
	expectedErr string
}

var postgresqlSetupTests = []queryTest{
	// PostgreSQL Setup
	{
		name:        "postgresqlWideTableDrop",
		source:      postgresqlTestSource,
		testQuery:   "drop table if exists wide_table;",
		checkQuery:  "select * from wide_table",
		expectedErr: "SQLExecute: {42P01} ERROR: relation \"wide_table\" does not exist;\nError while preparing parameters",
	},
	{
		name:        "postgresqlWideTableCreate",
		source:      postgresqlTestSource,
		testQuery:   `create table wide_table(mybigint bigint, mybit bit(5), mybitvarying varbit, myboolean boolean, mybox box, mybytea bytea, mychar char(3), myvarchar varchar(100), mycidr cidr, mycircle circle, mydate date, mydoubleprecision double precision, myinet inet, myinteger integer, myinterval interval, myjson json, myjsonb jsonb, myline line, mylseg lseg, mymacaddr macaddr, mymoney money, mynumeric numeric(10,5), mypath path, mypg_lsn pg_lsn, mypoint point, mypolygon polygon, myreal real, mysmallint smallint, mytext text, mytime time, mytimetz timetz, mytimestamp timestamp, mytimestamptz timestamptz, mytsquery tsquery, mytsvector tsvector, myuuid uuid, myxml xml);`,
		checkQuery:  "select * from wide_table",
		checkResult: " mybigint | mybit | mybitvarying | myboolean | mybox | mybytea | mychar | myvarchar | mycidr | mycircle | mydate | mydoubleprecision | myinet | myinteger | myinterval | myjson | myjsonb | myline | mylseg | mymacaddr | mymoney | mynumeric | mypath | mypg_lsn | mypoint | mypolygon | myreal | mysmallint | mytext | mytime | mytimetz | mytimestamp | mytimestamptz | mytsquery | mytsvector | myuuid | myxml \n----------+-------+--------------+-----------+-------+---------+--------+-----------+--------+----------+--------+-------------------+--------+-----------+------------+--------+---------+--------+--------+-----------+---------+-----------+--------+----------+---------+-----------+--------+------------+--------+--------+----------+-------------+---------------+-----------+------------+--------+-------\n(0 rows)",
	},
	{
		name:        "postgresqlWideTableInsert",
		source:      postgresqlTestSource,
		testQuery:   `INSERT INTO wide_table(mybigint, mybit, mybitvarying, myboolean, mybox, mybytea, mychar, myvarchar, mycidr, mycircle, mydate, mydoubleprecision, myinet, myinteger, myinterval, myjson, myjsonb, myline, mylseg, mymacaddr, mymoney , mynumeric, mypath, mypg_lsn, mypoint, mypolygon, myreal, mysmallint, mytext, mytime, mytimetz, mytimestamp, mytimestamptz, mytsquery, mytsvector, myuuid, myxml) values (6514798382812790784, B'10001', B'1001', true, '(8,9), (1,3)', '\xAAAABBBB', 'abc', '"my"varch''ar,123@gmail.com', '192.168.100.128/25', '(( 1 , 5 ), 5)', '2014-01-10 20:14:54.140332'::date, 529.56218983375436, '192.168.100.128', 745910651, (timestamptz '2014-01-20 20:00:00 PST' - timestamptz '2014-01-10 10:00:00 PST'), '{"mykey": "this\"  ''is'' m,y val"}', '{"mykey": "this is my val"}', '{1, 5, 20}', '[(5, 4), (2, 1)]', '08:00:2b:01:02:03', '$35,244.33'::money, 449.82115, '[( 1, 4), (8, 7)]', '16/B374D848'::pg_lsn, '(5, 7)', '((5, 8), (6, 10), (7, 20))', 9673.1094, 24345, 'myte",xt123@gmail.com', '03:46:38.765594+05', '03:46:38.765594+05', '2014-01-10 10:05:04 PST', '2014-01-10 10:05:04 PST', 'fat & rat'::tsquery, 'a fat cat sat on a mat and ate a fat rat'::tsvector, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid, '<foo>bar</foo>'),(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);`,
		checkQuery:  "select * from wide_table",
		checkResult: "      mybigint       | mybit | mybitvarying | myboolean |    mybox    | mybytea | mychar |         myvarchar          |       mycidr       | mycircle  |        mydate        | mydoubleprecision |     myinet      | myinteger |    myinterval    |              myjson               |           myjsonb           |  myline  |    mylseg     |     mymacaddr     | mymoney  | mynumeric |    mypath     |  mypg_lsn   | mypoint |       mypolygon       |  myreal  | mysmallint |        mytext         |        mytime        |      mytimetz      |     mytimestamp      |    mytimestamptz     |   mytsquery   |                     mytsvector                     |                myuuid                |     myxml      \n---------------------+-------+--------------+-----------+-------------+---------+--------+----------------------------+--------------------+-----------+----------------------+-------------------+-----------------+-----------+------------------+-----------------------------------+-----------------------------+----------+---------------+-------------------+----------+-----------+---------------+-------------+---------+-----------------------+----------+------------+-----------------------+----------------------+--------------------+----------------------+----------------------+---------------+----------------------------------------------------+--------------------------------------+----------------\n 6514798382812790784 |  true |         1001 |         1 | (8,9),(1,3) |    \xaa\xaa\xbb\xbb |    abc | \"my\"varch'ar,123@gmail.com | 192.168.100.128/25 | <(1,5),5> | 2014-01-10T00:00:00Z | 529.5621898337544 | 192.168.100.128 | 745910651 | 10 days 10:00:00 | {\"mykey\": \"this\\\"  'is' m,y val\"} | {\"mykey\": \"this is my val\"} | {1,5,20} | [(5,4),(2,1)] | 08:00:2b:01:02:03 | 35244.33 | 449.82115 | [(1,4),(8,7)] | 16/B374D848 |   (5,7) | ((5,8),(6,10),(7,20)) | 9673.109 |      24345 | myte\",xt123@gmail.com | 0001-01-01T03:46:38Z | 03:46:38.765594+05 | 2014-01-10T10:05:04Z | 2014-01-10T18:05:04Z | 'fat' & 'rat' | 'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat' | a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 | <foo>bar</foo> \n                     |       |              |           |             |         |        |                            |                    |           |                      |                   |                 |           |                  |                                   |                             |          |               |                   |          |           |               |             |         |                       |          |            |                       |                      |                    |                      |                      |               |                                                    |                                      |                \n(2 rows)",
	},
	{
		name:        "postgresqlLoadTableDrop",
		source:      postgresqlTestSource,
		testQuery:   "drop table if exists load_table;",
		checkQuery:  "select * from load_table",
		expectedErr: "SQLExecute: {42P01} ERROR: relation \"load_table\" does not exist;\nError while preparing parameters",
	},
	{
		name:        "postgresqlLoadTableCreate",
		source:      postgresqlTestSource,
		testQuery:   `create table load_table(mybigint bigint, mybit bit(5), mybitvarying varbit, myboolean boolean, mybox box, mybytea bytea, mychar char(3), myvarchar varchar(100), mycidr cidr, mycircle circle, mydate date, mydoubleprecision double precision, myinet inet, myinteger integer, myinterval interval, myjson json, myjsonb jsonb, myline line, mylseg lseg, mymacaddr macaddr, mymoney money, mynumeric numeric(10,5), mypath path, mypg_lsn pg_lsn, mypoint point, mypolygon polygon, myreal real, mysmallint smallint, mytext text, mytime time, mytimetz timetz, mytimestamp timestamp, mytimestamptz timestamptz, mytsquery tsquery, mytsvector tsvector, myuuid uuid, myxml xml);`,
		checkQuery:  "select * from load_table",
		checkResult: " mybigint | mybit | mybitvarying | myboolean | mybox | mybytea | mychar | myvarchar | mycidr | mycircle | mydate | mydoubleprecision | myinet | myinteger | myinterval | myjson | myjsonb | myline | mylseg | mymacaddr | mymoney | mynumeric | mypath | mypg_lsn | mypoint | mypolygon | myreal | mysmallint | mytext | mytime | mytimetz | mytimestamp | mytimestamptz | mytsquery | mytsvector | myuuid | myxml \n----------+-------+--------------+-----------+-------+---------+--------+-----------+--------+----------+--------+-------------------+--------+-----------+------------+--------+---------+--------+--------+-----------+---------+-----------+--------+----------+---------+-----------+--------+------------+--------+--------+----------+-------------+---------------+-----------+------------+--------+-------\n(0 rows)",
	},
	{
		name:        "postgresqlLoadTableInsert",
		source:      postgresqlTestSource,
		testQuery:   `INSERT INTO load_table(mybigint, mybit, mybitvarying, myboolean, mybox, mybytea, mychar, myvarchar, mycidr, mycircle, mydate, mydoubleprecision, myinet, myinteger, myinterval, myjson, myjsonb, myline, mylseg, mymacaddr, mymoney , mynumeric, mypath, mypg_lsn, mypoint, mypolygon, myreal, mysmallint, mytext, mytime, mytimetz, mytimestamp, mytimestamptz, mytsquery, mytsvector, myuuid, myxml) select 6514798382812790784, B'10001', B'1001', true, '(8,9), (1,3)', '\xAAAABBBB', 'abc', '"my"varch''ar,123@gmail.com', '192.168.100.128/25', '(( 1 , 5 ), 5)', '2014-01-10 20:14:54.140332'::date, 529.56218983375436, '192.168.100.128', 745910651, (timestamptz '2014-01-20 20:00:00 PST' - timestamptz '2014-01-10 10:00:00 PST'), '{"mykey": "this\"  ''is'' m,y val"}', '{"mykey": "this is my val"}', '{1, 5, 20}', '[(5, 4), (2, 1)]', '08:00:2b:01:02:03', '$35,244.33'::money, 449.82115, '[( 1, 4), (8, 7)]', '16/B374D848'::pg_lsn, '(5, 7)', '((5, 8), (6, 10), (7, 20))', 9673.1094, 24345, 'myte",xt123@gmail.com', '03:46:38.765594+05', '03:46:38.765594+05', '2014-01-10 10:05:04 PST', '2014-01-10 10:05:04 PST', 'fat & rat'::tsquery, 'a fat cat sat on a mat and ate a fat rat'::tsvector, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid, '<foo>bar</foo>' from generate_series(1, 10000) seq;`,
		checkQuery:  "select count(*) from load_table",
		checkResult: " count \n-------\n 10000 \n(1 row)",
	},
}

var postgresqlTransferTests = []transferTest{
	// PostgreSQL Transfers
	{
		name: "postgresql2postgresql_wide",
		transfer: data.Transfer{
			Source:          postgresqlTestSource,
			Target:          data.Target{SystemType: "postgresql", OdbcDsn: "Driver=PostgreSQL;Server=localhost;Port=5432;Database=postgres;Uid=postgres;Pwd=Mypass123;", Schema: "public", Table: "postgresql_wide_table"},
			Query:           "select * from wide_table",
			DropTargetTable: true,
		},
		checkQuery:  "select * from postgresql_wide_table;",
		checkResult: "      mybigint       | mybit | mybitvarying | myboolean |    mybox    | mybytea  | mychar |         myvarchar          |       mycidr       | mycircle  |        mydate        | mydoubleprecision |     myinet      | myinteger |    myinterval    |              myjson               |           myjsonb           |  myline  |    mylseg     |     mymacaddr     | mymoney  | mynumeric |    mypath     |  mypg_lsn   | mypoint |       mypolygon       |  myreal  | mysmallint |        mytext         |        mytime        |      mytimetz      |     mytimestamp      |    mytimestamptz     |   mytsquery   |                     mytsvector                     |                myuuid                |     myxml      \n---------------------+-------+--------------+-----------+-------------+----------+--------+----------------------------+--------------------+-----------+----------------------+-------------------+-----------------+-----------+------------------+-----------------------------------+-----------------------------+----------+---------------+-------------------+----------+-----------+---------------+-------------+---------+-----------------------+----------+------------+-----------------------+----------------------+--------------------+----------------------+----------------------+---------------+----------------------------------------------------+--------------------------------------+----------------\n 6514798382812790784 |     1 |         1001 |         1 | (8,9),(1,3) | aaaabbbb |    abc | \"my\"varch'ar,123@gmail.com | 192.168.100.128/25 | <(1,5),5> | 2014-01-10T00:00:00Z | 529.5621898337544 | 192.168.100.128 | 745910651 | 10 days 10:00:00 | (\"mykey\": \"this\\\"  'is' m,y val\") | (\"mykey\": \"this is my val\") | (1,5,20) | [(5,4),(2,1)] | 08:00:2b:01:02:03 | 35244.33 | 449.82115 | [(1,4),(8,7)] | 16/B374D848 |   (5,7) | ((5,8),(6,10),(7,20)) | 9673.109 |      24345 | myte\",xt123@gmail.com | 0001-01-01T03:46:38Z | 03:46:38.765594+05 | 2014-01-10T10:05:04Z | 2014-01-10T18:05:04Z | 'fat' & 'rat' | 'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat' | a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 | <foo>bar</foo> \n                     |       |              |           |             |          |        |                            |                    |           |                      |                   |                 |           |                  |                                   |                             |          |               |                   |          |           |               |             |         |                       |          |            |                       |                      |                    |                      |                      |               |                                                    |                                      |                \n(2 rows)",
	},
}

func TestPostgresqlSetup(t *testing.T) {
	// t.Parallel()
	ctx := context.Background()

	sourceDb, err := sql.Open(
		"odbc",
		postgresqlTestSource.OdbcDsn,
	)
	if err != nil {
		t.Fatalf("unable to create postgresql source db. err:\n\n%v\n", err)
	}

	postgresqlTestSource.Db = sourceDb
	err = postgresqlTestSource.Db.Ping()
	if err != nil {
		t.Fatalf("unable to ping postgresql source db. err:\n\n%v\n", err)
	}

	// Loop over the test cases.
	for _, tt := range postgresqlSetupTests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := queries.RunQuery(ctx, data.Query{Source: postgresqlTestSource, Query: tt.testQuery})

			if err != nil && err.Error() != tt.expectedErr {

				t.Fatalf("unable to run test query. err:\n\n%v\n", err)
			}

			if tt.checkQuery != "" {
				result, _, err := queries.RunQuery(ctx, data.Query{Source: postgresqlTestSource, Query: tt.checkQuery})

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

func TestPostgresqlTransfers(t *testing.T) {
	// t.Parallel()
	ctx := context.Background()

	// Loop over the test cases.
	for _, tt := range postgresqlTransferTests {

		transfer := tt.transfer
		source := tt.transfer.Source
		target := tt.transfer.Target
		var err error

		source.Db, err = sql.Open(
			"odbc",
			source.OdbcDsn,
		)
		if err != nil {
			t.Fatalf("unable to create postgresql source db. err:\n\n%v\n", err)
		}

		err = source.Db.Ping()
		if err != nil {
			t.Fatalf("unable to ping postgresql source db. err:\n\n%v\n", err)
		}

		target.Db, err = sql.Open(
			"odbc",
			target.OdbcDsn,
		)
		if err != nil {
			t.Fatalf("unable to create postgresql target db. err:\n\n%v\n", err)
		}

		err = target.Db.Ping()
		if err != nil {
			t.Fatalf("unable to ping postgresql target db. err:\n\n%v\n", err)
		}

		transfer.Source = source
		transfer.Target = target

		t.Run(tt.name, func(t *testing.T) {
			// t.Parallel()

			_, _, err := transfers.RunTransfer(
				ctx,
				transfer,
			)

			if err != nil {
				t.Fatalf("unable to run transfer. err:\n\n%v\n", err)
			}

			if tt.checkQuery != "" {
				result, _, err := queries.RunQuery(ctx, data.Query{Source: transfer.Source, Query: tt.checkQuery})

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
