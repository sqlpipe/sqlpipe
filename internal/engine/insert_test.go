package engine

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/shomali11/xsql"
	_ "github.com/sqlpipe/odbc"
)

var insertTests = []setupTest{
	// PostgreSQL
	{
		name:        "postgresql wide_table insert",
		source:      postgresqlTestSource,
		testQuery:   `insert into wide_table(mybigint, mybit, mybitvarying, myboolean, mybox, mybytea, mychar, myvarchar, mycidr, mycircle, mydate, mydoubleprecision, myinet, myinteger, myinterval, myjson, myjsonb, myline, mylseg, mymacaddr, mymoney , mynumeric, mypath, mypg_lsn, mypoint, mypolygon, myreal, mysmallint, mytext, mytime, mytimetz, mytimestamp, mytimestamptz, mytsquery, mytsvector, myuuid, myxml) values (6514798382812790784, B'10001', B'1001', true, '(8,9), (1,3)', '\xAAAABBBB', 'abc', '"my"varch''ar,123@gmail.com', '192.168.100.128/25', '(( 1 , 5 ), 5)', '2014-01-10 20:14:54.140332'::date, 529.56218983375436, '192.168.100.128', 745910651, (timestamptz '2014-01-20 20:00:00 PST' - timestamptz '2014-01-10 10:00:00 PST'), '{"mykey": "this\"  ''is'' m,y val"}', '{"mykey": "this is my val"}', '{1, 5, 20}', '[(5, 4), (2, 1)]', '08:00:2b:01:02:03', '$35,244.33'::money, 449.82115, '[( 1, 4), (8, 7)]', '16/B374D848'::pg_lsn, '(5, 7)', '((5, 8), (6, 10), (7, 20))', 9673.1094, 24345, 'myte",xt123@gmail.com', '03:46:38.765594+05', '03:46:38.765594+05', '2014-01-10 10:05:04 PST', '2014-01-10 10:05:04 PST', 'fat & rat'::tsquery, 'a fat cat sat on a mat and ate a fat rat'::tsvector, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid, '<foo>bar</foo>'),(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);`,
		expectedErr: "Stmt did not create a result set",
		checkQuery:  "select * from wide_table",
		checkResult: "      mybigint       | mybit | mybitvarying | myboolean |    mybox    | mybytea | mychar |         myvarchar          |       mycidr       | mycircle  |        mydate        | mydoubleprecision |     myinet      | myinteger |    myinterval    |              myjson               |           myjsonb           |  myline  |    mylseg     |     mymacaddr     | mymoney  | mynumeric |    mypath     |  mypg_lsn   | mypoint |       mypolygon       |  myreal  | mysmallint |        mytext         |        mytime        |      mytimetz      |     mytimestamp      |    mytimestamptz     |   mytsquery   |                     mytsvector                     |                myuuid                |     myxml      \n---------------------+-------+--------------+-----------+-------------+---------+--------+----------------------------+--------------------+-----------+----------------------+-------------------+-----------------+-----------+------------------+-----------------------------------+-----------------------------+----------+---------------+-------------------+----------+-----------+---------------+-------------+---------+-----------------------+----------+------------+-----------------------+----------------------+--------------------+----------------------+----------------------+---------------+----------------------------------------------------+--------------------------------------+----------------\n 6514798382812790784 |  true |         1001 |         1 | (8,9),(1,3) |    \xaa\xaa\xbb\xbb |    abc | \"my\"varch'ar,123@gmail.com | 192.168.100.128/25 | <(1,5),5> | 2014-01-10T00:00:00Z | 529.5621898337544 | 192.168.100.128 | 745910651 | 10 days 10:00:00 | {\"mykey\": \"this\\\"  'is' m,y val\"} | {\"mykey\": \"this is my val\"} | {1,5,20} | [(5,4),(2,1)] | 08:00:2b:01:02:03 | 35244.33 | 449.82115 | [(1,4),(8,7)] | 16/B374D848 |   (5,7) | ((5,8),(6,10),(7,20)) | 9673.109 |      24345 | myte\",xt123@gmail.com | 0001-01-01T03:46:38Z | 03:46:38.765594+05 | 2014-01-10T10:05:04Z | 2014-01-10T18:05:04Z | 'fat' & 'rat' | 'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat' | a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 | <foo>bar</foo> \n                     |       |              |           |             |         |        |                            |                    |           |                      |                   |                 |           |                  |                                   |                             |          |               |                   |          |           |               |             |         |                       |          |            |                       |                      |                    |                      |                      |               |                                                    |                                      |                \n(2 rows)",
	},
	// MSSQL
	{
		name:        "mssql wide_table insert",
		source:      mssqlTestSource,
		testQuery:   `insert into wide_table (mybigint, mybit, mydecimal, myint, mymoney, mynumeric, mysmallint, mysmallmoney, mytinyint, myfloat, myreal, mydate, mydatetime2, mydatetime, mydatetimeoffset, mysmalldatetime, mytime, mychar, myvarchar, mytext, mynchar, mynvarchar, myntext, mybinary, myvarbinary, myuniqueidentifier, myxml) values(435345, 1, 324.43, 54, 43.21, 54.33, 12, 22.10, 4, 45.5, 47.7, '2013-10-12', CAST('2005-06-12 11:40:17.632' AS datetime2), CAST('2005-06-12 11:40:17.632' AS datetime), CAST('2005-06-12 11:40:17.632 +01:00' AS datetimeoffset), CAST('2005-06-12 11:40:00' AS smalldatetime), CAST('11:40:12.543654' AS time), 'yoo', 'gday guvna', 'omg have you hea''rd" a,bout the latest craze that the people are talking about?', 'yoo', 'gday guvna', 'omg have you heard about the latest craze that the people are talking about?', 101, 100001, N'6F9619FF-8B86-D011-B42D-00C04FC964FF','<foo>bar</foo>'),(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);`,
		expectedErr: "Stmt did not create a result set",
		checkQuery:  "select * from wide_table",
		checkResult: " mybigint | mybit | mydecimal | myint | mymoney | mynumeric | mysmallint | mysmallmoney | mytinyint | myfloat |      myreal       |   mydate   |         mydatetime2         |        mydatetime        |          mydatetimeoffset          |   mysmalldatetime    |      mytime      | mychar | myvarchar  |                                     mytext                                      | mynchar | mynvarchar |                                   myntext                                    | mybinary | myvarbinary |          myuniqueidentifier          |     myxml      \n----------+-------+-----------+-------+---------+-----------+------------+--------------+-----------+---------+-------------------+------------+-----------------------------+--------------------------+------------------------------------+----------------------+------------------+--------+------------+---------------------------------------------------------------------------------+---------+------------+------------------------------------------------------------------------------+----------+-------------+--------------------------------------+----------------\n   435345 |  true |    324.43 |    54 |   43.21 |     54.33 |         12 |         22.1 |         4 |    45.5 | 47.70000076293945 | 2013-10-12 | 2005-06-12 11:40:17.6320000 | 2005-06-12T11:40:17.633Z | 2005-06-12 11:40:17.6320000 +01:00 | 2005-06-12T11:40:00Z | 11:40:12.5436540 |    yoo | gday guvna | omg have you hea'rd\" a,bout the latest craze that the people are talking about? |     yoo | gday guvna | omg have you heard about the latest craze that the people are talking about? |      \x00\x00e |        \x00\x01\x86\xa1 | 6f9619ff-8b86-d011-b42d-00c04fc964ff | <foo>bar</foo> \n          |       |           |       |         |           |            |              |           |         |                   |            |                             |                          |                                    |                      |                  |        |            |                                                                                 |         |            |                                                                              |          |             |                                      |                \n(2 rows)",
	},
	// MySQL
	{
		name:        "mysql wide_table insert",
		source:      mysqlTestSource,
		testQuery:   `insert into wide_table (mybit, mybit5, mybit64, mytinyint, mysmallint, mymediumint, myint, mybigint, mydecimal, myfloat, mydouble, mydate, mytime, mydatetime, mytimestamp, myyear, mychar, myvarchar, mynchar, mynvarchar, mybinary, myvarbinary, mytinyblob, mymediumblob, myblob, mylongblob, mytinytext, mytext, mymediumtext, mylongtext, myenum, myset, myjson) VALUES (1, b'01010', b'1111111111111111111111111111111111111111111111111111111111111111', 2, 5, 50, 4595435, 392809438543, 30.5, 45.9, 54.3, '2009-05-28', '14:23:54.105302', '2010-10-24 20:52:51.969491', '1989-02-22 3:17:21.243061', 1905, 'chr', 'my varchar ''st"ri,ng wheeeee', 'ncr', 'my nvarchar string wheeeee', 'bnr', 'my binary string wahooooo', 'blob city bb', 'blob city bb', 'blob city bb', 'blob city bb', 'text city bb', 'text city bb', 'text city bb', 'text city bb', 'enumval1', 'setval1', '{"mykey": "this is\\" m\\"y, ''val''"}'),(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);`,
		expectedErr: "Stmt did not create a result set",
		checkQuery:  "select * from wide_table",
		checkResult: " myserial | mybit | mybit5 | mybit64  | mytinyint | mysmallint | mymediumint |  myint  |   mybigint   | mydecimal | myfloat | mydouble |        mydate        |        mytime        |      mydatetime      |     mytimestamp      | myyear | mychar |          myvarchar           | mynchar |         mynvarchar         | mybinary |        myvarbinary        |  mytinyblob  | mymediumblob |    myblob    |  mylongblob  |  mytinytext  |    mytext    | mymediumtext |  mylongtext  |  myenum  |  myset  |               myjson               \n----------+-------+--------+----------+-----------+------------+-------------+---------+--------------+-----------+---------+----------+----------------------+----------------------+----------------------+----------------------+--------+--------+------------------------------+---------+----------------------------+----------+---------------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+----------+---------+------------------------------------\n        1 |  true |      \n | \xff\xff\xff\xff\xff\xff\xff\xff |         2 |          5 |          50 | 4595435 | 392809438543 |      30.5 |    45.9 |     54.3 | 2009-05-28T00:00:00Z | 0001-01-01T14:23:54Z | 2010-10-24T20:52:52Z | 1989-02-22T03:17:21Z |   1905 |    chr | my varchar 'st\"ri,ng wheeeee |     ncr | my nvarchar string wheeeee |      bnr | my binary string wahooooo | blob city bb | blob city bb | blob city bb | blob city bb | text city bb | text city bb | text city bb | text city bb | enumval1 | setval1 | {\"mykey\": \"this is\\\" m\\\"y, 'val'\"} \n        2 |       |        |          |           |            |             |         |              |           |         |          |                      |                      |                      |                      |        |        |                              |         |                            |          |                           |              |              |              |              |              |              |              |              |          |         |                                    \n(2 rows)",
	},
	// Snowflake
	{
		name:        "snowflake wide_table insert",
		source:      snowflakeTestSource,
		testQuery:   `insert into wide_table (mynumber, myint, myfloat, myvarchar, mybinary, myboolean, mydate, mytime, mytimestamp_ltz, mytimestamp_ntz, mytimestamp_tz, myvariant, myobject, myarray, mygeography) select column1, column2, column3, column4, column5, column6, column7, column8, column9, column10, column11, parse_json(column12), parse_json(column13), parse_json(column14), column15 from values (25.5, 22, 42.5, 'hellooooo h''er"es ,my varchar value', to_binary('0011'), true, '2000-10-15', '23:54:01', '2000-10-15 23:54:01.345673', '2000-10-15 23:54:01.345673', '2000-10-15 23:54:01.345673 +0100', '{"mykey": "this is \\"my'' v,al"}', '{"key3": "value3", "key4": "value4"}', '[true, 1, -1.2e-3, "Abc", ["x","y"], {"a":1}]', 'POINT(-122.35 37.55)'),(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);`,
		expectedErr: "Stmt did not create a result set",
		checkQuery:  "select * from wide_table",
		checkResult: " MYNUMBER | MYINT | MYFLOAT |              MYVARCHAR              | MYBINARY | MYBOOLEAN |        MYDATE        |        MYTIME        |       MYTIMESTAMP_LTZ       |       MYTIMESTAMP_NTZ       |       MYTIMESTAMP_TZ        |              MYVARIANT              |                  MYOBJECT                  |                                             MYARRAY                                              |                             MYGEOGRAPHY                              \n----------+-------+---------+-------------------------------------+----------+-----------+----------------------+----------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------------+--------------------------------------------+--------------------------------------------------------------------------------------------------+----------------------------------------------------------------------\n     25.5 |    22 |    42.5 | hellooooo h'er\"es ,my varchar value |       \x00\x11 |      true | 2000-10-15T00:00:00Z | 0001-01-01T23:54:01Z | 2000-10-16T06:54:01.345673Z | 2000-10-15T23:54:01.345673Z | 2000-10-15T22:54:01.345673Z | {\n  \"mykey\": \"this is \\\"my' v,al\"\n} | {\n  \"key3\": \"value3\",\n  \"key4\": \"value4\"\n} | [\n  true,\n  1,\n  -1.200000000000000e-03,\n  \"Abc\",\n  [\n    \"x\",\n    \"y\"\n  ],\n  {\n    \"a\": 1\n  }\n] | {\n  \"coordinates\": [\n    -122.35,\n    37.55\n  ],\n  \"type\": \"Point\"\n} \n          |       |         |                                     |          |           |                      |                      |                             |                             |                             |                                     |                                            |                                                                                                  |                                                                      \n(2 rows)",
	},
}

func TestInsert(t *testing.T) {
	ctx := context.Background()
	var err error

	for _, tt := range insertTests {
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
			_, err = tt.source.Db.QueryContext(ctx, tt.testQuery)

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
