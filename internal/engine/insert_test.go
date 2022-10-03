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

var insertTests = []setupTest{
	// PostgreSQL
	{
		name:        "postgresql wide_table insert",
		source:      postgresqlTestSource,
		testQuery:   `INSERT INTO wide_table(mybigint, mybit, mybitvarying, myboolean, mybox, mybytea, mychar, myvarchar, mycidr, mycircle, mydate, mydoubleprecision, myinet, myinteger, myinterval, myjson, myjsonb, myline, mylseg, mymacaddr, mymoney , mynumeric, mypath, mypg_lsn, mypoint, mypolygon, myreal, mysmallint, mytext, mytime, mytimetz, mytimestamp, mytimestamptz, mytsquery, mytsvector, myuuid, myxml) values (6514798382812790784, B'10001', B'1001', true, '(8,9), (1,3)', '\xAAAABBBB', 'abc', '"my"varch''ar,123@gmail.com', '192.168.100.128/25', '(( 1 , 5 ), 5)', '2014-01-10 20:14:54.140332'::date, 529.56218983375436, '192.168.100.128', 745910651, (timestamptz '2014-01-20 20:00:00 PST' - timestamptz '2014-01-10 10:00:00 PST'), '{"mykey": "this\"  ''is'' m,y val"}', '{"mykey": "this is my val"}', '{1, 5, 20}', '[(5, 4), (2, 1)]', '08:00:2b:01:02:03', '$35,244.33'::money, 449.82115, '[( 1, 4), (8, 7)]', '16/B374D848'::pg_lsn, '(5, 7)', '((5, 8), (6, 10), (7, 20))', 9673.1094, 24345, 'myte",xt123@gmail.com', '03:46:38.765594+05', '03:46:38.765594+05', '2014-01-10 10:05:04 PST', '2014-01-10 10:05:04 PST', 'fat & rat'::tsquery, 'a fat cat sat on a mat and ate a fat rat'::tsvector, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid, '<foo>bar</foo>'),(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);`,
		checkQuery:  "select * from wide_table",
		checkResult: "      mybigint       | mybit | mybitvarying | myboolean |    mybox    | mybytea | mychar |         myvarchar          |       mycidr       | mycircle  |        mydate        | mydoubleprecision |     myinet      | myinteger |    myinterval    |              myjson               |           myjsonb           |  myline  |    mylseg     |     mymacaddr     | mymoney  | mynumeric |    mypath     |  mypg_lsn   | mypoint |       mypolygon       |  myreal  | mysmallint |        mytext         |        mytime        |      mytimetz      |     mytimestamp      |    mytimestamptz     |   mytsquery   |                     mytsvector                     |                myuuid                |     myxml      \n---------------------+-------+--------------+-----------+-------------+---------+--------+----------------------------+--------------------+-----------+----------------------+-------------------+-----------------+-----------+------------------+-----------------------------------+-----------------------------+----------+---------------+-------------------+----------+-----------+---------------+-------------+---------+-----------------------+----------+------------+-----------------------+----------------------+--------------------+----------------------+----------------------+---------------+----------------------------------------------------+--------------------------------------+----------------\n 6514798382812790784 |  true |         1001 |         1 | (8,9),(1,3) |    \xaa\xaa\xbb\xbb |    abc | \"my\"varch'ar,123@gmail.com | 192.168.100.128/25 | <(1,5),5> | 2014-01-10T00:00:00Z | 529.5621898337544 | 192.168.100.128 | 745910651 | 10 days 10:00:00 | {\"mykey\": \"this\\\"  'is' m,y val\"} | {\"mykey\": \"this is my val\"} | {1,5,20} | [(5,4),(2,1)] | 08:00:2b:01:02:03 | 35244.33 | 449.82115 | [(1,4),(8,7)] | 16/B374D848 |   (5,7) | ((5,8),(6,10),(7,20)) | 9673.109 |      24345 | myte\",xt123@gmail.com | 0001-01-01T03:46:38Z | 03:46:38.765594+05 | 2014-01-10T10:05:04Z | 2014-01-10T18:05:04Z | 'fat' & 'rat' | 'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat' | a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 | <foo>bar</foo> \n                     |       |              |           |             |         |        |                            |                    |           |                      |                   |                 |           |                  |                                   |                             |          |               |                   |          |           |               |             |         |                       |          |            |                       |                      |                    |                      |                      |               |                                                    |                                      |                \n(2 rows)",
	},
	// MSSQL
	{
		name:        "mssql wide_table insert",
		source:      mssqlTestSource,
		testQuery:   `insert into wide_table (mybigint, mybit, mydecimal, myint, mymoney, mynumeric, mysmallint, mysmallmoney, mytinyint, myfloat, myreal, mydate, mydatetime2, mydatetime, mydatetimeoffset, mysmalldatetime, mytime, mychar, myvarchar, mytext, mynchar, mynvarchar, myntext, mybinary, myvarbinary, myuniqueidentifier, myxml) values(435345, 1, 324.43, 54, 43.21, 54.33, 12, 22.10, 4, 45.5, 47.7, '2013-10-12', CAST('2005-06-12 11:40:17.632' AS datetime2), CAST('2005-06-12 11:40:17.632' AS datetime), CAST('2005-06-12 11:40:17.632 +01:00' AS datetimeoffset), CAST('2005-06-12 11:40:00' AS smalldatetime), CAST('11:40:12.543654' AS time), 'yoo', 'gday guvna', 'omg have you hea''rd" a,bout the latest craze that the people are talking about?', 'yoo', 'gday guvna', 'omg have you heard about the latest craze that the people are talking about?', 101, 100001, N'6F9619FF-8B86-D011-B42D-00C04FC964FF','<foo>bar</foo>'),(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);`,
		checkQuery:  "select * from wide_table",
		checkResult: " mybigint | mybit | mydecimal | myint | mymoney | mynumeric | mysmallint | mysmallmoney | mytinyint | myfloat |      myreal       |   mydate   |         mydatetime2         |        mydatetime        |          mydatetimeoffset          |   mysmalldatetime    |      mytime      | mychar | myvarchar  |                                     mytext                                      | mynchar | mynvarchar |                                   myntext                                    | mybinary | myvarbinary |          myuniqueidentifier          |     myxml      \n----------+-------+-----------+-------+---------+-----------+------------+--------------+-----------+---------+-------------------+------------+-----------------------------+--------------------------+------------------------------------+----------------------+------------------+--------+------------+---------------------------------------------------------------------------------+---------+------------+------------------------------------------------------------------------------+----------+-------------+--------------------------------------+----------------\n   435345 |  true |    324.43 |    54 |   43.21 |     54.33 |         12 |         22.1 |         4 |    45.5 | 47.70000076293945 | 2013-10-12 | 2005-06-12 11:40:17.6320000 | 2005-06-12T11:40:17.633Z | 2005-06-12 11:40:17.6320000 +01:00 | 2005-06-12T11:40:00Z | 11:40:12.5436540 |    yoo | gday guvna | omg have you hea'rd\" a,bout the latest craze that the people are talking about? |     yoo | gday guvna | omg have you heard about the latest craze that the people are talking about? |      \x00\x00e |        \x00\x01\x86\xa1 | 6f9619ff-8b86-d011-b42d-00c04fc964ff | <foo>bar</foo> \n          |       |           |       |         |           |            |              |           |         |                   |            |                             |                          |                                    |                      |                  |        |            |                                                                                 |         |            |                                                                              |          |             |                                      |                \n(2 rows)",
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
