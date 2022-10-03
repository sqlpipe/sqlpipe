package engine

import (
	"context"
	"reflect"
	"testing"

	_ "github.com/sqlpipe/odbc"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine/queries"
	"github.com/sqlpipe/sqlpipe/internal/engine/transfers"
)

type transferTest struct {
	name        string
	transfer    data.Transfer
	targetTable string
	checkQuery  string
	checkResult interface{}
	expectedErr string
}

var transferTests = []transferTest{
	// PostgreSQL
	{
		name: "postgresql2postgresql_wide",
		transfer: data.Transfer{
			Source:          postgresqlTestSource,
			Target:          postgresqlTestTarget,
			Query:           "select * from wide_table",
			DropTargetTable: true,
		},
		targetTable: "postgresql_wide_table",
		checkQuery:  "select * from postgresql_wide_table;",
		checkResult: "      mybigint       | mybit | mybitvarying | myboolean |    mybox    | mybytea  | mychar |         myvarchar          |       mycidr       | mycircle  |        mydate        | mydoubleprecision |     myinet      | myinteger |    myinterval    |              myjson               |           myjsonb           |  myline  |    mylseg     |     mymacaddr     | mymoney  | mynumeric |    mypath     |  mypg_lsn   | mypoint |       mypolygon       |  myreal  | mysmallint |        mytext         |        mytime        |      mytimetz      |     mytimestamp      |    mytimestamptz     |   mytsquery   |                     mytsvector                     |                myuuid                |     myxml      \n---------------------+-------+--------------+-----------+-------------+----------+--------+----------------------------+--------------------+-----------+----------------------+-------------------+-----------------+-----------+------------------+-----------------------------------+-----------------------------+----------+---------------+-------------------+----------+-----------+---------------+-------------+---------+-----------------------+----------+------------+-----------------------+----------------------+--------------------+----------------------+----------------------+---------------+----------------------------------------------------+--------------------------------------+----------------\n 6514798382812790784 |     1 |         1001 |         1 | (8,9),(1,3) | aaaabbbb |    abc | \"my\"varch'ar,123@gmail.com | 192.168.100.128/25 | <(1,5),5> | 2014-01-10T00:00:00Z | 529.5621898337544 | 192.168.100.128 | 745910651 | 10 days 10:00:00 | (\"mykey\": \"this\\\"  'is' m,y val\") | (\"mykey\": \"this is my val\") | (1,5,20) | [(5,4),(2,1)] | 08:00:2b:01:02:03 | 35244.33 | 449.82115 | [(1,4),(8,7)] | 16/B374D848 |   (5,7) | ((5,8),(6,10),(7,20)) | 9673.109 |      24345 | myte\",xt123@gmail.com | 0001-01-01T03:46:38Z | 03:46:38.765594+05 | 2014-01-10T10:05:04Z | 2014-01-10T18:05:04Z | 'fat' & 'rat' | 'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat' | a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 | <foo>bar</foo> \n                     |       |              |           |             |          |        |                            |                    |           |                      |                   |                 |           |                  |                                   |                             |          |               |                   |          |           |               |             |         |                       |          |            |                       |                      |                    |                      |                      |               |                                                    |                                      |                \n(2 rows)",
	},
}

func TestTransfers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, tt := range transferTests {

		transfer := tt.transfer
		source := tt.transfer.Source
		target := tt.transfer.Target
		target.Table = tt.targetTable
		transfer.Source = source
		transfer.Target = target

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

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
