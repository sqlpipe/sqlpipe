package engine

import (
	"reflect"
	"testing"

	"github.com/calmitchell617/sqlpipe/internal/data"
)

// Connection variables
var (
	postgresqlHostname       = "localhost"
	postgresqlTestConnection = data.Connection{
		DsType:   "postgresql",
		Username: "postgres",
		Password: "Mypass123",
		Hostname: postgresqlHostname,
		Port:     5432,
		DbName:   "postgres",
	}
)

type queryTest struct {
	name                  string
	connection            data.Connection
	testQuery             string
	checkQuery            string
	expectedErr           string
	expectedErrProperties map[string]string
	result                QueryResult
}

func TestRunQuery(t *testing.T) {
	// Define tests here
	tests := []queryTest{
		{
			name:                  "postgreSQLWideTableDrop",
			connection:            postgresqlTestConnection,
			testQuery:             "drop table if exists wide_table;",
			checkQuery:            "select * from wide_table",
			expectedErr:           "db.Query() threw an error",
			expectedErrProperties: postgreSQLWideTableDropErrProperties,
		},
		{
			name:       "postgresqlWideTableCreate",
			connection: postgresqlTestConnection,
			testQuery:  postgresqlWideTableCreateQuery,
			checkQuery: "select * from wide_table",
			result:     postgresqlWideTableCreateResult,
		},
	}

	// Loop over the test cases.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.testQuery != "" {
				err, _ := RunQuery(
					&data.Query{
						Query:      tt.testQuery,
						Connection: tt.connection,
					},
				)

				if err != nil {
					t.Fatal("unable to run test query")
				}
			}

			if tt.checkQuery != "" {
				dsConn := GetDs(tt.connection)
				queryResult, err, errProperties := standardGetFormattedResults(dsConn, tt.checkQuery)

				if err != nil && err.Error() != tt.expectedErr {
					t.Errorf("\nwanted error:\n%v\n\ngot:\n%v\n", tt.expectedErr, err)
				}

				if err != nil && !reflect.DeepEqual(errProperties, tt.expectedErrProperties) {
					t.Errorf("\nwanted errProperties:\n%v\n\ngot:\n%v", tt.expectedErrProperties, errProperties)
				}

				if !reflect.DeepEqual(queryResult, tt.result) {
					t.Errorf("\n\nWanted:\n%v\n\nGot:\n%v", queryResult, tt.result)
				}
			}
		})
	}
}
