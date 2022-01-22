package engine

import (
	"reflect"
	"testing"

	"github.com/calmitchell617/sqlpipe/internal/data"
)

type queryTest struct {
	name                  string
	connection            data.Connection
	testQuery             string
	checkQuery            string
	checkResult           QueryResult
	expectedErr           string
	expectedErrProperties map[string]string
}

type transferTest struct {
	name                  string
	source                data.Connection
	target                data.Connection
	overwrite             bool
	targetSchema          string
	targetTable           string
	transferQuery         string
	checkQuery            string
	checkResult           QueryResult
	expectedErr           string
	expectedErrProperties map[string]string
}

func TestRunQuery(t *testing.T) {
	// Define tests here
	tests := []queryTest{
		// PostgreSQL Setup
		{
			name:                  "postgresqlWideTableDrop",
			connection:            postgresqlTestConnection,
			testQuery:             "drop table if exists wide_table;",
			checkQuery:            "select * from wide_table",
			expectedErr:           "db.Query() threw an error",
			expectedErrProperties: postgresqlWideTableDropErrProperties,
		},
		{
			name:        "postgresqlWideTableCreate",
			connection:  postgresqlTestConnection,
			testQuery:   postgresqlWideTableCreateQuery,
			checkQuery:  "select * from wide_table",
			checkResult: postgresqlWideTableCreateResult,
		},
		{
			name:        "postgresqlWideTableInsert",
			connection:  postgresqlTestConnection,
			testQuery:   postgresqlWideTableInsertQuery,
			checkQuery:  "select * from wide_table",
			checkResult: postgresqlWideTableInsertResult,
		},
	}

	// Loop over the test cases.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errProperties, err := RunQuery(
				&data.Query{
					Query:      tt.testQuery,
					Connection: tt.connection,
				},
			)

			if err != nil {
				t.Fatalf("unable to run test query. err:\n\n%v\n\nerrProperties:\n%v", errProperties, err)
			}

			if tt.checkQuery != "" {
				dsConn, _, err := GetDs(tt.connection)
				if err != nil {
					t.Fatalf("Couldn't get DsConn")
				}
				queryResult, errProperties, err := standardGetFormattedResults(dsConn, tt.checkQuery)

				if err != nil && err.Error() != tt.expectedErr {
					// t.Error(errProperties)
					t.Fatalf("\nwanted error:\n%#v\n\ngot error:\n%#v\n", tt.expectedErr, err)
				}

				if err != nil && !reflect.DeepEqual(errProperties, tt.expectedErrProperties) {
					t.Fatalf("\nwanted errProperties:\n%#v\n\ngot:\n%#v", tt.expectedErrProperties, errProperties)
				}

				if !reflect.DeepEqual(queryResult, tt.checkResult) {
					t.Fatalf("\n\nWanted:\n%#v\n\nGot:\n%#v", tt.checkResult, queryResult)
				}
			}
		})
	}
}

func TestRunTransfer(t *testing.T) {
	// Define tests here
	tests := []transferTest{
		// PostgreSQL Setup
		{
			name:          "postgresql2mysql_wide",
			source:        postgresqlTestConnection,
			target:        mysqlTestConnection,
			overwrite:     true,
			targetTable:   "postgresql_wide_table",
			transferQuery: "select * from wide_table",
			checkQuery:    "select * from postgresql_wide_table",
			checkResult:   postgresql2mysql_wide_result,
		},
	}

	// Loop over the test cases.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errProperties, err := RunTransfer(
				&data.Transfer{
					Query:       tt.transferQuery,
					Overwrite:   tt.overwrite,
					TargetTable: tt.targetTable,
					Source:      tt.source,
					Target:      tt.target,
				},
			)

			if err != nil {
				t.Fatalf("unable to run transfer. err:\n\n%v\n\nerrProperties:\n%v", errProperties, err)
			}

			if tt.checkQuery != "" {
				dsConn, _, err := GetDs(tt.target)
				if err != nil {
					t.Fatalf("Couldn't get DsConn")
				}
				queryResult, errProperties, err := standardGetFormattedResults(dsConn, tt.checkQuery)

				if err != nil && err.Error() != tt.expectedErr {
					// t.Error(errProperties)
					t.Fatalf("\nwanted error:\n%#v\n\ngot error:\n%#v\n", tt.expectedErr, err)
				}

				if err != nil && !reflect.DeepEqual(errProperties, tt.expectedErrProperties) {
					t.Fatalf("\nwanted errProperties:\n%#v\n\ngot:\n%#v", tt.expectedErrProperties, errProperties)
				}

				if !reflect.DeepEqual(queryResult, tt.checkResult) {
					t.Fatalf("\n\nWanted:\n%#v\n\nGot:\n%#v", tt.checkResult, queryResult)
				}
			}
		})
	}
}
