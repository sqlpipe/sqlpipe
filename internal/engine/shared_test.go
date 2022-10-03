package engine

import "github.com/sqlpipe/sqlpipe/internal/data"

var (
	// Sources
	postgresqlTestSource = data.Source{
		OdbcDsn: "Driver=PostgreSQL;Server=localhost;Port=5432;Database=postgres;Uid=postgres;Pwd=Mypass123;",
	}
	mssqlTestSource = data.Source{
		OdbcDsn: "DRIVER=MSSQL;SERVER=localhost;PORT=1433;Database=master;UID=sa;PWD=Mypass123;TDS_Version=7.0",
	}
)

type setupTest struct {
	name        string      // name of test
	source      data.Source //source to run query on
	testQuery   string      // query
	checkQuery  string      // query to test if the testQuery was successful
	checkResult string      // expected result of checkQuery
	expectedErr string      // if an error is expected, this will be the expected error
}
