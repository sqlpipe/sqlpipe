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
	mysqlTestSource = data.Source{
		OdbcDsn: "DRIVER=MySQL;SERVER=localhost;PORT=3306;UID=root;database=mysql;PWD=Mypass123;",
	}
)

type setupTest struct {
	name             string      // name of test
	source           data.Source //source to run query on
	testQuery        string      // query
	checkQuery       string      // query to test if the testQuery was successful
	checkResult      string      // expected result of checkQuery
	expectedErr      string      // if an error is expected, this will be the expected error
	expectedCheckErr string      // if an error is expected during check query, this will be the expected error
}
