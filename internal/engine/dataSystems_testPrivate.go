package engine

import "github.com/calmitchell617/sqlpipe/internal/data"

// Connection variables
var (
	postgresqlTestConnection = data.Connection{
		DsType:   "postgresql",
		Username: "sqlpipe",
		Password: "Mypass123",
		Hostname: "sqlpipe-test-postgresql.cg1tst0w2iko.eu-west-2.rds.amazonaws.com",
		Port:     5432,
		DbName:   "testing",
	}
	mysqlTestConnection = data.Connection{
		DsType:   "mysql",
		Username: "sqlpipe",
		Password: "Mypass123",
		Hostname: "sqlpipe-test-mysql.cg1tst0w2iko.eu-west-2.rds.amazonaws.com",
		Port:     3306,
		DbName:   "testing",
	}
)
