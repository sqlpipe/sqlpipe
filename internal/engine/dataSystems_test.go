package engine

import (
	"os"
	"reflect"
	"testing"

	"github.com/calmitchell617/sqlpipe/internal/data"
)

var queryTests = []queryTest{
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
	// MySQL Setup
	{
		name:                  "mysqlWideTableDrop",
		connection:            mysqlTestConnection,
		testQuery:             "drop table if exists wide_table;",
		checkQuery:            "select * from wide_table",
		expectedErr:           "db.Query() threw an error",
		expectedErrProperties: mysqlWideTableDropErrProperties,
	},
	{
		name:        "mysqlWideTableCreate",
		connection:  mysqlTestConnection,
		testQuery:   mysqlWideTableCreateQuery,
		checkQuery:  "select * from wide_table",
		checkResult: mysqlWideTableCreateResult,
	},
	{
		name:        "mysqlWideTableInsert",
		connection:  mysqlTestConnection,
		testQuery:   mysqlWideTableInsertQuery,
		checkQuery:  "select * from wide_table",
		checkResult: mysqlWideTableInsertResult,
	},
	// MSSQL setup
	{
		name:                  "mssqlTestingDbDrop",
		connection:            mssqlMasterTestConnection,
		testQuery:             "drop database if exists testing",
		expectedErr:           "db.Query() threw an error",
		expectedErrProperties: map[string]string{"dsType": "mssql", "error": "mssql: Cannot drop database \"testing\" because it is currently in use.", "query": "drop database if exists testing"},
	},
	{
		name:                  "mssqlTestingDbCreate",
		connection:            mssqlMasterTestConnection,
		testQuery:             "create database testing",
		expectedErr:           "db.Query() threw an error",
		expectedErrProperties: map[string]string{"dsType": "mssql", "error": "mssql: Database 'testing' already exists. Choose a different database name.", "query": "create database testing"},
	},
	{
		name:                  "mssqlWideTableDrop",
		connection:            mssqlTestConnection,
		testQuery:             "drop table if exists wide_table;",
		checkQuery:            "select * from wide_table",
		expectedErr:           "db.Query() threw an error",
		expectedErrProperties: mssqlWideTableDropErrProperties,
	},
	{
		name:        "mssqlWideTableCreate",
		connection:  mssqlTestConnection,
		testQuery:   mssqlWideTableCreateQuery,
		checkQuery:  "select * from wide_table",
		checkResult: mssqlWideTableCreateResult,
	},
	{
		name:        "mssqlWideTableInsert",
		connection:  mssqlTestConnection,
		testQuery:   mssqlWideTableInsertQuery,
		checkQuery:  "select * from wide_table",
		checkResult: mssqlWideTableInsertResult,
	},

	// Oracle Setup
	{
		name:                  "oracleWideTableDrop",
		connection:            oracleTestConnection,
		testQuery:             "drop table wide_table",
		expectedErr:           "db.Query() threw an error",
		expectedErrProperties: map[string]string{"dsType": "oracle", "error": "ORA-00942: table or view does not exist\n", "query": "drop table wide_table"},
	},
	{
		name:        "oracleWideTableCreate",
		connection:  oracleTestConnection,
		testQuery:   "create table wide_table (mychar char(3), myvarchar varchar(20), myvarchar2 varchar2(20), mynchar nchar(3), mynvarchar2 nvarchar2(20), myclob clob, mylong long, mynumber number, mybinary_float binary_float, mybinary_double binary_double, mydate date, mytimestamp timestamp, mytimestamptz timestamp with time zone, mytimestampwithlocaltz timestamp with local time zone, myblob blob)",
		checkQuery:  "select * from wide_table",
		checkResult: QueryResult{ColumnTypes: map[string]string{"MYBINARY_DOUBLE": "IBDouble", "MYBINARY_FLOAT": "IBFloat", "MYBLOB": "OCIBlobLocator", "MYCHAR": "CHAR", "MYCLOB": "OCIClobLocator", "MYDATE": "DATE", "MYLONG": "LONG", "MYNCHAR": "CHAR", "MYNUMBER": "NUMBER", "MYNVARCHAR2": "NCHAR", "MYTIMESTAMP": "TimeStampDTY", "MYTIMESTAMPTZ": "TimeStampTZ_DTY", "MYTIMESTAMPWITHLOCALTZ": "TimeStampLTZ_DTY", "MYVARCHAR": "NCHAR", "MYVARCHAR2": "NCHAR"}, Rows: []interface{}{}},
	},
	{
		name:        "oracleWideTableInsert",
		connection:  oracleTestConnection,
		testQuery:   `insert into wide_table (mychar, myvarchar, myvarchar2, mynchar, mynvarchar2, myclob, mylong, mynumber, mybinary_float, mybinary_double, mydate, mytimestamp, mytimestamptz, mytimestampwithlocaltz, myblob)  WITH rows_to_insert (mychar, myvarchar, myvarchar2, mynchar, mynvarchar2, myclob, mylong, mynumber, mybinary_float, mybinary_double, mydate, mytimestamp, mytimestamptz, mytimestampwithlocaltz, myblob) AS (SELECT 'chr', 'my vr''c",hr', 'my vrchr2', 'ncr', 'mynvarch2', 'myclob', 'wow such long text wow', 12.5, 47.5, 900.2, TO_DATE('2005/09/16', 'yyyy/mm/dd'), to_timestamp('2021-07-22 10:18:59.194681', 'YYYY-MM-DD HH24:MI:SS.FF'), to_timestamp_tz('2021-07-22 10:18:59.194681 +0100', 'YYYY-MM-DD HH24:MI:SS.FF +TZHTZM'), to_timestamp_tz('2021-07-22 10:18:59.194681 +0100', 'YYYY-MM-DD HH24:MI:SS.FF +TZHTZM'), hextoraw('111a')  FROM dual UNION ALL SELECT null, null, null, null, null, null, null, null, null, null, null, null, null, null, null  FROM dual) select * from rows_to_insert`,
		checkQuery:  "select * from wide_table",
		checkResult: QueryResult{ColumnTypes: map[string]string{"MYBINARY_DOUBLE": "IBDouble", "MYBINARY_FLOAT": "IBFloat", "MYBLOB": "OCIBlobLocator", "MYCHAR": "CHAR", "MYCLOB": "OCIClobLocator", "MYDATE": "DATE", "MYLONG": "LONG", "MYNCHAR": "CHAR", "MYNUMBER": "NUMBER", "MYNVARCHAR2": "NCHAR", "MYTIMESTAMP": "TimeStampDTY", "MYTIMESTAMPTZ": "TimeStampTZ_DTY", "MYTIMESTAMPWITHLOCALTZ": "TimeStampLTZ_DTY", "MYVARCHAR": "NCHAR", "MYVARCHAR2": "NCHAR"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "12.5", "47.5", "900.2", "TO_DATE('2005-09-16', 'YYYY-MM-DD')", "TO_TIMESTAMP('2021-07-22 10:18:59.194681', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2021-07-25 03:18:59.194681', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2021-07-22 09:18:59.194681', 'YYYY-MM-DD HH24:MI:SS.FF')", "hextoraw('111a')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "<nil>", "<nil>", "<nil>", "null", "null", "null", "null", "hextoraw('%!x(<nil>)')"}},
	},
	// Redshift Setup
	{
		name:                  "redshiftWideTableDrop",
		connection:            redshiftTestConnection,
		testQuery:             "drop table if exists wide_table;",
		checkQuery:            "select * from wide_table",
		expectedErr:           "db.Query() threw an error",
		expectedErrProperties: redshiftWideTableDropErrProperties,
	},
	{
		name:        "redshiftWideTableCreate",
		connection:  redshiftTestConnection,
		testQuery:   redshiftWideTableCreateQuery,
		checkQuery:  "select * from wide_table",
		checkResult: redshiftWideTableCreateResult,
	},
	{
		name:        "redshiftWideTableInsert",
		connection:  redshiftTestConnection,
		testQuery:   redshiftWideTableInsertQuery,
		checkQuery:  "select * from wide_table",
		checkResult: redshiftWideTableInsertResult,
	},
	// Snowflake Setup
	{
		name:                  "snowflakeWideTableDrop",
		connection:            snowflakeTestConnection,
		testQuery:             "drop table if exists wide_table;",
		checkQuery:            "select * from wide_table",
		expectedErr:           "db.Query() threw an error",
		expectedErrProperties: snowflakeWideTableDropErrProperties,
	},
	{
		name:        "snowflakeWideTableCreate",
		connection:  snowflakeTestConnection,
		testQuery:   snowflakeWideTableCreateQuery,
		checkQuery:  "select * from wide_table",
		checkResult: snowflakeWideTableCreateResult,
	},
	{
		name:        "snowflakeWideTableInsert",
		connection:  snowflakeTestConnection,
		testQuery:   snowflakeWideTableInsertQuery,
		checkQuery:  "select * from wide_table",
		checkResult: snowflakeWideTableInsertResult,
	},
}

var transferTests = []transferTest{
	// PostgreSQL Transfers
	{
		name:          "postgresql2postgresql_wide",
		source:        postgresqlTestConnection,
		target:        postgresqlTestConnection,
		overwrite:     true,
		targetSchema:  "public",
		targetTable:   "postgresql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from postgresql_wide_table",
		checkResult:   postgresql2postgresql_wide_result,
	},
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
	{
		name:          "postgresql2mssql_wide",
		source:        postgresqlTestConnection,
		target:        mssqlTestConnection,
		overwrite:     true,
		targetTable:   "postgresql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from postgresql_wide_table",
		checkResult:   postgresql2mssql_wide_result,
	},
	{
		name:          "postgresql2oracle_wide",
		source:        postgresqlTestConnection,
		target:        oracleTestConnection,
		overwrite:     true,
		targetTable:   "postgresql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from postgresql_wide_table",
		checkResult:   postgresql2oracle_wide_result,
	},
	{
		name:          "postgresql2redshift_wide",
		source:        postgresqlTestConnection,
		target:        redshiftTestConnection,
		overwrite:     true,
		targetSchema:  "public",
		targetTable:   "postgresql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from postgresql_wide_table",
		checkResult:   postgresql2redshift_wide_result,
	},
	{
		name:          "postgresql2snowflake_wide",
		source:        postgresqlTestConnection,
		target:        snowflakeTestConnection,
		overwrite:     true,
		targetSchema:  "public",
		targetTable:   "postgresql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from postgresql_wide_table",
		checkResult:   postgresql2snowflake_wide_result,
	},
	// MySQL Transfers
	{
		name:          "mysql2postgresql_wide",
		source:        mysqlTestConnection,
		target:        postgresqlTestConnection,
		overwrite:     true,
		targetSchema:  "public",
		targetTable:   "mysql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from mysql_wide_table",
		checkResult:   mysql2postgresql_wide_result,
	},
	{
		name:          "mysql2mysql_wide",
		source:        mysqlTestConnection,
		target:        mysqlTestConnection,
		overwrite:     true,
		targetTable:   "mysql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from mysql_wide_table",
		checkResult:   mysql2mysql_wide_result,
	},
	{
		name:          "mysql2mssql_wide",
		source:        mysqlTestConnection,
		target:        mssqlTestConnection,
		overwrite:     true,
		targetTable:   "mysql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from mysql_wide_table",
		checkResult:   mysql2mssql_wide_result,
	},
	{
		name:          "mysql2oracle_wide",
		source:        mysqlTestConnection,
		target:        oracleTestConnection,
		overwrite:     true,
		targetTable:   "mysql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from mysql_wide_table",
		checkResult:   mysql2oracle_wide_result,
	},
	{
		name:          "mysql2redshift_wide",
		source:        mysqlTestConnection,
		target:        redshiftTestConnection,
		overwrite:     true,
		targetSchema:  "public",
		targetTable:   "mysql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from mysql_wide_table",
		checkResult:   mysql2redshift_wide_result,
	},
	{
		name:          "mysql2snowflake_wide",
		source:        mysqlTestConnection,
		target:        snowflakeTestConnection,
		overwrite:     true,
		targetSchema:  "public",
		targetTable:   "mysql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from mysql_wide_table",
		checkResult:   mysql2snowflake_wide_result,
	},
	// MSSQL Transfers
	{
		name:          "mssql2postgresql_wide",
		source:        mssqlTestConnection,
		target:        postgresqlTestConnection,
		overwrite:     true,
		targetSchema:  "public",
		targetTable:   "mssql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from mssql_wide_table",
		checkResult:   mssql2postgresql_wide_result,
	},
	{
		name:          "mssql2mysql_wide",
		source:        mssqlTestConnection,
		target:        mysqlTestConnection,
		overwrite:     true,
		targetTable:   "mssql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from mssql_wide_table",
		checkResult:   mssql2mysql_wide_result,
	},
	{
		name:          "mssql2mssql_wide",
		source:        mssqlTestConnection,
		target:        mssqlTestConnection,
		overwrite:     true,
		targetTable:   "mssql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from mssql_wide_table",
		checkResult:   mssql2mssql_wide_result,
	},
	{
		name:          "mssql2oracle_wide",
		source:        mssqlTestConnection,
		target:        oracleTestConnection,
		overwrite:     true,
		targetTable:   "mssql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from mssql_wide_table",
		checkResult:   mssql2oracle_wide_result,
	},
	{
		name:          "mssql2redshift_wide",
		source:        mssqlTestConnection,
		target:        redshiftTestConnection,
		overwrite:     true,
		targetSchema:  "public",
		targetTable:   "mssql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from mssql_wide_table",
		checkResult:   mssql2redshift_wide_result,
	},
	{
		name:          "mssql2snowflake_wide",
		source:        mssqlTestConnection,
		target:        snowflakeTestConnection,
		overwrite:     true,
		targetSchema:  "public",
		targetTable:   "mssql_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from mssql_wide_table",
		checkResult:   mssql2snowflake_wide_result,
	},
	// Oracle Transfers
	{
		name:          "oracle2postgresql_wide",
		source:        oracleTestConnection,
		target:        postgresqlTestConnection,
		overwrite:     true,
		targetSchema:  "public",
		targetTable:   "oracle_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from oracle_wide_table",
		checkResult:   QueryResult{ColumnTypes: map[string]string{"mybinary_double": "NUMERIC", "mybinary_float": "NUMERIC", "myblob": "BYTEA", "mychar": "TEXT", "myclob": "VARCHAR", "mydate": "DATE", "mylong": "TEXT", "mynchar": "TEXT", "mynumber": "NUMERIC", "mynvarchar2": "TEXT", "mytimestamp": "TIMESTAMP", "mytimestamptz": "TIMESTAMP", "mytimestampwithlocaltz": "TIMESTAMP", "myvarchar": "TEXT", "myvarchar2": "TEXT"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "12.5", "47.5", "900.2", "'2005-09-16 00:00:00.000000 +0000'", "'2021-07-22 10:18:59.194681 +0000'", "'2021-07-22 10:18:59.194681 +0000'", "'2021-07-22 09:18:59.194681 +0000'", "'\\x111a'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "null", "null", "null", "null", "'\\x%!x(<nil>)'"}},
	},
	{
		name:          "oracle2mysql_wide",
		source:        oracleTestConnection,
		target:        mysqlTestConnection,
		overwrite:     true,
		targetTable:   "oracle_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from oracle_wide_table",
		checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBINARY_DOUBLE": "DOUBLE", "MYBINARY_FLOAT": "DOUBLE", "MYBLOB": "BLOB", "MYCHAR": "TEXT", "MYCLOB": "TEXT", "MYDATE": "DATE", "MYLONG": "TEXT", "MYNCHAR": "TEXT", "MYNUMBER": "DOUBLE", "MYNVARCHAR2": "TEXT", "MYTIMESTAMP": "TIMESTAMP", "MYTIMESTAMPTZ": "TIMESTAMP", "MYTIMESTAMPWITHLOCALTZ": "TIMESTAMP", "MYVARCHAR": "TEXT", "MYVARCHAR2": "TEXT"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "12.5", "47.5", "900.2", "'2005-09-16'", "'2021-07-22 10:18:59'", "'2021-07-22 10:18:59'", "'2021-07-22 09:18:59'", "x'111a'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "x'%!x(<nil>)'"}},
	},
	{
		name:          "oracle2mssql_wide",
		source:        oracleTestConnection,
		target:        mssqlTestConnection,
		overwrite:     true,
		targetTable:   "oracle_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from oracle_wide_table",
		checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBINARY_DOUBLE": "FLOAT", "MYBINARY_FLOAT": "FLOAT", "MYBLOB": "VARBINARY", "MYCHAR": "NTEXT", "MYCLOB": "NVARCHAR", "MYDATE": "DATE", "MYLONG": "NTEXT", "MYNCHAR": "NTEXT", "MYNUMBER": "FLOAT", "MYNVARCHAR2": "NTEXT", "MYTIMESTAMP": "DATETIME2", "MYTIMESTAMPTZ": "DATETIME2", "MYTIMESTAMPWITHLOCALTZ": "DATETIME2", "MYVARCHAR": "NTEXT", "MYVARCHAR2": "NTEXT"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "12.5", "47.5", "900.2", "CONVERT(DATETIME2, '2005-09-16 00:00:00.0000000', 121)", "CONVERT(DATETIME2, '2021-07-22 10:18:59.1946810', 121)", "CONVERT(DATETIME2, '2021-07-22 10:18:59.1946810', 121)", "CONVERT(DATETIME2, '2021-07-22 09:18:59.1946810', 121)", "CONVERT(VARBINARY(8000), '0x111a', 1)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "null", "null", "null", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)"}},
	},
	{
		name:          "oracle2oracle_wide",
		source:        oracleTestConnection,
		target:        oracleTestConnection,
		overwrite:     true,
		targetTable:   "oracle_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from oracle_wide_table",
		checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBINARY_DOUBLE": "NUMBER", "MYBINARY_FLOAT": "NUMBER", "MYBLOB": "OCIBlobLocator", "MYCHAR": "NCHAR", "MYCLOB": "OCIClobLocator", "MYDATE": "DATE", "MYLONG": "LONG", "MYNCHAR": "NCHAR", "MYNUMBER": "NUMBER", "MYNVARCHAR2": "NCHAR", "MYTIMESTAMP": "TimeStampDTY", "MYTIMESTAMPTZ": "TimeStampDTY", "MYTIMESTAMPWITHLOCALTZ": "TimeStampDTY", "MYVARCHAR": "NCHAR", "MYVARCHAR2": "NCHAR"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "12.5", "47.5", "900.2", "TO_DATE('2005-09-16', 'YYYY-MM-DD')", "TO_TIMESTAMP('2021-07-22 10:18:59.194681', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2021-07-22 10:18:59.194681', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2021-07-22 09:18:59.194681', 'YYYY-MM-DD HH24:MI:SS.FF')", "hextoraw('111a')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "<nil>", "<nil>", "<nil>", "null", "null", "null", "null", "hextoraw('%!x(<nil>)')"}},
	},
	{
		name:          "oracle2redshift_wide",
		source:        oracleTestConnection,
		target:        redshiftTestConnection,
		overwrite:     true,
		targetSchema:  "public",
		targetTable:   "oracle_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from oracle_wide_table",
		checkResult:   QueryResult{ColumnTypes: map[string]string{"mybinary_double": "FLOAT8", "mybinary_float": "FLOAT8", "myblob": "VARCHAR", "mychar": "VARCHAR", "myclob": "VARCHAR", "mydate": "DATE", "mylong": "VARCHAR", "mynchar": "VARCHAR", "mynumber": "FLOAT8", "mynvarchar2": "VARCHAR", "mytimestamp": "TIMESTAMP", "mytimestamptz": "TIMESTAMP", "mytimestampwithlocaltz": "TIMESTAMP", "myvarchar": "VARCHAR", "myvarchar2": "VARCHAR"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "12.5", "47.5", "900.2", "'2005-09-16 00:00:00.000000 +0000'", "'2021-07-22 10:18:59.194681 +0000'", "'2021-07-22 10:18:59.194681 +0000'", "'2021-07-22 09:18:59.194681 +0000'", "'\x11\x1a'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "null", "null", "null", "'%!s(<nil>)'"}},
	},
	{
		name:          "oracle2snowflake_wide",
		source:        oracleTestConnection,
		target:        snowflakeTestConnection,
		overwrite:     true,
		targetSchema:  "public",
		targetTable:   "oracle_wide_table",
		transferQuery: "select * from wide_table",
		checkQuery:    "select * from oracle_wide_table",
		checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBINARY_DOUBLE": "REAL", "MYBINARY_FLOAT": "REAL", "MYBLOB": "BINARY", "MYCHAR": "TEXT", "MYCLOB": "TEXT", "MYDATE": "DATE", "MYLONG": "TEXT", "MYNCHAR": "TEXT", "MYNUMBER": "REAL", "MYNVARCHAR2": "TEXT", "MYTIMESTAMP": "TIMESTAMP_NTZ", "MYTIMESTAMPTZ": "TIMESTAMP_NTZ", "MYTIMESTAMPWITHLOCALTZ": "TIMESTAMP_NTZ", "MYVARCHAR": "TEXT", "MYVARCHAR2": "TEXT"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "%!g(string=12.500000)", "%!g(string=47.500000)", "%!g(string=900.200000)", "'2005-09-16 00:00:00.000000'", "'2021-07-22 10:18:59.194681'", "'2021-07-22 10:18:59.194681'", "'2021-07-22 09:18:59.194681'", "to_binary('111a')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "null", "null", "null", "to_binary('%!x(<nil>)')"}},
	},
}

func TestRunQuery(t *testing.T) {

	// Loop over the test cases.
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			errProperties, err := RunQuery(
				&data.Query{
					Query:      tt.testQuery,
					Connection: tt.connection,
				},
			)

			if err != nil && (err.Error() != tt.expectedErr || !reflect.DeepEqual(errProperties, tt.expectedErrProperties)) {

				t.Fatalf("unable to run test query. err:\n\n%v\n\nerrProperties:\n%#v", err, errProperties)
			}

			if tt.checkQuery != "" {
				dsConn, _, err := GetDs(tt.connection)
				if err != nil {
					t.Fatalf("Couldn't get DsConn")
				}
				queryResult, errProperties, err := standardGetFormattedResults(dsConn, tt.checkQuery)

				if err != nil && err.Error() != tt.expectedErr {
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

	// Loop over the test cases.
	for _, tt := range transferTests {

		t.Run(tt.name, func(t *testing.T) {
			errProperties, err := RunTransfer(
				&data.Transfer{
					Query:        tt.transferQuery,
					Overwrite:    tt.overwrite,
					TargetSchema: tt.targetSchema,
					TargetTable:  tt.targetTable,
					Source:       tt.source,
					Target:       tt.target,
				},
			)

			if err != nil {
				t.Fatalf("unable to run transfer. err:\n\n%v\n\nerrProperties:\n%v", err, errProperties)
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

// Test connections
var (
	postgresqlTestConnection = data.Connection{
		DsType:   "postgresql",
		Username: os.Getenv("postgresqlUsername"),
		Password: os.Getenv("postgresqlPassword"),
		Hostname: os.Getenv("postgresqlHostname"),
		DbName:   os.Getenv("postgresqlDbName"),
		Port:     5432,
	}
	mysqlTestConnection = data.Connection{
		DsType:   "mysql",
		Username: os.Getenv("mysqlUsername"),
		Password: os.Getenv("mysqlPassword"),
		Hostname: os.Getenv("mysqlHostname"),
		DbName:   os.Getenv("mysqlDbName"),
		Port:     3306,
	}
	mssqlMasterTestConnection = data.Connection{
		DsType:   "mssql",
		Username: os.Getenv("mssqlUsername"),
		Password: os.Getenv("mssqlPassword"),
		Hostname: os.Getenv("mssqlHostname"),
		DbName:   "master",
		Port:     1433,
	}
	mssqlTestConnection = data.Connection{
		DsType:   "mssql",
		Username: os.Getenv("mssqlUsername"),
		Password: os.Getenv("mssqlPassword"),
		Hostname: os.Getenv("mssqlHostname"),
		DbName:   os.Getenv("mssqlDbName"),
		Port:     1433,
	}
	oracleTestConnection = data.Connection{
		DsType:   "oracle",
		Username: os.Getenv("oracleUsername"),
		Password: os.Getenv("oraclePassword"),
		Hostname: os.Getenv("oracleHostname"),
		DbName:   os.Getenv("oracleDbName"),
		Port:     1521,
	}
	redshiftTestConnection = data.Connection{
		DsType:   "redshift",
		Username: os.Getenv("redshiftUsername"),
		Password: os.Getenv("redshiftPassword"),
		Hostname: os.Getenv("redshiftHostname"),
		DbName:   os.Getenv("redshiftDbName"),
		Port:     5439,
	}

	snowflakeTestConnection = data.Connection{
		DsType:    "snowflake",
		Username:  os.Getenv("snowflakeUsername"),
		Password:  os.Getenv("snowflakePassword"),
		AccountId: os.Getenv("snowflakeAccountId"),
		DbName:    os.Getenv("snowflakeDbName"),
	}
)
