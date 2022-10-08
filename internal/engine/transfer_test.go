package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/shomali11/xsql"
	_ "github.com/sqlpipe/odbc"

	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/engine/transfers"
)

var (
	// Targets
	postgresqlTestTarget = data.Target{
		SystemType: "postgresql",
		OdbcDsn:    "Driver=PostgreSQL;Server=localhost;Port=5432;Database=postgres;Uid=postgres;Pwd=Mypass123;",
		Schema:     "public",
	}
	mssqlTestTarget = data.Target{
		SystemType: "mssql",
		OdbcDsn:    "DRIVER=MSSQL;SERVER=localhost;PORT=1433;Database=master;UID=sa;PWD=Mypass123;TDS_Version=7.0",
		Schema:     "dbo",
	}
	mysqlTestTarget = data.Target{
		SystemType: "mysql",
		OdbcDsn:    "DRIVER=MySQL;SERVER=localhost;PORT=3306;UID=root;database=mysql;PWD=Mypass123;",
	}
	snowflakeTestTarget = data.Target{
		SystemType: "snowflake",
		OdbcDsn:    fmt.Sprintf("DRIVER=Snowflake;Server=%v.snowflakecomputing.com;PWD=%v;UID=%v;database=testing;schema=public;", os.Getenv("SNOWFLAKE_ACCOUNT"), os.Getenv("SNOWFLAKE_PASSWORD"), os.Getenv("SNOWFLAKE_USER")),
	}
)

type transferTest struct {
	name              string
	transfer          data.Transfer
	targetCheckSource data.Source
	targetTable       string
	checkQuery        string
	checkResult       interface{}
	expectedErr       string
}

var transferTests = []transferTest{
	// PostgreSQL source
	{
		name: "postgresql wide_table to postgresql",
		transfer: data.Transfer{
			Source:            postgresqlTestSource,
			Target:            postgresqlTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: postgresqlTestSource,
		targetTable:       "postgresql_wide_table",
		checkQuery:        "select * from postgresql_wide_table;",
		checkResult:       "      mybigint       | mybit | mybitvarying | myboolean |    mybox    | mybytea  | mychar |         myvarchar          |       mycidr       | mycircle  |        mydate        | mydoubleprecision |     myinet      | myinteger |    myinterval    |              myjson               |           myjsonb           |  myline  |    mylseg     |     mymacaddr     | mymoney  | mynumeric |    mypath     |  mypg_lsn   | mypoint |       mypolygon       |  myreal  | mysmallint |        mytext         |        mytime        |      mytimetz      |     mytimestamp      |    mytimestamptz     |   mytsquery   |                     mytsvector                     |                myuuid                |     myxml      \n---------------------+-------+--------------+-----------+-------------+----------+--------+----------------------------+--------------------+-----------+----------------------+-------------------+-----------------+-----------+------------------+-----------------------------------+-----------------------------+----------+---------------+-------------------+----------+-----------+---------------+-------------+---------+-----------------------+----------+------------+-----------------------+----------------------+--------------------+----------------------+----------------------+---------------+----------------------------------------------------+--------------------------------------+----------------\n 6514798382812790784 |     1 |         1001 |         1 | (8,9),(1,3) | aaaabbbb |    abc | \"my\"varch'ar,123@gmail.com | 192.168.100.128/25 | <(1,5),5> | 2014-01-10T00:00:00Z | 529.5621898337544 | 192.168.100.128 | 745910651 | 10 days 10:00:00 | (\"mykey\": \"this\\\"  'is' m,y val\") | (\"mykey\": \"this is my val\") | (1,5,20) | [(5,4),(2,1)] | 08:00:2b:01:02:03 | 35244.33 | 449.82115 | [(1,4),(8,7)] | 16/B374D848 |   (5,7) | ((5,8),(6,10),(7,20)) | 9673.109 |      24345 | myte\",xt123@gmail.com | 0001-01-01T03:46:38Z | 03:46:38.765594+05 | 2014-01-10T10:05:04Z | 2014-01-10T18:05:04Z | 'fat' & 'rat' | 'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat' | a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 | <foo>bar</foo> \n                     |       |              |           |             |          |        |                            |                    |           |                      |                   |                 |           |                  |                                   |                             |          |               |                   |          |           |               |             |         |                       |          |            |                       |                      |                    |                      |                      |               |                                                    |                                      |                \n(2 rows)",
	},
	{
		name: "postgresql wide_table to mssql",
		transfer: data.Transfer{
			Source:            postgresqlTestSource,
			Target:            mssqlTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: mssqlTestSource,
		targetTable:       "postgresql_wide_table",
		checkQuery:        "select * from postgresql_wide_table;",
		checkResult:       "       mybigint        | mybit | mybitvarying | myboolean |    mybox    | mybytea  | mychar |         myvarchar          |       mycidr       | mycircle  |   mydate   | mydoubleprecision |     myinet      | myinteger |    myinterval    |              myjson               |           myjsonb           |  myline  |    mylseg     |     mymacaddr     | mymoney  | mynumeric |    mypath     |  mypg_lsn   | mypoint |       mypolygon       |  myreal  | mysmallint |        mytext         |      mytime      |      mytimetz      |         mytimestamp         |        mytimestamptz        |   mytsquery   |                     mytsvector                     |                myuuid                |     myxml      \n-----------------------+-------+--------------+-----------+-------------+----------+--------+----------------------------+--------------------+-----------+------------+-------------------+-----------------+-----------+------------------+-----------------------------------+-----------------------------+----------+---------------+-------------------+----------+-----------+---------------+-------------+---------+-----------------------+----------+------------+-----------------------+------------------+--------------------+-----------------------------+-----------------------------+---------------+----------------------------------------------------+--------------------------------------+----------------\n 6.514798382812791e+18 |  true |         1001 |         1 | (8,9),(1,3) | aaaabbbb |    abc | \"my\"varch'ar,123@gmail.com | 192.168.100.128/25 | <(1,5),5> | 2014-01-10 | 529.5621898337544 | 192.168.100.128 | 745910651 | 10 days 10:00:00 | (\"mykey\": \"this\\\"  'is' m,y val\") | (\"mykey\": \"this is my val\") | (1,5,20) | [(5,4),(2,1)] | 08:00:2b:01:02:03 | 35244.33 | 449.82115 | [(1,4),(8,7)] | 16/B374D848 |   (5,7) | ((5,8),(6,10),(7,20)) | 9673.109 |      24345 | myte\",xt123@gmail.com | 03:46:38.0000000 | 03:46:38.765594+05 | 2014-01-10 10:05:04.0000000 | 2014-01-10 18:05:04.0000000 | 'fat' & 'rat' | 'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat' | a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 | <foo>bar</foo> \n                       |       |              |           |             |          |        |                            |                    |           |            |                   |                 |           |                  |                                   |                             |          |               |                   |          |           |               |             |         |                       |          |            |                       |                  |                    |                             |                             |               |                                                    |                                      |                \n(2 rows)",
	},
	{
		name: "postgresql wide_table to mysql",
		transfer: data.Transfer{
			Source:            postgresqlTestSource,
			Target:            mysqlTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: mysqlTestSource,
		targetTable:       "postgresql_wide_table",
		checkQuery:        "select * from postgresql_wide_table;",
		checkResult:       "      mybigint       | mybit | mybitvarying | myboolean |    mybox    | mybytea  | mychar |         myvarchar          |       mycidr       | mycircle  |        mydate        | mydoubleprecision |     myinet      | myinteger |    myinterval    |              myjson              |           myjsonb           |  myline  |    mylseg     |     mymacaddr     | mymoney  | mynumeric |    mypath     |  mypg_lsn   | mypoint |       mypolygon       |  myreal  | mysmallint |        mytext         |        mytime        |      mytimetz      |     mytimestamp      |    mytimestamptz     |   mytsquery   |                     mytsvector                     |                myuuid                |     myxml      \n---------------------+-------+--------------+-----------+-------------+----------+--------+----------------------------+--------------------+-----------+----------------------+-------------------+-----------------+-----------+------------------+----------------------------------+-----------------------------+----------+---------------+-------------------+----------+-----------+---------------+-------------+---------+-----------------------+----------+------------+-----------------------+----------------------+--------------------+----------------------+----------------------+---------------+----------------------------------------------------+--------------------------------------+----------------\n 6514798382812790784 |     1 |         1001 |         1 | (8,9),(1,3) | aaaabbbb |    abc | \"my\"varch'ar,123@gmail.com | 192.168.100.128/25 | <(1,5),5> | 2014-01-10T00:00:00Z | 529.5621898337544 | 192.168.100.128 | 745910651 | 10 days 10:00:00 | (\"mykey\": \"this\"  'is' m,y val\") | (\"mykey\": \"this is my val\") | (1,5,20) | [(5,4),(2,1)] | 08:00:2b:01:02:03 | 35244.33 | 449.82115 | [(1,4),(8,7)] | 16/B374D848 |   (5,7) | ((5,8),(6,10),(7,20)) | 9673.109 |      24345 | myte\",xt123@gmail.com | 0001-01-01T03:46:38Z | 03:46:38.765594+05 | 2014-01-10T10:05:04Z | 2014-01-10T18:05:04Z | 'fat' & 'rat' | 'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat' | a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 | <foo>bar</foo> \n                     |       |              |           |             |          |        |                            |                    |           |                      |                   |                 |           |                  |                                  |                             |          |               |                   |          |           |               |             |         |                       |          |            |                       |                      |                    |                      |                      |               |                                                    |                                      |                \n(2 rows)",
	},
	{
		name: "postgresql wide_table to snowflake",
		transfer: data.Transfer{
			Source:            postgresqlTestSource,
			Target:            snowflakeTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: snowflakeTestSource,
		targetTable:       "postgresql_wide_table",
		checkQuery:        "select * from postgresql_wide_table;",
		checkResult:       "       MYBIGINT        | MYBIT | MYBITVARYING | MYBOOLEAN |    MYBOX    | MYBYTEA | MYCHAR |         MYVARCHAR          |       MYCIDR       | MYCIRCLE  |        MYDATE        | MYDOUBLEPRECISION |     MYINET      |   MYINTEGER    |    MYINTERVAL    |              MYJSON              |           MYJSONB           |  MYLINE  |    MYLSEG     |     MYMACADDR     | MYMONEY  | MYNUMERIC |    MYPATH     |  MYPG_LSN   | MYPOINT |       MYPOLYGON       |  MYREAL  | MYSMALLINT |        MYTEXT         |        MYTIME        |      MYTIMETZ      |     MYTIMESTAMP      |    MYTIMESTAMPTZ     |   MYTSQUERY   |                     MYTSVECTOR                     |                MYUUID                |     MYXML      \n-----------------------+-------+--------------+-----------+-------------+---------+--------+----------------------------+--------------------+-----------+----------------------+-------------------+-----------------+----------------+------------------+----------------------------------+-----------------------------+----------+---------------+-------------------+----------+-----------+---------------+-------------+---------+-----------------------+----------+------------+-----------------------+----------------------+--------------------+----------------------+----------------------+---------------+----------------------------------------------------+--------------------------------------+----------------\n 6.514798382812791e+18 |  true |         1001 |         1 | (8,9),(1,3) |    \xaa\xaa\xbb\xbb |    abc | \"my\"varch'ar,123@gmail.com | 192.168.100.128/25 | <(1,5),5> | 2014-01-10T00:00:00Z | 529.5621898337544 | 192.168.100.128 | 7.45910651e+08 | 10 days 10:00:00 | (\"mykey\": \"this\"  'is' m,y val\") | (\"mykey\": \"this is my val\") | (1,5,20) | [(5,4),(2,1)] | 08:00:2b:01:02:03 | 35244.33 | 449.82115 | [(1,4),(8,7)] | 16/B374D848 |   (5,7) | ((5,8),(6,10),(7,20)) | 9673.109 |      24345 | myte\",xt123@gmail.com | 0001-01-01T03:46:38Z | 03:46:38.765594+05 | 2014-01-10T10:05:04Z | 2014-01-10T18:05:04Z | 'fat' & 'rat' | 'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat' | a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 | <foo>bar</foo> \n                       |       |              |           |             |         |        |                            |                    |           |                      |                   |                 |                |                  |                                  |                             |          |               |                   |          |           |               |             |         |                       |          |            |                       |                      |                    |                      |                      |               |                                                    |                                      |                \n(2 rows)",
	},
	// MSSQL source
	{
		name: "mssql wide_table to postgresql",
		transfer: data.Transfer{
			Source:            mssqlTestSource,
			Target:            postgresqlTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: postgresqlTestSource,
		targetTable:       "mssql_wide_table",
		checkQuery:        "select * from mssql_wide_table;",
		checkResult:       " mybigint | mybit | mydecimal | myint | mymoney | mynumeric | mysmallint | mysmallmoney | mytinyint | myfloat |      myreal       |   mydate   |         mydatetime2         |        mydatetime        |          mydatetimeoffset          |   mysmalldatetime    |      mytime      | mychar | myvarchar  |                                     mytext                                      | mynchar | mynvarchar |                                   myntext                                    | mybinary | myvarbinary |          myuniqueidentifier          |     myxml      \n----------+-------+-----------+-------+---------+-----------+------------+--------------+-----------+---------+-------------------+------------+-----------------------------+--------------------------+------------------------------------+----------------------+------------------+--------+------------+---------------------------------------------------------------------------------+---------+------------+------------------------------------------------------------------------------+----------+-------------+--------------------------------------+----------------\n   435345 |     1 |    324.43 |    54 |   43.21 |     54.33 |         12 |         22.1 |         4 |    45.5 | 47.70000076293945 | 2013-10-12 | 2005-06-12 11:40:17.6320000 | 2005-06-12T11:40:17.633Z | 2005-06-12 11:40:17.6320000 +01:00 | 2005-06-12T11:40:00Z | 11:40:12.5436540 |    yoo | gday guvna | omg have you hea'rd\" a,bout the latest craze that the people are talking about? |     yoo | gday guvna | omg have you heard about the latest craze that the people are talking about? |   000065 |    000186a1 | 6f9619ff-8b86-d011-b42d-00c04fc964ff | <foo>bar</foo> \n          |       |           |       |         |           |            |              |           |         |                   |            |                             |                          |                                    |                      |                  |        |            |                                                                                 |         |            |                                                                              |          |             |                                      |                \n(2 rows)",
	},
	{
		name: "mssql wide_table to mssql",
		transfer: data.Transfer{
			Source:            mssqlTestSource,
			Target:            mssqlTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: mssqlTestSource,
		targetTable:       "mssql_wide_table",
		checkQuery:        "select * from mssql_wide_table;",
		checkResult:       " mybigint | mybit | mydecimal | myint | mymoney | mynumeric | mysmallint | mysmallmoney | mytinyint | myfloat |      myreal       |   mydate   |         mydatetime2         |         mydatetime          |          mydatetimeoffset          |       mysmalldatetime       |      mytime      | mychar | myvarchar  |                                     mytext                                      | mynchar | mynvarchar |                                   myntext                                    | mybinary | myvarbinary |          myuniqueidentifier          |     myxml      \n----------+-------+-----------+-------+---------+-----------+------------+--------------+-----------+---------+-------------------+------------+-----------------------------+-----------------------------+------------------------------------+-----------------------------+------------------+--------+------------+---------------------------------------------------------------------------------+---------+------------+------------------------------------------------------------------------------+----------+-------------+--------------------------------------+----------------\n   435345 |  true |    324.43 |    54 |   43.21 |     54.33 |         12 |         22.1 |         4 |    45.5 | 47.70000076293945 | 2013-10-12 | 2005-06-12 11:40:17.6320000 | 2005-06-12 11:40:17.6330000 | 2005-06-12 11:40:17.6320000 +01:00 | 2005-06-12 11:40:00.0000000 | 11:40:12.5436540 |    yoo | gday guvna | omg have you hea'rd\" a,bout the latest craze that the people are talking about? |     yoo | gday guvna | omg have you heard about the latest craze that the people are talking about? |   000065 |    000186a1 | 6f9619ff-8b86-d011-b42d-00c04fc964ff | <foo>bar</foo> \n          |       |           |       |         |           |            |              |           |         |                   |            |                             |                             |                                    |                             |                  |        |            |                                                                                 |         |            |                                                                              |          |             |                                      |                \n(2 rows)",
	},
	{
		name: "mssql wide_table to mysql",
		transfer: data.Transfer{
			Source:            mssqlTestSource,
			Target:            mysqlTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: mysqlTestSource,
		targetTable:       "mssql_wide_table",
		checkQuery:        "select * from mssql_wide_table;",
		checkResult:       " mybigint | mybit | mydecimal | myint | mymoney | mynumeric | mysmallint | mysmallmoney | mytinyint | myfloat |      myreal       |   mydate   |         mydatetime2         |      mydatetime      |          mydatetimeoffset          |   mysmalldatetime    |      mytime      | mychar | myvarchar  |                                     mytext                                      | mynchar | mynvarchar |                                   myntext                                    | mybinary | myvarbinary |          myuniqueidentifier          |     myxml      \n----------+-------+-----------+-------+---------+-----------+------------+--------------+-----------+---------+-------------------+------------+-----------------------------+----------------------+------------------------------------+----------------------+------------------+--------+------------+---------------------------------------------------------------------------------+---------+------------+------------------------------------------------------------------------------+----------+-------------+--------------------------------------+----------------\n   435345 |     1 |    324.43 |    54 |   43.21 |     54.33 |         12 |         22.1 |         4 |    45.5 | 47.70000076293945 | 2013-10-12 | 2005-06-12 11:40:17.6320000 | 2005-06-12T11:40:18Z | 2005-06-12 11:40:17.6320000 +01:00 | 2005-06-12T11:40:00Z | 11:40:12.5436540 |    yoo | gday guvna | omg have you hea'rd\" a,bout the latest craze that the people are talking about? |     yoo | gday guvna | omg have you heard about the latest craze that the people are talking about? |   000065 |    000186a1 | 6f9619ff-8b86-d011-b42d-00c04fc964ff | <foo>bar</foo> \n          |       |           |       |         |           |            |              |           |         |                   |            |                             |                      |                                    |                      |                  |        |            |                                                                                 |         |            |                                                                              |          |             |                                      |                \n(2 rows)",
	},
	{
		name: "mssql wide_table to snowflake",
		transfer: data.Transfer{
			Source:            mssqlTestSource,
			Target:            snowflakeTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: snowflakeTestSource,
		targetTable:       "mssql_wide_table",
		checkQuery:        "select * from mssql_wide_table;",
		checkResult:       " MYBIGINT | MYBIT | MYDECIMAL | MYINT | MYMONEY | MYNUMERIC | MYSMALLINT | MYSMALLMONEY | MYTINYINT | MYFLOAT |      MYREAL       |   MYDATE   |         MYDATETIME2         |        MYDATETIME        |          MYDATETIMEOFFSET          |   MYSMALLDATETIME    |      MYTIME      | MYCHAR | MYVARCHAR  |                                     MYTEXT                                      | MYNCHAR | MYNVARCHAR |                                   MYNTEXT                                    | MYBINARY | MYVARBINARY |          MYUNIQUEIDENTIFIER          |     MYXML      \n----------+-------+-----------+-------+---------+-----------+------------+--------------+-----------+---------+-------------------+------------+-----------------------------+--------------------------+------------------------------------+----------------------+------------------+--------+------------+---------------------------------------------------------------------------------+---------+------------+------------------------------------------------------------------------------+----------+-------------+--------------------------------------+----------------\n   435345 |  true |    324.43 |    54 |   43.21 |     54.33 |         12 |         22.1 |         4 |    45.5 | 47.70000076293945 | 2013-10-12 | 2005-06-12 11:40:17.6320000 | 2005-06-12T11:40:17.633Z | 2005-06-12 11:40:17.6320000 +01:00 | 2005-06-12T11:40:00Z | 11:40:12.5436540 |    yoo | gday guvna | omg have you hea'rd\" a,bout the latest craze that the people are talking about? |     yoo | gday guvna | omg have you heard about the latest craze that the people are talking about? |      \x00\x00e |        \x00\x01\x86\xa1 | 6f9619ff-8b86-d011-b42d-00c04fc964ff | <foo>bar</foo> \n          |       |           |       |         |           |            |              |           |         |                   |            |                             |                          |                                    |                      |                  |        |            |                                                                                 |         |            |                                                                              |          |             |                                      |                \n(2 rows)",
	},
	// MySQL source
	{
		name: "mysql wide_table to postgresql",
		transfer: data.Transfer{
			Source:            mysqlTestSource,
			Target:            postgresqlTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: postgresqlTestSource,
		targetTable:       "mysql_wide_table",
		checkQuery:        "select * from mysql_wide_table;",
		checkResult:       " myserial | mybit | mybit5 |     mybit64      | mytinyint | mysmallint | mymediumint |  myint  |   mybigint   | mydecimal | myfloat | mydouble |        mydate        |        mytime        |      mydatetime      |     mytimestamp      | myyear | mychar |          myvarchar           | mynchar |         mynvarchar         | mybinary |                    myvarbinary                     |        mytinyblob        |       mymediumblob       |          myblob          |        mylongblob        |  mytinytext  |    mytext    | mymediumtext |  mylongtext  |  myenum  |  myset  |               myjson               \n----------+-------+--------+------------------+-----------+------------+-------------+---------+--------------+-----------+---------+----------+----------------------+----------------------+----------------------+----------------------+--------+--------+------------------------------+---------+----------------------------+----------+----------------------------------------------------+--------------------------+--------------------------+--------------------------+--------------------------+--------------+--------------+--------------+--------------+----------+---------+------------------------------------\n        1 |     1 |     0a | ffffffffffffffff |         2 |          5 |          50 | 4595435 | 392809438543 |      30.5 |    45.9 |     54.3 | 2009-05-28T00:00:00Z | 0001-01-01T14:23:54Z | 2010-10-24T20:52:52Z | 1989-02-22T03:17:21Z |   1905 |    chr | my varchar 'st\"ri,ng wheeeee |     ncr | my nvarchar string wheeeee |   626e72 | 6d792062696e61727920737472696e67207761686f6f6f6f6f | 626c6f622063697479206262 | 626c6f622063697479206262 | 626c6f622063697479206262 | 626c6f622063697479206262 | text city bb | text city bb | text city bb | text city bb | enumval1 | setval1 | (\"mykey\": \"this is\\\" m\\\"y, 'val'\") \n        2 |       |        |                  |           |            |             |         |              |           |         |          |                      |                      |                      |                      |        |        |                              |         |                            |          |                                                    |                          |                          |                          |                          |              |              |              |              |          |         |                                    \n(2 rows)",
	},
	{
		name: "mysql wide_table to mssql",
		transfer: data.Transfer{
			Source:            mysqlTestSource,
			Target:            mssqlTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: mssqlTestSource,
		targetTable:       "mysql_wide_table",
		checkQuery:        "select * from mysql_wide_table;",
		checkResult:       " myserial | mybit | mybit5 |     mybit64      | mytinyint | mysmallint | mymediumint |  myint  |     mybigint      | mydecimal | myfloat | mydouble |   mydate   |      mytime      |         mydatetime          |         mytimestamp         | myyear | mychar |          myvarchar           | mynchar |         mynvarchar         | mybinary |                    myvarbinary                     |        mytinyblob        |       mymediumblob       |          myblob          |        mylongblob        |  mytinytext  |    mytext    | mymediumtext |  mylongtext  |  myenum  |  myset  |               myjson               \n----------+-------+--------+------------------+-----------+------------+-------------+---------+-------------------+-----------+---------+----------+------------+------------------+-----------------------------+-----------------------------+--------+--------+------------------------------+---------+----------------------------+----------+----------------------------------------------------+--------------------------+--------------------------+--------------------------+--------------------------+--------------+--------------+--------------+--------------+----------+---------+------------------------------------\n        1 |  true |     0a | ffffffffffffffff |         2 |          5 |          50 | 4595435 | 3.92809438543e+11 |      30.5 |    45.9 |     54.3 | 2009-05-28 | 14:23:54.0000000 | 2010-10-24 20:52:52.0000000 | 1989-02-22 03:17:21.0000000 |   1905 |    chr | my varchar 'st\"ri,ng wheeeee |     ncr | my nvarchar string wheeeee |   626e72 | 6d792062696e61727920737472696e67207761686f6f6f6f6f | 626c6f622063697479206262 | 626c6f622063697479206262 | 626c6f622063697479206262 | 626c6f622063697479206262 | text city bb | text city bb | text city bb | text city bb | enumval1 | setval1 | (\"mykey\": \"this is\\\" m\\\"y, 'val'\") \n        2 |       |        |                  |           |            |             |         |                   |           |         |          |            |                  |                             |                             |        |        |                              |         |                            |          |                                                    |                          |                          |                          |                          |              |              |              |              |          |         |                                    \n(2 rows)",
	},
	{
		name: "mysql wide_table to mysql",
		transfer: data.Transfer{
			Source:            mysqlTestSource,
			Target:            mysqlTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: mysqlTestSource,
		targetTable:       "mysql_wide_table",
		checkQuery:        "select * from mysql_wide_table;",
		checkResult:       " myserial | mybit | mybit5 |     mybit64      | mytinyint | mysmallint | mymediumint |  myint  |   mybigint   | mydecimal | myfloat | mydouble |        mydate        |        mytime        |      mydatetime      |     mytimestamp      | myyear | mychar |          myvarchar           | mynchar |         mynvarchar         | mybinary |                    myvarbinary                     |        mytinyblob        |       mymediumblob       |          myblob          |        mylongblob        |  mytinytext  |    mytext    | mymediumtext |  mylongtext  |  myenum  |  myset  |              myjson              \n----------+-------+--------+------------------+-----------+------------+-------------+---------+--------------+-----------+---------+----------+----------------------+----------------------+----------------------+----------------------+--------+--------+------------------------------+---------+----------------------------+----------+----------------------------------------------------+--------------------------+--------------------------+--------------------------+--------------------------+--------------+--------------+--------------+--------------+----------+---------+----------------------------------\n        1 |     1 |     0a | ffffffffffffffff |         2 |          5 |          50 | 4595435 | 392809438543 |      30.5 |    45.9 |     54.3 | 2009-05-28T00:00:00Z | 0001-01-01T14:23:54Z | 2010-10-24T20:52:52Z | 1989-02-22T03:17:21Z |   1905 |    chr | my varchar 'st\"ri,ng wheeeee |     ncr | my nvarchar string wheeeee |   626e72 | 6d792062696e61727920737472696e67207761686f6f6f6f6f | 626c6f622063697479206262 | 626c6f622063697479206262 | 626c6f622063697479206262 | 626c6f622063697479206262 | text city bb | text city bb | text city bb | text city bb | enumval1 | setval1 | (\"mykey\": \"this is\" m\"y, 'val'\") \n        2 |       |        |                  |           |            |             |         |              |           |         |          |                      |                      |                      |                      |        |        |                              |         |                            |          |                                                    |                          |                          |                          |                          |              |              |              |              |          |         |                                  \n(2 rows)",
	},
	{
		name: "mysql wide_table to snowflake",
		transfer: data.Transfer{
			Source:            mysqlTestSource,
			Target:            snowflakeTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: snowflakeTestSource,
		targetTable:       "mysql_wide_table",
		checkQuery:        "select * from mysql_wide_table;",
		checkResult:       " MYSERIAL | MYBIT | MYBIT5 | MYBIT64  | MYTINYINT | MYSMALLINT | MYMEDIUMINT |    MYINT     |     MYBIGINT      | MYDECIMAL | MYFLOAT | MYDOUBLE |        MYDATE        |        MYTIME        |      MYDATETIME      |     MYTIMESTAMP      | MYYEAR | MYCHAR |          MYVARCHAR           | MYNCHAR |         MYNVARCHAR         | MYBINARY |        MYVARBINARY        |  MYTINYBLOB  | MYMEDIUMBLOB |    MYBLOB    |  MYLONGBLOB  |  MYTINYTEXT  |    MYTEXT    | MYMEDIUMTEXT |  MYLONGTEXT  |  MYENUM  |  MYSET  |              MYJSON              \n----------+-------+--------+----------+-----------+------------+-------------+--------------+-------------------+-----------+---------+----------+----------------------+----------------------+----------------------+----------------------+--------+--------+------------------------------+---------+----------------------------+----------+---------------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+----------+---------+----------------------------------\n        1 |  true |      \n | \xff\xff\xff\xff\xff\xff\xff\xff |         2 |          5 |          50 | 4.595435e+06 | 3.92809438543e+11 |      30.5 |    45.9 |     54.3 | 2009-05-28T00:00:00Z | 0001-01-01T14:23:54Z | 2010-10-24T20:52:52Z | 1989-02-22T03:17:21Z |   1905 |    chr | my varchar 'st\"ri,ng wheeeee |     ncr | my nvarchar string wheeeee |      bnr | my binary string wahooooo | blob city bb | blob city bb | blob city bb | blob city bb | text city bb | text city bb | text city bb | text city bb | enumval1 | setval1 | (\"mykey\": \"this is\" m\"y, 'val'\") \n        2 |       |        |          |           |            |             |              |                   |           |         |          |                      |                      |                      |                      |        |        |                              |         |                            |          |                           |              |              |              |              |              |              |              |              |          |         |                                  \n(2 rows)",
	},
	{
		name: "snowflake wide_table to postgresql",
		transfer: data.Transfer{
			Source:            snowflakeTestSource,
			Target:            postgresqlTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: postgresqlTestSource,
		targetTable:       "snowflake_wide_table",
		checkQuery:        "select * from snowflake_wide_table;",
		checkResult:       " mynumber | myint | myfloat |              myvarchar              | mybinary | myboolean |        mydate        |        mytime        |       mytimestamp_ltz       |       mytimestamp_ntz       |       mytimestamp_tz        |              myvariant              |                  myobject                  |                                             myarray                                              |                             mygeography                              \n----------+-------+---------+-------------------------------------+----------+-----------+----------------------+----------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------------+--------------------------------------------+--------------------------------------------------------------------------------------------------+----------------------------------------------------------------------\n     25.5 |    22 |    42.5 | hellooooo h'er\"es ,my varchar value |     0011 |         1 | 2000-10-15T00:00:00Z | 0001-01-01T23:54:01Z | 2000-10-16T06:54:01.345673Z | 2000-10-15T23:54:01.345673Z | 2000-10-15T22:54:01.345673Z | (\n  \"mykey\": \"this is \\\"my' v,al\"\n) | (\n  \"key3\": \"value3\",\n  \"key4\": \"value4\"\n) | [\n  true,\n  1,\n  -1.200000000000000e-03,\n  \"Abc\",\n  [\n    \"x\",\n    \"y\"\n  ],\n  (\n    \"a\": 1\n  )\n] | (\n  \"coordinates\": [\n    -122.35,\n    37.55\n  ],\n  \"type\": \"Point\"\n) \n          |       |         |                                     |          |           |                      |                      |                             |                             |                             |                                     |                                            |                                                                                                  |                                                                      \n(2 rows)",
	},
	{
		name: "snowflake wide_table to mysql",
		transfer: data.Transfer{
			Source:            snowflakeTestSource,
			Target:            mysqlTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: mysqlTestSource,
		targetTable:       "snowflake_wide_table",
		checkQuery:        "select * from snowflake_wide_table;",
		checkResult:       " MYNUMBER | MYINT | MYFLOAT |              MYVARCHAR              | MYBINARY | MYBOOLEAN |        MYDATE        |        MYTIME        |   MYTIMESTAMP_LTZ    |   MYTIMESTAMP_NTZ    |    MYTIMESTAMP_TZ    |             MYVARIANT              |                  MYOBJECT                  |                                             MYARRAY                                              |                             MYGEOGRAPHY                              \n----------+-------+---------+-------------------------------------+----------+-----------+----------------------+----------------------+----------------------+----------------------+----------------------+------------------------------------+--------------------------------------------+--------------------------------------------------------------------------------------------------+----------------------------------------------------------------------\n     25.5 |    22 |    42.5 | hellooooo h'er\"es ,my varchar value |     0011 |         1 | 2000-10-15T00:00:00Z | 0001-01-01T23:54:01Z | 2000-10-16T06:54:01Z | 2000-10-15T23:54:01Z | 2000-10-15T22:54:01Z | (\n  \"mykey\": \"this is \"my' v,al\"\n) | (\n  \"key3\": \"value3\",\n  \"key4\": \"value4\"\n) | [\n  true,\n  1,\n  -1.200000000000000e-03,\n  \"Abc\",\n  [\n    \"x\",\n    \"y\"\n  ],\n  (\n    \"a\": 1\n  )\n] | (\n  \"coordinates\": [\n    -122.35,\n    37.55\n  ],\n  \"type\": \"Point\"\n) \n          |       |         |                                     |          |           |                      |                      |                      |                      |                      |                                    |                                            |                                                                                                  |                                                                      \n(2 rows)",
	},
	{
		name: "snowflake wide_table to mssql",
		transfer: data.Transfer{
			Source:            snowflakeTestSource,
			Target:            mssqlTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: mssqlTestSource,
		targetTable:       "snowflake_wide_table",
		checkQuery:        "select * from snowflake_wide_table;",
		checkResult:       " MYNUMBER | MYINT | MYFLOAT |              MYVARCHAR              | MYBINARY | MYBOOLEAN |   MYDATE   |      MYTIME      |       MYTIMESTAMP_LTZ       |       MYTIMESTAMP_NTZ       |       MYTIMESTAMP_TZ        |              MYVARIANT              |                  MYOBJECT                  |                                             MYARRAY                                              |                             MYGEOGRAPHY                              \n----------+-------+---------+-------------------------------------+----------+-----------+------------+------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------------+--------------------------------------------+--------------------------------------------------------------------------------------------------+----------------------------------------------------------------------\n     25.5 |    22 |    42.5 | hellooooo h'er\"es ,my varchar value |     0011 |      true | 2000-10-15 | 23:54:01.0000000 | 2000-10-16 06:54:01.3456730 | 2000-10-15 23:54:01.3456730 | 2000-10-15 22:54:01.3456730 | (\n  \"mykey\": \"this is \\\"my' v,al\"\n) | (\n  \"key3\": \"value3\",\n  \"key4\": \"value4\"\n) | [\n  true,\n  1,\n  -1.200000000000000e-03,\n  \"Abc\",\n  [\n    \"x\",\n    \"y\"\n  ],\n  (\n    \"a\": 1\n  )\n] | (\n  \"coordinates\": [\n    -122.35,\n    37.55\n  ],\n  \"type\": \"Point\"\n) \n          |       |         |                                     |          |           |            |                  |                             |                             |                             |                                     |                                            |                                                                                                  |                                                                      \n(2 rows)",
	},
	{
		name: "snowflake wide_table to snowflake",
		transfer: data.Transfer{
			Source:            snowflakeTestSource,
			Target:            snowflakeTestTarget,
			Query:             "select * from wide_table",
			DropTargetTable:   true,
			CreateTargetTable: true,
		},
		targetCheckSource: snowflakeTestSource,
		targetTable:       "snowflake_wide_table",
		checkQuery:        "select * from snowflake_wide_table;",
		checkResult:       " MYNUMBER | MYINT | MYFLOAT |              MYVARCHAR              | MYBINARY | MYBOOLEAN |        MYDATE        |        MYTIME        |       MYTIMESTAMP_LTZ       |       MYTIMESTAMP_NTZ       |       MYTIMESTAMP_TZ        |             MYVARIANT              |                  MYOBJECT                  |                                             MYARRAY                                              |                             MYGEOGRAPHY                              \n----------+-------+---------+-------------------------------------+----------+-----------+----------------------+----------------------+-----------------------------+-----------------------------+-----------------------------+------------------------------------+--------------------------------------------+--------------------------------------------------------------------------------------------------+----------------------------------------------------------------------\n     25.5 |    22 |    42.5 | hellooooo h'er\"es ,my varchar value |       \x00\x11 |      true | 2000-10-15T00:00:00Z | 0001-01-01T23:54:01Z | 2000-10-16T06:54:01.345673Z | 2000-10-15T23:54:01.345673Z | 2000-10-15T22:54:01.345673Z | (\n  \"mykey\": \"this is \"my' v,al\"\n) | (\n  \"key3\": \"value3\",\n  \"key4\": \"value4\"\n) | [\n  true,\n  1,\n  -1.200000000000000e-03,\n  \"Abc\",\n  [\n    \"x\",\n    \"y\"\n  ],\n  (\n    \"a\": 1\n  )\n] | (\n  \"coordinates\": [\n    -122.35,\n    37.55\n  ],\n  \"type\": \"Point\"\n) \n          |       |         |                                     |          |           |                      |                      |                             |                             |                             |                                    |                                            |                                                                                                  |                                                                      \n(2 rows)",
	},
}

func TestTransfers(t *testing.T) {
	ctx := context.Background()
	var err error

	for _, tt := range transferTests {
		tt.transfer.Source.Db, err = sql.Open(
			"odbc",
			tt.transfer.Source.OdbcDsn,
		)
		if err != nil {
			t.Fatalf("unable to create transfer source db, err: %v\n", err)
		}

		tt.transfer.Target.Db, err = sql.Open(
			"odbc",
			tt.transfer.Target.OdbcDsn,
		)
		if err != nil {
			t.Fatalf("unable to create transfer target db, err: %v\n", err)
		}

		tt.targetCheckSource.Db, err = sql.Open(
			"odbc",
			tt.transfer.Target.OdbcDsn,
		)
		if err != nil {
			t.Fatalf("unable to create target check source db, err: %v\n", err)
		}

		tt.transfer.Target.Table = tt.targetTable

		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := transfers.RunTransfer(
				ctx,
				tt.transfer,
			)

			if err != nil {
				t.Fatalf("unable to run transfer. err:\n\n%v\n", err)
			}

			if tt.checkQuery != "" {
				rows, err := tt.targetCheckSource.Db.QueryContext(ctx, tt.checkQuery)
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
