package engine

import (
	"os"
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

var queryTests = []queryTest{
	// PostgreSQL Setup
	{
		name:                  "postgresqlWideTableDrop",
		connection:            postgresqlTestConnection,
		testQuery:             "drop table if exists wide_table;",
		checkQuery:            "select * from wide_table",
		expectedErr:           "db.Query() threw an error",
		expectedErrProperties: map[string]string{"dsType": "postgresql", "error": `ERROR: relation "wide_table" does not exist (SQLSTATE 42P01)`, "query": "select * from wide_table"},
	},
	{
		name:        "postgresqlWideTableCreate",
		connection:  postgresqlTestConnection,
		testQuery:   `create table wide_table(mybigint bigint, mybit bit(5), mybitvarying varbit, myboolean boolean, mybox box, mybytea bytea, mychar char(3), myvarchar varchar(100), mycidr cidr, mycircle circle, mydate date, mydoubleprecision double precision, myinet inet, myinteger integer, myinterval interval, myjson json, myjsonb jsonb, myline line, mylseg lseg, mymacaddr macaddr, mymoney money, mynumeric numeric(10,5), mypath path, mypg_lsn pg_lsn, mypoint point, mypolygon polygon, myreal real, mysmallint smallint, mytext text, mytime time, mytimetz timetz, mytimestamp timestamp, mytimestamptz timestamptz, mytsquery tsquery, mytsvector tsvector, myuuid uuid, myxml xml);`,
		checkQuery:  "select * from wide_table",
		checkResult: QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybit": "BIT", "mybitvarying": "VARBIT", "myboolean": "BOOL", "mybox": "BOX", "mybytea": "BYTEA", "mychar": "BPCHAR", "mycidr": "CIDR", "mycircle": "CIRCLE", "mydate": "DATE", "mydoubleprecision": "FLOAT8", "myinet": "INET", "myinteger": "INT4", "myinterval": "INTERVAL", "myjson": "JSON", "myjsonb": "JSONB", "myline": "LINE", "mylseg": "LSEG", "mymacaddr": "MACADDR", "mymoney": "790", "mynumeric": "NUMERIC", "mypath": "PATH", "mypg_lsn": "3220", "mypoint": "POINT", "mypolygon": "POLYGON", "myreal": "FLOAT4", "mysmallint": "INT2", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytimestamptz": "TIMESTAMPTZ", "mytimetz": "1266", "mytsquery": "3615", "mytsvector": "3614", "myuuid": "UUID", "myvarchar": "VARCHAR", "myxml": "142"}, Rows: []interface{}{}},
	},
	{
		name:        "postgresqlWideTableInsert",
		connection:  postgresqlTestConnection,
		testQuery:   `INSERT INTO wide_table(mybigint, mybit, mybitvarying, myboolean, mybox, mybytea, mychar, myvarchar, mycidr, mycircle, mydate, mydoubleprecision, myinet, myinteger, myinterval, myjson, myjsonb, myline, mylseg, mymacaddr, mymoney , mynumeric, mypath, mypg_lsn, mypoint, mypolygon, myreal, mysmallint, mytext, mytime, mytimetz, mytimestamp, mytimestamptz, mytsquery, mytsvector, myuuid, myxml) values (6514798382812790784, B'10001', B'1001', true, '(8,9), (1,3)', '\xAAAABBBB', 'abc', '"my"varch''ar,123@gmail.com', '192.168.100.128/25', '(( 1 , 5 ), 5)', '2014-01-10 20:14:54.140332'::date, 529.56218983375436, '192.168.100.128', 745910651, (timestamptz '2014-01-20 20:00:00 PST' - timestamptz '2014-01-10 10:00:00 PST'), '{"mykey": "this\"  ''is'' m,y val"}', '{"mykey": "this is my val"}', '{1, 5, 20}', '[(5, 4), (2, 1)]', '08:00:2b:01:02:03', '$35,244.33'::money, 449.82115, '[( 1, 4), (8, 7)]', '16/B374D848'::pg_lsn, '(5, 7)', '((5, 8), (6, 10), (7, 20))', 9673.1094, 24345, 'myte",xt123@gmail.com', '03:46:38.765594+05', '03:46:38.765594+05', '2014-01-10 10:05:04 PST', '2014-01-10 10:05:04 PST', 'fat & rat'::tsquery, 'a fat cat sat on a mat and ate a fat rat'::tsvector, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid, '<foo>bar</foo>'),(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);`,
		checkQuery:  "select * from wide_table",
		checkResult: QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybit": "BIT", "mybitvarying": "VARBIT", "myboolean": "BOOL", "mybox": "BOX", "mybytea": "BYTEA", "mychar": "BPCHAR", "mycidr": "CIDR", "mycircle": "CIRCLE", "mydate": "DATE", "mydoubleprecision": "FLOAT8", "myinet": "INET", "myinteger": "INT4", "myinterval": "INTERVAL", "myjson": "JSON", "myjsonb": "JSONB", "myline": "LINE", "mylseg": "LSEG", "mymacaddr": "MACADDR", "mymoney": "790", "mynumeric": "NUMERIC", "mypath": "PATH", "mypg_lsn": "3220", "mypoint": "POINT", "mypolygon": "POLYGON", "myreal": "FLOAT4", "mysmallint": "INT2", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytimestamptz": "TIMESTAMPTZ", "mytimetz": "1266", "mytsquery": "3615", "mytsvector": "3614", "myuuid": "UUID", "myvarchar": "VARCHAR", "myxml": "142"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "true", "'(8,9),(1,3)'", "'\\xaaaabbbb'", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "'2014-01-10 00:00:00.000000 +0000'", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\\\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.109375", "24345", "'myte\",xt123@gmail.com'", "'03:46:38.765594'", "'03:46:38.765594+05'", "'2014-01-10 10:05:04.000000 +0000'", "'2014-01-10 18:05:04.000000 +0000'", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!t(<nil>)", "'%!s(<nil>)'", "'\\x%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	},
	// MySQL Setup
	{
		name:                  "mysqlWideTableDrop",
		connection:            mysqlTestConnection,
		testQuery:             "drop table if exists wide_table;",
		checkQuery:            "select * from wide_table",
		expectedErr:           "db.Query() threw an error",
		expectedErrProperties: map[string]string{"dsType": "mysql", "error": "Error 1146: Table 'testing.wide_table' doesn't exist", "query": "select * from wide_table"},
	},
	{
		name:        "mysqlWideTableCreate",
		connection:  mysqlTestConnection,
		testQuery:   `create table wide_table(myserial serial, mybit bit, mybit5 bit(5), mybit64 bit(64), mytinyint tinyint, mysmallint smallint, mymediumint mediumint, myint int, mybigint bigint, mydecimal decimal(10, 5), myfloat float, mydouble double, mydate date, mytime time, mydatetime datetime, mytimestamp timestamp, myyear year, mychar char(3), myvarchar varchar(200), mynchar nchar(3), mynvarchar nvarchar(200), mybinary binary(3), myvarbinary varbinary(200), mytinyblob tinyblob, mymediumblob mediumblob, myblob blob, mylongblob longblob, mytinytext tinytext, mytext text, mymediumtext mediumtext, mylongtext longtext, myenum ENUM('enumval1', 'enumval2'), myset SET('setval1', 'setval2'), mygeometry geometry, mypoint point, mylinestring linestring, mypolygon polygon, mymultipoint multipoint, mymultilinestring multilinestring, mymultipolygon multipolygon, mygeometrycollection geometrycollection, myjson json);`,
		checkQuery:  "select * from wide_table",
		checkResult: QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybinary": "BINARY", "mybit": "BIT", "mybit5": "BIT", "mybit64": "BIT", "myblob": "BLOB", "mychar": "CHAR", "mydate": "DATE", "mydatetime": "DATETIME", "mydecimal": "DECIMAL", "mydouble": "DOUBLE", "myenum": "CHAR", "myfloat": "FLOAT", "mygeometry": "GEOMETRY", "mygeometrycollection": "GEOMETRY", "myint": "INT", "myjson": "JSON", "mylinestring": "GEOMETRY", "mylongblob": "BLOB", "mylongtext": "TEXT", "mymediumblob": "BLOB", "mymediumint": "MEDIUMINT", "mymediumtext": "TEXT", "mymultilinestring": "GEOMETRY", "mymultipoint": "GEOMETRY", "mymultipolygon": "GEOMETRY", "mynchar": "CHAR", "mynvarchar": "VARCHAR", "mypoint": "GEOMETRY", "mypolygon": "GEOMETRY", "myserial": "BIGINT", "myset": "CHAR", "mysmallint": "SMALLINT", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytinyblob": "BLOB", "mytinyint": "TINYINT", "mytinytext": "TEXT", "myvarbinary": "VARBINARY", "myvarchar": "VARCHAR", "myyear": "YEAR"}, Rows: []interface{}{}},
	},
	{
		name:        "mysqlWideTableInsert",
		connection:  mysqlTestConnection,
		testQuery:   `insert into wide_table (mybit, mybit5, mybit64, mytinyint, mysmallint, mymediumint, myint, mybigint, mydecimal, myfloat, mydouble, mydate, mytime, mydatetime, mytimestamp, myyear, mychar, myvarchar, mynchar, mynvarchar, mybinary, myvarbinary, mytinyblob, mymediumblob, myblob, mylongblob, mytinytext, mytext, mymediumtext, mylongtext, myenum, myset, mygeometry, mypoint, mylinestring, mypolygon, mymultipoint, mymultilinestring, mymultipolygon, mygeometrycollection, myjson) VALUES (1, b'01010', b'1111111111111111111111111111111111111111111111111111111111111111', 2, 5, 50, 4595435, 392809438543, 30.5, 45.9, 54.3, '2009-05-28', '14:23:54.105302', '2010-10-24 20:52:51.969491', '1989-02-22 3:17:21.243061', 1905, 'chr', 'my varchar ''st"ri,ng wheeeee', 'ncr', 'my nvarchar string wheeeee', 'bnr', 'my binary string wahooooo', 'blob city bb', 'blob city bb', 'blob city bb', 'blob city bb', 'text city bb', 'text city bb', 'text city bb', 'text city bb', 'enumval1', 'setval1', ST_GeomFromText('POINT(1 1)'), ST_GeomFromText('POINT(1 1)'), ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'), ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))'), ST_GEOMFROMTEXT('MultiPoint( 1 1, 2 2, 5 3, 7 2, 9 3, 8 4, 6 6, 6 9, 4 9, 1 5 )'), ST_GEOMFROMTEXT('MultiLineString((1 1,2 2,3 3),(4 4,5 5))'), ST_GEOMFROMTEXT('MultiPolygon(((0 0,0 3,3 3,3 0,0 0),(1 1,1 2,2 2,2 1,1 1)))'), ST_GEOMFROMTEXT('MultiPolygon(((0 0,0 3,3 3,3 0,0 0),(1 1,1 2,2 2,2 1,1 1)))'), '{"mykey": "this is\\" m\\"y, ''val''"}'),(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);`,
		checkQuery:  "select * from wide_table",
		checkResult: QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybinary": "BINARY", "mybit": "BIT", "mybit5": "BIT", "mybit64": "BIT", "myblob": "BLOB", "mychar": "CHAR", "mydate": "DATE", "mydatetime": "DATETIME", "mydecimal": "DECIMAL", "mydouble": "DOUBLE", "myenum": "CHAR", "myfloat": "FLOAT", "mygeometry": "GEOMETRY", "mygeometrycollection": "GEOMETRY", "myint": "INT", "myjson": "JSON", "mylinestring": "GEOMETRY", "mylongblob": "BLOB", "mylongtext": "TEXT", "mymediumblob": "BLOB", "mymediumint": "MEDIUMINT", "mymediumtext": "TEXT", "mymultilinestring": "GEOMETRY", "mymultipoint": "GEOMETRY", "mymultipolygon": "GEOMETRY", "mynchar": "CHAR", "mynvarchar": "VARCHAR", "mypoint": "GEOMETRY", "mypolygon": "GEOMETRY", "myserial": "BIGINT", "myset": "CHAR", "mysmallint": "SMALLINT", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytinyblob": "BLOB", "mytinyint": "TINYINT", "mytinytext": "TEXT", "myvarbinary": "VARBINARY", "myvarchar": "VARCHAR", "myyear": "YEAR"}, Rows: []interface{}{"1", "x'01'", "x'0a'", "x'ffffffffffffffff'", "2", "5", "50", "4595435", "392809438543", "30.50000", "45.9", "54.3", "'2009-05-28'", "'14:23:54'", "'2010-10-24 20:52:52'", "'1989-02-22 03:17:21'", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "x'626e72'", "x'6d792062696e61727920737472696e67207761686f6f6f6f6f'", "x'626c6f622063697479206262'", "x'626c6f622063697479206262'", "x'626c6f622063697479206262'", "x'626c6f622063697479206262'", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "'\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'\x00\x00\x00\x00\x01\x02\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@'", "'\x00\x00\x00\x00\x01\x03\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@'", "'\x00\x00\x00\x00\x01\x04\x00\x00\x00\n\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\b@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x00@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\"@\x00\x00\x00\x00\x00\x00\b@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00 @\x00\x00\x00\x00\x00\x00\x10@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x18@\x00\x00\x00\x00\x00\x00\x18@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x18@\x00\x00\x00\x00\x00\x00\"@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\"@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x14@'", "'\x00\x00\x00\x00\x01\x05\x00\x00\x00\x02\x00\x00\x00\x01\x02\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@'", "'\x00\x00\x00\x00\x01\x06\x00\x00\x00\x01\x00\x00\x00\x01\x03\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'\x00\x00\x00\x00\x01\x06\x00\x00\x00\x01\x00\x00\x00\x01\x03\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'{\"mykey\": \"this is\\\\\" m\\\\\"y, ''val''\"}'", "2", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
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
		expectedErrProperties: map[string]string{"dsType": "mssql", "error": "mssql: Invalid object name 'wide_table'.", "query": "select * from wide_table"},
	},
	{
		name:        "mssqlWideTableCreate",
		connection:  mssqlTestConnection,
		testQuery:   `create table wide_table (mybigint bigint, mybit bit, mydecimal decimal(10,5), myint int, mymoney money, mynumeric numeric(11,7), mysmallint smallint, mysmallmoney smallmoney, mytinyint tinyint, myfloat float, myreal real, mydate date, mydatetime2 datetime2, mydatetime datetime, mydatetimeoffset datetimeoffset, mysmalldatetime smalldatetime, mytime time, mychar char(3), myvarchar varchar(20), mytext text, mynchar nchar(3), mynvarchar nvarchar(20), myntext ntext, mybinary binary(3), myvarbinary varbinary(30), myuniqueidentifier uniqueidentifier, myxml xml);`,
		checkQuery:  "select * from wide_table",
		checkResult: QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybinary": "BINARY", "mybit": "BIT", "mychar": "CHAR", "mydate": "DATE", "mydatetime": "DATETIME", "mydatetime2": "DATETIME2", "mydatetimeoffset": "DATETIMEOFFSET", "mydecimal": "DECIMAL", "myfloat": "FLOAT", "myint": "INT", "mymoney": "MONEY", "mynchar": "NCHAR", "myntext": "NTEXT", "mynumeric": "DECIMAL", "mynvarchar": "NVARCHAR", "myreal": "REAL", "mysmalldatetime": "SMALLDATETIME", "mysmallint": "SMALLINT", "mysmallmoney": "SMALLMONEY", "mytext": "TEXT", "mytime": "TIME", "mytinyint": "TINYINT", "myuniqueidentifier": "UNIQUEIDENTIFIER", "myvarbinary": "VARBINARY", "myvarchar": "VARCHAR", "myxml": "XML"}, Rows: []interface{}{}},
	},
	{
		name:        "mssqlWideTableInsert",
		connection:  mssqlTestConnection,
		testQuery:   `insert into wide_table (mybigint, mybit, mydecimal, myint, mymoney, mynumeric, mysmallint, mysmallmoney, mytinyint, myfloat, myreal, mydate, mydatetime2, mydatetime, mydatetimeoffset, mysmalldatetime, mytime, mychar, myvarchar, mytext, mynchar, mynvarchar, myntext, mybinary, myvarbinary, myuniqueidentifier, myxml) values(435345, 1, 324.43, 54, 43.21, 54.33, 12, 22.10, 4, 45.5, 47.7, '2013-10-12', CAST('2005-06-12 11:40:17.632' AS datetime2), CAST('2005-06-12 11:40:17.632' AS datetime), CAST('2005-06-12 11:40:17.632 +01:00' AS datetimeoffset), CAST('2005-06-12 11:40:00' AS smalldatetime), CAST('11:40:12.543654' AS time), 'yoo', 'gday guvna', 'omg have you hea''rd" a,bout the latest craze that the people are talking about?', 'yoo', 'gday guvna', 'omg have you heard about the latest craze that the people are talking about?', 101, 100001, N'6F9619FF-8B86-D011-B42D-00C04FC964FF','<foo>bar</foo>'),(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);`,
		checkQuery:  "select * from wide_table",
		checkResult: QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybinary": "BINARY", "mybit": "BIT", "mychar": "CHAR", "mydate": "DATE", "mydatetime": "DATETIME", "mydatetime2": "DATETIME2", "mydatetimeoffset": "DATETIMEOFFSET", "mydecimal": "DECIMAL", "myfloat": "FLOAT", "myint": "INT", "mymoney": "MONEY", "mynchar": "NCHAR", "myntext": "NTEXT", "mynumeric": "DECIMAL", "mynvarchar": "NVARCHAR", "myreal": "REAL", "mysmalldatetime": "SMALLDATETIME", "mysmallint": "SMALLINT", "mysmallmoney": "SMALLMONEY", "mytext": "TEXT", "mytime": "TIME", "mytinyint": "TINYINT", "myuniqueidentifier": "UNIQUEIDENTIFIER", "myvarbinary": "VARBINARY", "myvarchar": "VARCHAR", "myxml": "XML"}, Rows: []interface{}{"435345", "1", "324.43000", "54", "'43.2100'", "54.3300000", "12", "'22.1000'", "4", "45.5", "47.70000076293945", "CONVERT(DATETIME2, '2013-10-12 00:00:00.0000000', 121)", "CONVERT(DATETIME2, '2005-06-12 11:40:17.6320000', 121)", "CONVERT(DATETIME2, '2005-06-12 11:40:17.6330000', 121)", "CONVERT(DATETIME2, '2005-06-12 11:40:17.6320000', 121)", "CONVERT(DATETIME2, '2005-06-12 11:40:00.0000000', 121)", "CONVERT(DATETIME2, '0001-01-01 11:40:12.5436540', 121)", "'yoo'", "'gday guvna'", "'omg have you hea''rd\" a,bout the latest craze that the people are talking about?'", "'yoo'", "'gday guvna'", "'omg have you heard about the latest craze that the people are talking about?'", "CONVERT(VARBINARY(8000), '0x000065', 1)", "CONVERT(VARBINARY(8000), '0x000186a1', 1)", "N'6F9619FF-8B86-D011-B42D-00C04FC964FF'", "'<foo>bar</foo>'", "%!d(<nil>)", "null", "%!s(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "%!s(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "null", "null", "null", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "null", "'%!s(<nil>)'"}},
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
		expectedErrProperties: map[string]string{"dsType": "redshift", "error": "ERROR: relation \"wide_table\" does not exist (SQLSTATE 42P01)", "query": "select * from wide_table"},
	},
	{
		name:        "redshiftWideTableCreate",
		connection:  redshiftTestConnection,
		testQuery:   `create table wide_table(mybigint bigint, myboolean boolean, mychar char(3), myvarchar varchar(100), mydate date, mydoubleprecision double precision, myinteger integer, mynumeric numeric(10,5), myreal real, mysmallint smallint, mytext text, mytime time, mytimetz timetz, mytimestamp timestamp, mytimestamptz timestamptz);`,
		checkQuery:  "select * from wide_table",
		checkResult: QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "myboolean": "BOOL", "mychar": "BPCHAR", "mydate": "DATE", "mydoubleprecision": "FLOAT8", "myinteger": "INT4", "mynumeric": "NUMERIC", "myreal": "FLOAT4", "mysmallint": "INT2", "mytext": "VARCHAR", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytimestamptz": "TIMESTAMPTZ", "mytimetz": "1266", "myvarchar": "VARCHAR"}, Rows: []interface{}{}},
	},
	{
		name:        "redshiftWideTableInsert",
		connection:  redshiftTestConnection,
		testQuery:   `insert into wide_table(mybigint, myboolean, mychar, myvarchar, mydate, mydoubleprecision, myinteger, mynumeric, myreal, mysmallint, mytext, mytime, mytimetz, mytimestamp, mytimestamptz) values (9223372036854775800, true, 'car', 'hey its ''  "varchar",', '2014-01-10', 435.544, 435345, 5466.453, 45.22, 3, 'helloooooo san diego', '03:46:38.765594+05', '03:46:38.765594+05', '2014-01-10 10:05:04 PST', '2014-01-10 10:05:04 PST'),(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);`,
		checkQuery:  "select * from wide_table",
		checkResult: QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "myboolean": "BOOL", "mychar": "BPCHAR", "mydate": "DATE", "mydoubleprecision": "FLOAT8", "myinteger": "INT4", "mynumeric": "NUMERIC", "myreal": "FLOAT4", "mysmallint": "INT2", "mytext": "VARCHAR", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytimestamptz": "TIMESTAMPTZ", "mytimetz": "1266", "myvarchar": "VARCHAR"}, Rows: []interface{}{"9223372036854775800", "true", "'car'", "'hey its ''  \"varchar\",'", "'2014-01-10 00:00:00.000000 +0000'", "435.544", "435345", "5466.45300", "45.220001220703125", "3", "'helloooooo san diego'", "'03:46:38.765594'", "'22:46:38.765594+00'", "'2014-01-10 10:05:04.000000 +0000'", "'2014-01-10 18:05:04.000000 +0000'", "%!d(<nil>)", "%!t(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "%!d(<nil>)", "%!s(<nil>)", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null"}},
	},
	// Snowflake Setup
	{
		name:                  "snowflakeWideTableDrop",
		connection:            snowflakeTestConnection,
		testQuery:             "drop table if exists public.wide_table;",
		checkQuery:            "select * from public.wide_table",
		expectedErr:           "db.Query() threw an error",
		expectedErrProperties: map[string]string{"dsType": "snowflake", "error": "002003 (42S02): SQL compilation error:\nObject 'TESTING.PUBLIC.WIDE_TABLE' does not exist or not authorized.", "query": "select * from public.wide_table"},
	},
	{
		name:        "snowflakeWideTableCreate",
		connection:  snowflakeTestConnection,
		testQuery:   `create table public.wide_table (mynumber number(10, 5), myint int, myfloat float, myvarchar varchar, mybinary binary, myboolean boolean, mydate date, mytime time, mytimestamp_ltz timestamp_ltz, mytimestamp_ntz timestamp_ntz, mytimestamp_tz timestamptz, myvariant variant, myobject object, myarray array, mygeography geography);`,
		checkQuery:  "select * from public.wide_table",
		checkResult: QueryResult{ColumnTypes: map[string]string{"MYARRAY": "ARRAY", "MYBINARY": "BINARY", "MYBOOLEAN": "BOOLEAN", "MYDATE": "DATE", "MYFLOAT": "REAL", "MYGEOGRAPHY": "OBJECT", "MYINT": "FIXED", "MYNUMBER": "FIXED", "MYOBJECT": "OBJECT", "MYTIME": "TIME", "MYTIMESTAMP_LTZ": "TIMESTAMP_LTZ", "MYTIMESTAMP_NTZ": "TIMESTAMP_NTZ", "MYTIMESTAMP_TZ": "TIMESTAMP_TZ", "MYVARCHAR": "TEXT", "MYVARIANT": "VARIANT"}, Rows: []interface{}{}},
	},
	{
		name:        "snowflakeWideTableInsert",
		connection:  snowflakeTestConnection,
		testQuery:   `insert into public.wide_table (mynumber, myint, myfloat, myvarchar, mybinary, myboolean, mydate, mytime, mytimestamp_ltz, mytimestamp_ntz, mytimestamp_tz, myvariant, myobject, myarray, mygeography) select column1, column2, column3, column4, column5, column6, column7, column8, column9, column10, column11, parse_json(column12), parse_json(column13), parse_json(column14), column15 from values (25.5, 22, 42.5, 'hellooooo h''er"es ,my varchar value', to_binary('0011'), true, '2000-10-15', '23:54:01', '2000-10-15 23:54:01.345673', '2000-10-15 23:54:01.345673', '2000-10-15 23:54:01.345673 +0100', '{"mykey": "this is \\"my'' v,al"}', '{"key3": "value3", "key4": "value4"}', '[true, 1, -1.2e-3, "Abc", ["x","y"], {"a":1}]', 'POINT(-122.35 37.55)'),(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);`,
		checkQuery:  "select * from wide_table",
		checkResult: QueryResult{ColumnTypes: map[string]string{"MYARRAY": "ARRAY", "MYBINARY": "BINARY", "MYBOOLEAN": "BOOLEAN", "MYDATE": "DATE", "MYFLOAT": "REAL", "MYGEOGRAPHY": "OBJECT", "MYINT": "FIXED", "MYNUMBER": "FIXED", "MYOBJECT": "OBJECT", "MYTIME": "TIME", "MYTIMESTAMP_LTZ": "TIMESTAMP_LTZ", "MYTIMESTAMP_NTZ": "TIMESTAMP_NTZ", "MYTIMESTAMP_TZ": "TIMESTAMP_TZ", "MYVARCHAR": "TEXT", "MYVARIANT": "VARIANT"}, Rows: []interface{}{"25.500000", "22", "42.500000", "'hellooooo h''er\"es ,my varchar value'", "to_binary('0011')", "true", "'2000-10-15 00:00:00.000000'", "'0001-01-01 23:54:01.000000'", "'2000-10-16 07:54:01.345673'", "'2000-10-15 23:54:01.345673'", "'2000-10-15 23:54:01.345673'", "'{\n  \"mykey\": \"this is \\\\\"my'' v,al\"\n}'", "'{\n  \"key3\": \"value3\",\n  \"key4\": \"value4\"\n}'", "'[\n  true,\n  1,\n  -1.200000000000000e-03,\n  \"Abc\",\n  [\n    \"x\",\n    \"y\"\n  ],\n  {\n    \"a\": 1\n  }\n]'", "'{\n  \"coordinates\": [\n    -122.35,\n    37.55\n  ],\n  \"type\": \"Point\"\n}'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "to_binary('%!x(<nil>)')", "%!t(<nil>)", "null", "null", "null", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	},
}

var transferTests = []transferTest{
	// // PostgreSQL Transfers
	// {
	// 	name:          "postgresql2postgresql_wide",
	// 	source:        postgresqlTestConnection,
	// 	target:        postgresqlTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "postgresql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from postgresql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybit": "VARBIT", "mybitvarying": "VARBIT", "myboolean": "BOOL", "mybox": "BOX", "mybytea": "BYTEA", "mychar": "VARCHAR", "mycidr": "CIDR", "mycircle": "CIRCLE", "mydate": "TIMESTAMPTZ", "mydoubleprecision": "FLOAT8", "myinet": "INET", "myinteger": "INT4", "myinterval": "INTERVAL", "myjson": "JSON", "myjsonb": "JSONB", "myline": "LINE", "mylseg": "LSEG", "mymacaddr": "MACADDR", "mymoney": "VARCHAR", "mynumeric": "FLOAT8", "mypath": "PATH", "mypg_lsn": "3220", "mypoint": "POINT", "mypolygon": "POLYGON", "myreal": "FLOAT4", "mysmallint": "INT4", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMPTZ", "mytimestamptz": "TIMESTAMPTZ", "mytimetz": "1266", "mytsquery": "3615", "mytsvector": "3614", "myuuid": "UUID", "myvarchar": "VARCHAR", "myxml": "142"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "true", "'(8,9),(1,3)'", "'\\xaaaabbbb'", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "'2014-01-10 00:00:00.000000 +0000'", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\\\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.109375", "24345", "'myte\",xt123@gmail.com'", "'03:46:38.765594'", "'03:46:38.765594+05'", "'2014-01-10 10:05:04.000000 +0000'", "'2014-01-10 18:05:04.000000 +0000'", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!t(<nil>)", "'%!s(<nil>)'", "'\\x%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "postgresql2mysql_wide",
	// 	source:        postgresqlTestConnection,
	// 	target:        mysqlTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "postgresql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from postgresql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybit": "TEXT", "mybitvarying": "TEXT", "myboolean": "TINYINT", "mybox": "TEXT", "mybytea": "BLOB", "mychar": "TEXT", "mycidr": "TEXT", "mycircle": "TEXT", "mydate": "DATETIME", "mydoubleprecision": "DOUBLE", "myinet": "TEXT", "myinteger": "INT", "myinterval": "TEXT", "myjson": "JSON", "myjsonb": "JSON", "myline": "TEXT", "mylseg": "TEXT", "mymacaddr": "TEXT", "mymoney": "TEXT", "mynumeric": "DOUBLE", "mypath": "TEXT", "mypg_lsn": "TEXT", "mypoint": "TEXT", "mypolygon": "TEXT", "myreal": "FLOAT", "mysmallint": "INT", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "DATETIME", "mytimestamptz": "DATETIME", "mytimetz": "TIME", "mytsquery": "TEXT", "mytsvector": "TEXT", "myuuid": "TEXT", "myvarchar": "VARCHAR", "myxml": "TEXT"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "1", "'(8,9),(1,3)'", "x'786161616162626262'", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "'2014-01-10 00:00:00'", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\\\\\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.11", "24345", "'myte\",xt123@gmail.com'", "'03:46:39'", "'03:46:39'", "'2014-01-10 10:05:04'", "'2014-01-10 18:05:04'", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "x'%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "postgresql2mssql_wide",
	// 	source:        postgresqlTestConnection,
	// 	target:        mssqlTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "postgresql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from postgresql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybit": "VARCHAR", "mybitvarying": "VARCHAR", "myboolean": "BIT", "mybox": "VARCHAR", "mybytea": "VARBINARY", "mychar": "NVARCHAR", "mycidr": "VARCHAR", "mycircle": "VARCHAR", "mydate": "DATETIME2", "mydoubleprecision": "FLOAT", "myinet": "VARCHAR", "myinteger": "INT", "myinterval": "VARCHAR", "myjson": "NVARCHAR", "myjsonb": "NVARCHAR", "myline": "VARCHAR", "mylseg": "VARCHAR", "mymacaddr": "VARCHAR", "mymoney": "VARCHAR", "mynumeric": "FLOAT", "mypath": "VARCHAR", "mypg_lsn": "VARCHAR", "mypoint": "VARCHAR", "mypolygon": "VARCHAR", "myreal": "REAL", "mysmallint": "INT", "mytext": "NTEXT", "mytime": "TIME", "mytimestamp": "DATETIME2", "mytimestamptz": "DATETIME2", "mytimetz": "VARCHAR", "mytsquery": "NVARCHAR", "mytsvector": "NVARCHAR", "myuuid": "UNIQUEIDENTIFIER", "myvarchar": "NVARCHAR", "myxml": "XML"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "1", "'(8,9),(1,3)'", "CONVERT(VARBINARY(8000), '0xaaaabbbb', 1)", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "CONVERT(DATETIME2, '2014-01-10 00:00:00.0000000', 121)", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\\\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.109375", "24345", "'myte\",xt123@gmail.com'", "CONVERT(DATETIME2, '0001-01-01 03:46:38.7655940', 121)", "'03:46:38.765594+05'", "CONVERT(DATETIME2, '2014-01-10 10:05:04.0000000', 121)", "CONVERT(DATETIME2, '2014-01-10 18:05:04.0000000', 121)", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "N'A0EEBC99-9CB-4EF8-BB6D-6BB9BD380A11'", "'<foo>bar</foo>'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "'%!s(<nil>)'", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "null", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "postgresql2oracle_wide",
	// 	source:        postgresqlTestConnection,
	// 	target:        oracleTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "postgresql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from postgresql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBIGINT": "NUMBER", "MYBIT": "NCHAR", "MYBITVARYING": "NCHAR", "MYBOOLEAN": "NUMBER", "MYBOX": "NCHAR", "MYBYTEA": "OCIBlobLocator", "MYCHAR": "NCHAR", "MYCIDR": "NCHAR", "MYCIRCLE": "NCHAR", "MYDATE": "TimeStampDTY", "MYDOUBLEPRECISION": "IBDouble", "MYINET": "NCHAR", "MYINTEGER": "NUMBER", "MYINTERVAL": "NCHAR", "MYJSON": "NCHAR", "MYJSONB": "NCHAR", "MYLINE": "NCHAR", "MYLSEG": "NCHAR", "MYMACADDR": "NCHAR", "MYMONEY": "NCHAR", "MYNUMERIC": "IBDouble", "MYPATH": "NCHAR", "MYPG_LSN": "NCHAR", "MYPOINT": "NCHAR", "MYPOLYGON": "NCHAR", "MYREAL": "IBFloat", "MYSMALLINT": "NUMBER", "MYTEXT": "NCHAR", "MYTIME": "NCHAR", "MYTIMESTAMP": "TimeStampDTY", "MYTIMESTAMPTZ": "TimeStampDTY", "MYTIMETZ": "NCHAR", "MYTSQUERY": "NCHAR", "MYTSVECTOR": "NCHAR", "MYUUID": "NCHAR", "MYVARCHAR": "NCHAR", "MYXML": "NCHAR"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "1", "'(8,9),(1,3)'", "hextoraw('aaaabbbb')", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "TO_TIMESTAMP('2014-01-10 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\\\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.109", "24345", "'myte\",xt123@gmail.com'", "'03:46:38.765594'", "'03:46:38.765594+05'", "TO_TIMESTAMP('2014-01-10 10:05:04.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2014-01-10 18:05:04.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "<nil>", "'%!s(<nil>)'", "'%!s(<nil>)'", "<nil>", "'%!s(<nil>)'", "hextoraw('%!x(<nil>)')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "<nil>", "'%!s(<nil>)'", "<nil>", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "<nil>", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "<nil>", "<nil>", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "postgresql2redshift_wide",
	// 	source:        postgresqlTestConnection,
	// 	target:        redshiftTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "postgresql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from postgresql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybit": "VARCHAR", "mybitvarying": "VARCHAR", "myboolean": "BOOL", "mybox": "VARCHAR", "mybytea": "VARCHAR", "mychar": "VARCHAR", "mycidr": "VARCHAR", "mycircle": "VARCHAR", "mydate": "TIMESTAMP", "mydoubleprecision": "FLOAT8", "myinet": "VARCHAR", "myinteger": "INT4", "myinterval": "VARCHAR", "myjson": "VARCHAR", "myjsonb": "VARCHAR", "myline": "VARCHAR", "mylseg": "VARCHAR", "mymacaddr": "VARCHAR", "mymoney": "VARCHAR", "mynumeric": "FLOAT8", "mypath": "VARCHAR", "mypg_lsn": "VARCHAR", "mypoint": "VARCHAR", "mypolygon": "VARCHAR", "myreal": "FLOAT4", "mysmallint": "INT4", "mytext": "VARCHAR", "mytime": "VARCHAR", "mytimestamp": "TIMESTAMP", "mytimestamptz": "TIMESTAMP", "mytimetz": "1266", "mytsquery": "VARCHAR", "mytsvector": "VARCHAR", "myuuid": "VARCHAR", "myvarchar": "VARCHAR", "myxml": "VARCHAR"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "true", "'(8,9),(1,3)'", "'aaaabbbb'", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "'2014-01-10 00:00:00.000000 +0000'", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.109375", "24345", "'myte\",xt123@gmail.com'", "'03:46:38.765594'", "'22:46:38.765594+00'", "'2014-01-10 10:05:04.000000 +0000'", "'2014-01-10 18:05:04.000000 +0000'", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!t(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "postgresql2snowflake_wide",
	// 	source:        postgresqlTestConnection,
	// 	target:        snowflakeTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "postgresql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from postgresql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBIGINT": "FIXED", "MYBIT": "TEXT", "MYBITVARYING": "TEXT", "MYBOOLEAN": "BOOLEAN", "MYBOX": "TEXT", "MYBYTEA": "BINARY", "MYCHAR": "TEXT", "MYCIDR": "TEXT", "MYCIRCLE": "TEXT", "MYDATE": "TIMESTAMP_NTZ", "MYDOUBLEPRECISION": "REAL", "MYINET": "TEXT", "MYINTEGER": "FIXED", "MYINTERVAL": "TEXT", "MYJSON": "VARIANT", "MYJSONB": "VARIANT", "MYLINE": "TEXT", "MYLSEG": "TEXT", "MYMACADDR": "TEXT", "MYMONEY": "TEXT", "MYNUMERIC": "REAL", "MYPATH": "TEXT", "MYPG_LSN": "TEXT", "MYPOINT": "TEXT", "MYPOLYGON": "TEXT", "MYREAL": "REAL", "MYSMALLINT": "FIXED", "MYTEXT": "TEXT", "MYTIME": "TIME", "MYTIMESTAMP": "TIMESTAMP_NTZ", "MYTIMESTAMPTZ": "TIMESTAMP_NTZ", "MYTIMETZ": "TEXT", "MYTSQUERY": "TEXT", "MYTSVECTOR": "TEXT", "MYUUID": "TEXT", "MYVARCHAR": "TEXT", "MYXML": "TEXT"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "true", "'(8,9),(1,3)'", "to_binary('aaaabbbb')", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "'2014-01-10 00:00:00.000000'", "529.562190", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\n  \"mykey\": \"this\\\\\"  ''is'' m,y val\"\n}'", "'{\n  \"mykey\": \"this is my val\"\n}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.821150", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.109375", "24345", "'myte\",xt123@gmail.com'", "'0001-01-01 03:46:38.765594'", "'03:46:38.765594+05'", "'2014-01-10 10:05:04.000000'", "'2014-01-10 18:05:04.000000'", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!t(<nil>)", "'%!s(<nil>)'", "to_binary('%!x(<nil>)')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!s(<nil>)", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "null", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// // MySQL Transfers
	// {
	// 	name:          "mysql2postgresql_wide",
	// 	source:        mysqlTestConnection,
	// 	target:        postgresqlTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "mysql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from mysql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybinary": "BYTEA", "mybit": "VARBIT", "mybit5": "VARBIT", "mybit64": "VARBIT", "myblob": "BYTEA", "mychar": "VARCHAR", "mydate": "DATE", "mydatetime": "TIMESTAMP", "mydecimal": "NUMERIC", "mydouble": "FLOAT8", "myenum": "VARCHAR", "myfloat": "FLOAT4", "mygeometry": "BYTEA", "mygeometrycollection": "BYTEA", "myint": "INT4", "myjson": "JSON", "mylinestring": "BYTEA", "mylongblob": "BYTEA", "mylongtext": "TEXT", "mymediumblob": "BYTEA", "mymediumint": "INT4", "mymediumtext": "TEXT", "mymultilinestring": "BYTEA", "mymultipoint": "BYTEA", "mymultipolygon": "BYTEA", "mynchar": "VARCHAR", "mynvarchar": "VARCHAR", "mypoint": "BYTEA", "mypolygon": "BYTEA", "myserial": "INT8", "myset": "VARCHAR", "mysmallint": "INT2", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytinyblob": "BYTEA", "mytinyint": "INT2", "mytinytext": "TEXT", "myvarbinary": "BYTEA", "myvarchar": "VARCHAR", "myyear": "INT4"}, Rows: []interface{}{"1", "'1'", "'1010'", "'1111111111111111111111111111111111111111111111111111111111111111'", "2", "5", "50", "4595435", "392809438543", "30.50000", "45.900001525878906", "54.3", "'2009-05-28 00:00:00.000000 +0000'", "'14:23:54'", "'2010-10-24 20:52:52.000000 +0000'", "'1989-02-22 03:17:21.000000 +0000'", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "'\\x626e72'", "'\\x6d792062696e61727920737472696e67207761686f6f6f6f6f'", "'\\x626c6f622063697479206262'", "'\\x626c6f622063697479206262'", "'\\x626c6f622063697479206262'", "'\\x626c6f622063697479206262'", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "'\\x000000000101000000000000000000f03f000000000000f03f'", "'\\x000000000101000000000000000000f03f000000000000f03f'", "'\\x0000000001020000000300000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000400000000000000040'", "'\\x0000000001030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000005000000000000000000144000000000000014400000000000001c4000000000000014400000000000001c400000000000001c4000000000000014400000000000001c4000000000000014400000000000001440'", "'\\x0000000001040000000a0000000101000000000000000000f03f000000000000f03f01010000000000000000000040000000000000004001010000000000000000001440000000000000084001010000000000000000001c4000000000000000400101000000000000000000224000000000000008400101000000000000000000204000000000000010400101000000000000000000184000000000000018400101000000000000000000184000000000000022400101000000000000000000104000000000000022400101000000000000000000f03f0000000000001440'", "'\\x00000000010500000002000000010200000003000000000000000000f03f000000000000f03f00000000000000400000000000000040000000000000084000000000000008400102000000020000000000000000001040000000000000104000000000000014400000000000001440'", "'\\x0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f'", "'\\x0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f'", "'{\"mykey\": \"this is\\\" m\\\"y, ''val''\"}'", "2", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!s(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "'%!s(<nil>)'", "null", "null", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "mysql2mysql_wide",
	// 	source:        mysqlTestConnection,
	// 	target:        mysqlTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "mysql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from mysql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybinary": "BLOB", "mybit": "BIT", "mybit5": "BIT", "mybit64": "BIT", "myblob": "BLOB", "mychar": "TEXT", "mydate": "DATE", "mydatetime": "DATETIME", "mydecimal": "DECIMAL", "mydouble": "DOUBLE", "myenum": "TEXT", "myfloat": "FLOAT", "mygeometry": "GEOMETRY", "mygeometrycollection": "GEOMETRY", "myint": "INT", "myjson": "JSON", "mylinestring": "GEOMETRY", "mylongblob": "BLOB", "mylongtext": "TEXT", "mymediumblob": "BLOB", "mymediumint": "MEDIUMINT", "mymediumtext": "TEXT", "mymultilinestring": "GEOMETRY", "mymultipoint": "GEOMETRY", "mymultipolygon": "GEOMETRY", "mynchar": "TEXT", "mynvarchar": "TEXT", "mypoint": "GEOMETRY", "mypolygon": "GEOMETRY", "myserial": "BIGINT", "myset": "TEXT", "mysmallint": "SMALLINT", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytinyblob": "BLOB", "mytinyint": "TINYINT", "mytinytext": "TEXT", "myvarbinary": "BLOB", "myvarchar": "TEXT", "myyear": "YEAR"}, Rows: []interface{}{"1", "x'0000000000000001'", "x'000000000000000a'", "x'ffffffffffffffff'", "2", "5", "50", "4595435", "392809438543", "30.50000", "45.9", "54.3", "'2009-05-28'", "'14:23:54'", "'2010-10-24 20:52:52'", "'1989-02-22 03:17:21'", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "x'626e72'", "x'6d792062696e61727920737472696e67207761686f6f6f6f6f'", "x'626c6f622063697479206262'", "x'626c6f622063697479206262'", "x'626c6f622063697479206262'", "x'626c6f622063697479206262'", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "'\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'\x00\x00\x00\x00\x01\x02\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@'", "'\x00\x00\x00\x00\x01\x03\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@'", "'\x00\x00\x00\x00\x01\x04\x00\x00\x00\n\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\b@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x00@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\"@\x00\x00\x00\x00\x00\x00\b@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00 @\x00\x00\x00\x00\x00\x00\x10@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x18@\x00\x00\x00\x00\x00\x00\x18@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x18@\x00\x00\x00\x00\x00\x00\"@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\"@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x14@'", "'\x00\x00\x00\x00\x01\x05\x00\x00\x00\x02\x00\x00\x00\x01\x02\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@'", "'\x00\x00\x00\x00\x01\x06\x00\x00\x00\x01\x00\x00\x00\x01\x03\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'\x00\x00\x00\x00\x01\x06\x00\x00\x00\x01\x00\x00\x00\x01\x03\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'{\"mykey\": \"this is\\\\\" m\\\\\"y, ''val''\"}'", "2", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "mysql2mssql_wide",
	// 	source:        mysqlTestConnection,
	// 	target:        mssqlTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "mysql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from mysql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybinary": "VARBINARY", "mybit": "VARCHAR", "mybit5": "VARCHAR", "mybit64": "VARCHAR", "myblob": "VARBINARY", "mychar": "NVARCHAR", "mydate": "DATE", "mydatetime": "DATETIME2", "mydecimal": "DECIMAL", "mydouble": "FLOAT", "myenum": "NVARCHAR", "myfloat": "REAL", "mygeometry": "VARBINARY", "mygeometrycollection": "VARBINARY", "myint": "INT", "myjson": "NVARCHAR", "mylinestring": "VARBINARY", "mylongblob": "VARBINARY", "mylongtext": "NTEXT", "mymediumblob": "VARBINARY", "mymediumint": "INT", "mymediumtext": "NTEXT", "mymultilinestring": "VARBINARY", "mymultipoint": "VARBINARY", "mymultipolygon": "VARBINARY", "mynchar": "NVARCHAR", "mynvarchar": "NVARCHAR", "mypoint": "VARBINARY", "mypolygon": "VARBINARY", "myserial": "BIGINT", "myset": "NVARCHAR", "mysmallint": "SMALLINT", "mytext": "NTEXT", "mytime": "TIME", "mytimestamp": "DATETIME2", "mytinyblob": "VARBINARY", "mytinyint": "TINYINT", "mytinytext": "NTEXT", "myvarbinary": "VARBINARY", "myvarchar": "NVARCHAR", "myyear": "INT"}, Rows: []interface{}{"1", "'[1]'", "'[1010]'", "'[11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111]'", "2", "5", "50", "4595435", "392809438543", "30.50000", "45.900001525878906", "54.3", "CONVERT(DATETIME2, '2009-05-28 00:00:00.0000000', 121)", "CONVERT(DATETIME2, '0001-01-01 14:23:54.0000000', 121)", "CONVERT(DATETIME2, '2010-10-24 20:52:52.0000000', 121)", "CONVERT(DATETIME2, '1989-02-22 03:17:21.0000000', 121)", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "CONVERT(VARBINARY(8000), '0x626e72', 1)", "CONVERT(VARBINARY(8000), '0x6d792062696e61727920737472696e67207761686f6f6f6f6f', 1)", "CONVERT(VARBINARY(8000), '0x626c6f622063697479206262', 1)", "CONVERT(VARBINARY(8000), '0x626c6f622063697479206262', 1)", "CONVERT(VARBINARY(8000), '0x626c6f622063697479206262', 1)", "CONVERT(VARBINARY(8000), '0x626c6f622063697479206262', 1)", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "CONVERT(VARBINARY(8000), '0x000000000101000000000000000000f03f000000000000f03f', 1)", "CONVERT(VARBINARY(8000), '0x000000000101000000000000000000f03f000000000000f03f', 1)", "CONVERT(VARBINARY(8000), '0x0000000001020000000300000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000400000000000000040', 1)", "CONVERT(VARBINARY(8000), '0x0000000001030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000005000000000000000000144000000000000014400000000000001c4000000000000014400000000000001c400000000000001c4000000000000014400000000000001c4000000000000014400000000000001440', 1)", "CONVERT(VARBINARY(8000), '0x0000000001040000000a0000000101000000000000000000f03f000000000000f03f01010000000000000000000040000000000000004001010000000000000000001440000000000000084001010000000000000000001c4000000000000000400101000000000000000000224000000000000008400101000000000000000000204000000000000010400101000000000000000000184000000000000018400101000000000000000000184000000000000022400101000000000000000000104000000000000022400101000000000000000000f03f0000000000001440', 1)", "CONVERT(VARBINARY(8000), '0x00000000010500000002000000010200000003000000000000000000f03f000000000000f03f00000000000000400000000000000040000000000000084000000000000008400102000000020000000000000000001040000000000000104000000000000014400000000000001440', 1)", "CONVERT(VARBINARY(8000), '0x0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f', 1)", "CONVERT(VARBINARY(8000), '0x0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f', 1)", "'{\"mykey\": \"this is\\\" m\\\"y, ''val''\"}'", "2", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!s(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "null", "null", "null", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "mysql2oracle_wide",
	// 	source:        mysqlTestConnection,
	// 	target:        oracleTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "mysql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from mysql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBIGINT": "NUMBER", "MYBINARY": "OCIBlobLocator", "MYBIT": "OCIBlobLocator", "MYBIT5": "OCIBlobLocator", "MYBIT64": "OCIBlobLocator", "MYBLOB": "OCIBlobLocator", "MYCHAR": "NCHAR", "MYDATE": "DATE", "MYDATETIME": "TimeStampDTY", "MYDECIMAL": "NUMBER", "MYDOUBLE": "IBDouble", "MYENUM": "NCHAR", "MYFLOAT": "IBFloat", "MYGEOMETRY": "OCIBlobLocator", "MYGEOMETRYCOLLECTION": "OCIBlobLocator", "MYINT": "NUMBER", "MYJSON": "NCHAR", "MYLINESTRING": "OCIBlobLocator", "MYLONGBLOB": "OCIBlobLocator", "MYLONGTEXT": "NCHAR", "MYMEDIUMBLOB": "OCIBlobLocator", "MYMEDIUMINT": "NUMBER", "MYMEDIUMTEXT": "NCHAR", "MYMULTILINESTRING": "OCIBlobLocator", "MYMULTIPOINT": "OCIBlobLocator", "MYMULTIPOLYGON": "OCIBlobLocator", "MYNCHAR": "NCHAR", "MYNVARCHAR": "NCHAR", "MYPOINT": "OCIBlobLocator", "MYPOLYGON": "OCIBlobLocator", "MYSERIAL": "NUMBER", "MYSET": "NCHAR", "MYSMALLINT": "NUMBER", "MYTEXT": "NCHAR", "MYTIME": "NCHAR", "MYTIMESTAMP": "TimeStampDTY", "MYTINYBLOB": "OCIBlobLocator", "MYTINYINT": "NUMBER", "MYTINYTEXT": "NCHAR", "MYVARBINARY": "OCIBlobLocator", "MYVARCHAR": "NCHAR", "MYYEAR": "NUMBER"}, Rows: []interface{}{"1", "hextoraw('01')", "hextoraw('0a')", "hextoraw('ffffffffffffffff')", "2", "5", "50", "4595435", "392809438543", "30.5", "45.9", "54.3", "TO_DATE('2009-05-28', 'YYYY-MM-DD')", "'14:23:54'", "TO_TIMESTAMP('2010-10-24 20:52:52.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('1989-02-22 03:17:21.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "hextoraw('626e72')", "hextoraw('6d792062696e61727920737472696e67207761686f6f6f6f6f')", "hextoraw('626c6f622063697479206262')", "hextoraw('626c6f622063697479206262')", "hextoraw('626c6f622063697479206262')", "hextoraw('626c6f622063697479206262')", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "hextoraw('000000000101000000000000000000f03f000000000000f03f')", "hextoraw('000000000101000000000000000000f03f000000000000f03f')", "hextoraw('0000000001020000000300000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000400000000000000040')", "hextoraw('0000000001030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000005000000000000000000144000000000000014400000000000001c4000000000000014400000000000001c400000000000001c4000000000000014400000000000001c4000000000000014400000000000001440')", "hextoraw('0000000001040000000a0000000101000000000000000000f03f000000000000f03f01010000000000000000000040000000000000004001010000000000000000001440000000000000084001010000000000000000001c4000000000000000400101000000000000000000224000000000000008400101000000000000000000204000000000000010400101000000000000000000184000000000000018400101000000000000000000184000000000000022400101000000000000000000104000000000000022400101000000000000000000f03f0000000000001440')", "hextoraw('00000000010500000002000000010200000003000000000000000000f03f000000000000f03f00000000000000400000000000000040000000000000084000000000000008400102000000020000000000000000001040000000000000104000000000000014400000000000001440')", "hextoraw('0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f')", "hextoraw('0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f')", "'{\"mykey\": \"this is\\\" m\\\"y, ''val''\"}'", "2", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "null", "'%!s(<nil>)'", "null", "null", "<nil>", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "mysql2redshift_wide",
	// 	source:        mysqlTestConnection,
	// 	target:        redshiftTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "mysql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from mysql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybinary": "VARCHAR", "mybit": "VARCHAR", "mybit5": "VARCHAR", "mybit64": "VARCHAR", "myblob": "VARCHAR", "mychar": "VARCHAR", "mydate": "DATE", "mydatetime": "TIMESTAMP", "mydecimal": "NUMERIC", "mydouble": "FLOAT8", "myenum": "VARCHAR", "myfloat": "FLOAT4", "mygeometry": "VARCHAR", "mygeometrycollection": "VARCHAR", "myint": "INT4", "myjson": "VARCHAR", "mylinestring": "VARCHAR", "mylongblob": "VARCHAR", "mylongtext": "VARCHAR", "mymediumblob": "VARCHAR", "mymediumint": "INT4", "mymediumtext": "VARCHAR", "mymultilinestring": "VARCHAR", "mymultipoint": "VARCHAR", "mymultipolygon": "VARCHAR", "mynchar": "VARCHAR", "mynvarchar": "VARCHAR", "mypoint": "VARCHAR", "mypolygon": "VARCHAR", "myserial": "INT8", "myset": "VARCHAR", "mysmallint": "INT2", "mytext": "VARCHAR", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytinyblob": "VARCHAR", "mytinyint": "INT2", "mytinytext": "VARCHAR", "myvarbinary": "VARCHAR", "myvarchar": "VARCHAR", "myyear": "INT4"}, Rows: []interface{}{"1", "'1'", "'1010'", "'1111111111111111111111111111111111111111111111111111111111111111'", "2", "5", "50", "4595435", "392809438543", "30.50000", "45.900001525878906", "54.3", "'2009-05-28 00:00:00.000000 +0000'", "'14:23:54'", "'2010-10-24 20:52:52.000000 +0000'", "'1989-02-22 03:17:21.000000 +0000'", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "'bnr'", "'my binary string wahooooo'", "'626c6f622063697479206262'", "'626c6f622063697479206262'", "'626c6f622063697479206262'", "'626c6f622063697479206262'", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "'000000000101000000000000000000f03f000000000000f03f'", "'000000000101000000000000000000f03f000000000000f03f'", "'0000000001020000000300000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000400000000000000040'", "'0000000001030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000005000000000000000000144000000000000014400000000000001c4000000000000014400000000000001c400000000000001c4000000000000014400000000000001c4000000000000014400000000000001440'", "'0000000001040000000a0000000101000000000000000000f03f000000000000f03f01010000000000000000000040000000000000004001010000000000000000001440000000000000084001010000000000000000001c4000000000000000400101000000000000000000224000000000000008400101000000000000000000204000000000000010400101000000000000000000184000000000000018400101000000000000000000184000000000000022400101000000000000000000104000000000000022400101000000000000000000f03f0000000000001440'", "'00000000010500000002000000010200000003000000000000000000f03f000000000000f03f00000000000000400000000000000040000000000000084000000000000008400102000000020000000000000000001040000000000000104000000000000014400000000000001440'", "'0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f'", "'0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f'", "'{\"mykey\": \"this is\" m\"y, ''val''\"}'", "2", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!s(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "'%!s(<nil>)'", "null", "null", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "mysql2snowflake_wide",
	// 	source:        mysqlTestConnection,
	// 	target:        snowflakeTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "mysql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from mysql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBIGINT": "FIXED", "MYBINARY": "BINARY", "MYBIT": "BINARY", "MYBIT5": "BINARY", "MYBIT64": "BINARY", "MYBLOB": "BINARY", "MYCHAR": "TEXT", "MYDATE": "DATE", "MYDATETIME": "TIMESTAMP_NTZ", "MYDECIMAL": "FIXED", "MYDOUBLE": "REAL", "MYENUM": "TEXT", "MYFLOAT": "REAL", "MYGEOMETRY": "BINARY", "MYGEOMETRYCOLLECTION": "BINARY", "MYINT": "FIXED", "MYJSON": "VARIANT", "MYLINESTRING": "BINARY", "MYLONGBLOB": "BINARY", "MYLONGTEXT": "TEXT", "MYMEDIUMBLOB": "BINARY", "MYMEDIUMINT": "FIXED", "MYMEDIUMTEXT": "TEXT", "MYMULTILINESTRING": "BINARY", "MYMULTIPOINT": "BINARY", "MYMULTIPOLYGON": "BINARY", "MYNCHAR": "TEXT", "MYNVARCHAR": "TEXT", "MYPOINT": "BINARY", "MYPOLYGON": "BINARY", "MYSERIAL": "FIXED", "MYSET": "TEXT", "MYSMALLINT": "FIXED", "MYTEXT": "TEXT", "MYTIME": "TIME", "MYTIMESTAMP": "TIMESTAMP_NTZ", "MYTINYBLOB": "BINARY", "MYTINYINT": "FIXED", "MYTINYTEXT": "TEXT", "MYVARBINARY": "BINARY", "MYVARCHAR": "TEXT", "MYYEAR": "FIXED"}, Rows: []interface{}{"1", "to_binary('01')", "to_binary('0a')", "to_binary('ffffffffffffffff')", "2", "5", "50", "4595435", "392809438543", "30.500000", "45.900000", "54.300000", "'2009-05-28 00:00:00.000000'", "'0001-01-01 14:23:54.000000'", "'2010-10-24 20:52:52.000000'", "'1989-02-22 03:17:21.000000'", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "to_binary('626e72')", "to_binary('6d792062696e61727920737472696e67207761686f6f6f6f6f')", "to_binary('626c6f622063697479206262')", "to_binary('626c6f622063697479206262')", "to_binary('626c6f622063697479206262')", "to_binary('626c6f622063697479206262')", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "to_binary('000000000101000000000000000000f03f000000000000f03f')", "to_binary('000000000101000000000000000000f03f000000000000f03f')", "to_binary('0000000001020000000300000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000400000000000000040')", "to_binary('0000000001030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000005000000000000000000144000000000000014400000000000001c4000000000000014400000000000001c400000000000001c4000000000000014400000000000001c4000000000000014400000000000001440')", "to_binary('0000000001040000000a0000000101000000000000000000f03f000000000000f03f01010000000000000000000040000000000000004001010000000000000000001440000000000000084001010000000000000000001c4000000000000000400101000000000000000000224000000000000008400101000000000000000000204000000000000010400101000000000000000000184000000000000018400101000000000000000000184000000000000022400101000000000000000000104000000000000022400101000000000000000000f03f0000000000001440')", "to_binary('00000000010500000002000000010200000003000000000000000000f03f000000000000f03f00000000000000400000000000000040000000000000084000000000000008400102000000020000000000000000001040000000000000104000000000000014400000000000001440')", "to_binary('0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f')", "to_binary('0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f')", "'{\n  \"mykey\": \"this is\\\\\" m\\\\\"y, ''val''\"\n}'", "2", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "null", "null", "null", "null", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "'%!s(<nil>)'"}},
	// },
	// // MSSQL Transfers
	// {
	// 	name:          "mssql2postgresql_wide",
	// 	source:        mssqlTestConnection,
	// 	target:        postgresqlTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "mssql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from mssql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybinary": "BYTEA", "mybit": "BOOL", "mychar": "BPCHAR", "mydate": "TIMESTAMPTZ", "mydatetime": "TIMESTAMPTZ", "mydatetime2": "TIMESTAMPTZ", "mydatetimeoffset": "TIMESTAMPTZ", "mydecimal": "NUMERIC", "myfloat": "FLOAT8", "myint": "INT8", "mymoney": "TEXT", "mynchar": "BPCHAR", "myntext": "TEXT", "mynumeric": "NUMERIC", "mynvarchar": "VARCHAR", "myreal": "FLOAT8", "mysmalldatetime": "TIMESTAMPTZ", "mysmallint": "INT8", "mysmallmoney": "TEXT", "mytext": "TEXT", "mytime": "TIMESTAMPTZ", "mytinyint": "INT8", "myuniqueidentifier": "UUID", "myvarbinary": "BYTEA", "myvarchar": "VARCHAR", "myxml": "142"}, Rows: []interface{}{"435345", "true", "324.43000", "54", "'43.2100'", "54.3300000", "12", "'22.1000'", "4", "45.5", "47.70000076293945", "'2013-10-12 01:00:00.000000 +0100'", "'2005-06-12 12:40:17.632000 +0100'", "'2005-06-12 12:40:17.633000 +0100'", "'2005-06-12 11:40:17.632000 +0100'", "'2005-06-12 12:40:00.000000 +0100'", "'0001-01-01 11:03:27.543654 -0036'", "'yoo'", "'gday guvna'", "'omg have you hea''rd\" a,bout the latest craze that the people are talking about?'", "'yoo'", "'gday guvna'", "'omg have you heard about the latest craze that the people are talking about?'", "'\\x000065'", "'\\x000186a1'", "'6f9619ff-8b86-d011-b42d-00c04fc964ff'", "'<foo>bar</foo>'", "%!d(<nil>)", "%!t(<nil>)", "%!s(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "%!s(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "null", "null", "null", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "mssql2mysql_wide",
	// 	source:        mssqlTestConnection,
	// 	target:        mysqlTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "mssql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from mssql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybinary": "BLOB", "mybit": "TINYINT", "mychar": "CHAR", "mydate": "DATETIME", "mydatetime": "DATETIME", "mydatetime2": "DATETIME", "mydatetimeoffset": "DATETIME", "mydecimal": "DECIMAL", "myfloat": "DOUBLE", "myint": "BIGINT", "mymoney": "TEXT", "mynchar": "CHAR", "myntext": "TEXT", "mynumeric": "DECIMAL", "mynvarchar": "VARCHAR", "myreal": "DOUBLE", "mysmalldatetime": "DATETIME", "mysmallint": "BIGINT", "mysmallmoney": "TEXT", "mytext": "TEXT", "mytime": "DATETIME", "mytinyint": "BIGINT", "myuniqueidentifier": "TEXT", "myvarbinary": "BLOB", "myvarchar": "VARCHAR", "myxml": "TEXT"}, Rows: []interface{}{"435345", "1", "324.43000", "54", "'43.2100'", "54.3300000", "12", "'22.1000'", "4", "45.5", "47.70000076293945", "'2013-10-12 00:00:00'", "'2005-06-12 11:40:18'", "'2005-06-12 11:40:18'", "'2005-06-12 10:40:18'", "'2005-06-12 11:40:00'", "'0001-01-01 11:40:13'", "'yoo'", "'gday guvna'", "'omg have you hea''rd\" a,bout the latest craze that the people are talking about?'", "'yoo'", "'gday guvna'", "'omg have you heard about the latest craze that the people are talking about?'", "x'000065'", "x'000186a1'", "'6F9619FF8B86D011B42D00C04FC964FF'", "'<foo>bar</foo>'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "mssql2mssql_wide",
	// 	source:        mssqlTestConnection,
	// 	target:        mssqlTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "mssql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from mssql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybinary": "VARBINARY", "mybit": "BIT", "mychar": "CHAR", "mydate": "DATETIME2", "mydatetime": "DATETIME2", "mydatetime2": "DATETIME2", "mydatetimeoffset": "DATETIME2", "mydecimal": "DECIMAL", "myfloat": "FLOAT", "myint": "BIGINT", "mymoney": "VARCHAR", "mynchar": "NCHAR", "myntext": "NTEXT", "mynumeric": "DECIMAL", "mynvarchar": "NVARCHAR", "myreal": "FLOAT", "mysmalldatetime": "DATETIME2", "mysmallint": "BIGINT", "mysmallmoney": "VARCHAR", "mytext": "TEXT", "mytime": "DATETIME2", "mytinyint": "BIGINT", "myuniqueidentifier": "UNIQUEIDENTIFIER", "myvarbinary": "VARBINARY", "myvarchar": "VARCHAR", "myxml": "XML"}, Rows: []interface{}{"435345", "1", "324.43000", "54", "'43.2100'", "54.3300000", "12", "'22.1000'", "4", "45.5", "47.70000076293945", "CONVERT(DATETIME2, '2013-10-12 00:00:00.0000000', 121)", "CONVERT(DATETIME2, '2005-06-12 11:40:17.6320000', 121)", "CONVERT(DATETIME2, '2005-06-12 11:40:17.6330000', 121)", "CONVERT(DATETIME2, '2005-06-12 11:40:17.6320000', 121)", "CONVERT(DATETIME2, '2005-06-12 11:40:00.0000000', 121)", "CONVERT(DATETIME2, '0001-01-01 11:40:12.5436540', 121)", "'yoo'", "'gday guvna'", "'omg have you hea''rd\" a,bout the latest craze that the people are talking about?'", "'yoo'", "'gday guvna'", "'omg have you heard about the latest craze that the people are talking about?'", "CONVERT(VARBINARY(8000), '0x000065', 1)", "CONVERT(VARBINARY(8000), '0x000186a1', 1)", "N'6F9619FF-8B86-D011-B42D-00C04FC964FF'", "'<foo>bar</foo>'", "%!d(<nil>)", "null", "%!s(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "%!s(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "null", "null", "null", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "null", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "mssql2oracle_wide",
	// 	source:        mssqlTestConnection,
	// 	target:        oracleTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "mssql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from mssql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBIGINT": "NUMBER", "MYBINARY": "OCIBlobLocator", "MYBIT": "NUMBER", "MYCHAR": "CHAR", "MYDATE": "TimeStampDTY", "MYDATETIME": "TimeStampDTY", "MYDATETIME2": "TimeStampDTY", "MYDATETIMEOFFSET": "TimeStampDTY", "MYDECIMAL": "NUMBER", "MYFLOAT": "IBDouble", "MYINT": "NUMBER", "MYMONEY": "NCHAR", "MYNCHAR": "CHAR", "MYNTEXT": "NCHAR", "MYNUMERIC": "NUMBER", "MYNVARCHAR": "NCHAR", "MYREAL": "IBDouble", "MYSMALLDATETIME": "TimeStampDTY", "MYSMALLINT": "NUMBER", "MYSMALLMONEY": "NCHAR", "MYTEXT": "NCHAR", "MYTIME": "TimeStampDTY", "MYTINYINT": "NUMBER", "MYUNIQUEIDENTIFIER": "NCHAR", "MYVARBINARY": "OCIBlobLocator", "MYVARCHAR": "NCHAR", "MYXML": "NCHAR"}, Rows: []interface{}{"435345", "1", "324.43", "54", "'43.2100'", "54.33", "12", "'22.1000'", "4", "45.5", "47.70000076293945", "TO_TIMESTAMP('2013-10-12 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2005-06-12 11:40:17.632000', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2005-06-12 11:40:17.633000', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2005-06-12 11:40:17.632000', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2005-06-12 11:40:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('0001-01-01 11:40:12.543654', 'YYYY-MM-DD HH24:MI:SS.FF')", "'yoo'", "'gday guvna'", "'omg have you hea''rd\" a,bout the latest craze that the people are talking about?'", "'yoo'", "'gday guvna'", "'omg have you heard about the latest craze that the people are talking about?'", "hextoraw('000065')", "hextoraw('000186a1')", "'FF19966F868B11D0B42D00C04FC964FF'", "'<foo>bar</foo>'", "<nil>", "<nil>", "<nil>", "<nil>", "'%!s(<nil>)'", "<nil>", "<nil>", "'%!s(<nil>)'", "<nil>", "<nil>", "<nil>", "null", "null", "null", "null", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "mssql2redshift_wide",
	// 	source:        mssqlTestConnection,
	// 	target:        redshiftTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "mssql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from mssql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybinary": "VARCHAR", "mybit": "BOOL", "mychar": "BPCHAR", "mydate": "TIMESTAMP", "mydatetime": "TIMESTAMP", "mydatetime2": "TIMESTAMP", "mydatetimeoffset": "TIMESTAMP", "mydecimal": "NUMERIC", "myfloat": "FLOAT8", "myint": "INT8", "mymoney": "VARCHAR", "mynchar": "BPCHAR", "myntext": "VARCHAR", "mynumeric": "NUMERIC", "mynvarchar": "VARCHAR", "myreal": "FLOAT8", "mysmalldatetime": "TIMESTAMP", "mysmallint": "INT8", "mysmallmoney": "VARCHAR", "mytext": "VARCHAR", "mytime": "TIMESTAMP", "mytinyint": "INT8", "myuniqueidentifier": "VARCHAR", "myvarbinary": "VARCHAR", "myvarchar": "VARCHAR", "myxml": "VARCHAR"}, Rows: []interface{}{"435345", "true", "324.43000", "54", "'43.2100'", "54.3300000", "12", "'22.1000'", "4", "45.5", "47.70000076293945", "'2013-10-12 00:00:00.000000 +0000'", "'2005-06-12 11:40:17.632000 +0000'", "'2005-06-12 11:40:17.633000 +0000'", "'2005-06-12 11:40:17.632000 +0000'", "'2005-06-12 11:40:00.000000 +0000'", "'0001-01-01 11:40:12.543654 +0000'", "'yoo'", "'gday guvna'", "'omg have you hea''rd\" a,bout the latest craze that the people are talking about?'", "'yoo'", "'gday guvna'", "'omg have you heard about the latest craze that the people are talking about?'", "'000065'", "'000186a1'", "'6F9619FF8B86D011B42D00C04FC964FF'", "'<foo>bar</foo>'", "%!d(<nil>)", "%!t(<nil>)", "%!s(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "%!s(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "null", "null", "null", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "mssql2snowflake_wide",
	// 	source:        mssqlTestConnection,
	// 	target:        snowflakeTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "mssql_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from mssql_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBIGINT": "FIXED", "MYBINARY": "BINARY", "MYBIT": "BOOLEAN", "MYCHAR": "TEXT", "MYDATE": "TIMESTAMP_NTZ", "MYDATETIME": "TIMESTAMP_NTZ", "MYDATETIME2": "TIMESTAMP_NTZ", "MYDATETIMEOFFSET": "TIMESTAMP_NTZ", "MYDECIMAL": "FIXED", "MYFLOAT": "REAL", "MYINT": "FIXED", "MYMONEY": "TEXT", "MYNCHAR": "TEXT", "MYNTEXT": "TEXT", "MYNUMERIC": "FIXED", "MYNVARCHAR": "TEXT", "MYREAL": "REAL", "MYSMALLDATETIME": "TIMESTAMP_NTZ", "MYSMALLINT": "FIXED", "MYSMALLMONEY": "TEXT", "MYTEXT": "TEXT", "MYTIME": "TIMESTAMP_NTZ", "MYTINYINT": "FIXED", "MYUNIQUEIDENTIFIER": "TEXT", "MYVARBINARY": "BINARY", "MYVARCHAR": "TEXT", "MYXML": "TEXT"}, Rows: []interface{}{"435345", "true", "324.430000", "54", "'43.2100'", "54.330000", "12", "'22.1000'", "4", "45.500000", "47.700001", "'2013-10-12 00:00:00.000000'", "'2005-06-12 11:40:17.632000'", "'2005-06-12 11:40:17.633000'", "'2005-06-12 11:40:17.632000'", "'2005-06-12 11:40:00.000000'", "'0001-01-01 11:40:12.543654'", "'yoo'", "'gday guvna'", "'omg have you hea''rd\" a,bout the latest craze that the people are talking about?'", "'yoo'", "'gday guvna'", "'omg have you heard about the latest craze that the people are talking about?'", "to_binary('000065')", "to_binary('000186a1')", "'6F9619FF8B86D011B42D00C04FC964FF'", "'<foo>bar</foo>'", "%!s(<nil>)", "%!t(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "null", "null", "null", "null", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// // Oracle Transfers
	// {
	// 	name:          "oracle2postgresql_wide",
	// 	source:        oracleTestConnection,
	// 	target:        postgresqlTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "oracle_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from oracle_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybinary_double": "NUMERIC", "mybinary_float": "NUMERIC", "myblob": "BYTEA", "mychar": "TEXT", "myclob": "VARCHAR", "mydate": "DATE", "mylong": "TEXT", "mynchar": "TEXT", "mynumber": "NUMERIC", "mynvarchar2": "TEXT", "mytimestamp": "TIMESTAMP", "mytimestamptz": "TIMESTAMP", "mytimestampwithlocaltz": "TIMESTAMP", "myvarchar": "TEXT", "myvarchar2": "TEXT"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "12.5", "47.5", "900.2", "'2005-09-16 00:00:00.000000 +0000'", "'2021-07-22 10:18:59.194681 +0000'", "'2021-07-22 10:18:59.194681 +0000'", "'2021-07-22 09:18:59.194681 +0000'", "'\\x111a'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "null", "null", "null", "null", "'\\x%!x(<nil>)'"}},
	// },
	// {
	// 	name:          "oracle2mysql_wide",
	// 	source:        oracleTestConnection,
	// 	target:        mysqlTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "oracle_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from oracle_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBINARY_DOUBLE": "DOUBLE", "MYBINARY_FLOAT": "DOUBLE", "MYBLOB": "BLOB", "MYCHAR": "TEXT", "MYCLOB": "TEXT", "MYDATE": "DATE", "MYLONG": "TEXT", "MYNCHAR": "TEXT", "MYNUMBER": "DOUBLE", "MYNVARCHAR2": "TEXT", "MYTIMESTAMP": "TIMESTAMP", "MYTIMESTAMPTZ": "TIMESTAMP", "MYTIMESTAMPWITHLOCALTZ": "TIMESTAMP", "MYVARCHAR": "TEXT", "MYVARCHAR2": "TEXT"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "12.5", "47.5", "900.2", "'2005-09-16'", "'2021-07-22 10:18:59'", "'2021-07-22 10:18:59'", "'2021-07-22 09:18:59'", "x'111a'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "x'%!x(<nil>)'"}},
	// },
	// {
	// 	name:          "oracle2mssql_wide",
	// 	source:        oracleTestConnection,
	// 	target:        mssqlTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "oracle_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from oracle_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBINARY_DOUBLE": "FLOAT", "MYBINARY_FLOAT": "FLOAT", "MYBLOB": "VARBINARY", "MYCHAR": "NTEXT", "MYCLOB": "NVARCHAR", "MYDATE": "DATE", "MYLONG": "NTEXT", "MYNCHAR": "NTEXT", "MYNUMBER": "FLOAT", "MYNVARCHAR2": "NTEXT", "MYTIMESTAMP": "DATETIME2", "MYTIMESTAMPTZ": "DATETIME2", "MYTIMESTAMPWITHLOCALTZ": "DATETIME2", "MYVARCHAR": "NTEXT", "MYVARCHAR2": "NTEXT"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "12.5", "47.5", "900.2", "CONVERT(DATETIME2, '2005-09-16 00:00:00.0000000', 121)", "CONVERT(DATETIME2, '2021-07-22 10:18:59.1946810', 121)", "CONVERT(DATETIME2, '2021-07-22 10:18:59.1946810', 121)", "CONVERT(DATETIME2, '2021-07-22 09:18:59.1946810', 121)", "CONVERT(VARBINARY(8000), '0x111a', 1)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "null", "null", "null", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)"}},
	// },
	// {
	// 	name:          "oracle2oracle_wide",
	// 	source:        oracleTestConnection,
	// 	target:        oracleTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "oracle_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from oracle_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBINARY_DOUBLE": "NUMBER", "MYBINARY_FLOAT": "NUMBER", "MYBLOB": "OCIBlobLocator", "MYCHAR": "NCHAR", "MYCLOB": "OCIClobLocator", "MYDATE": "DATE", "MYLONG": "LONG", "MYNCHAR": "NCHAR", "MYNUMBER": "NUMBER", "MYNVARCHAR2": "NCHAR", "MYTIMESTAMP": "TimeStampDTY", "MYTIMESTAMPTZ": "TimeStampDTY", "MYTIMESTAMPWITHLOCALTZ": "TimeStampDTY", "MYVARCHAR": "NCHAR", "MYVARCHAR2": "NCHAR"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "12.5", "47.5", "900.2", "TO_DATE('2005-09-16', 'YYYY-MM-DD')", "TO_TIMESTAMP('2021-07-22 10:18:59.194681', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2021-07-22 10:18:59.194681', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2021-07-22 09:18:59.194681', 'YYYY-MM-DD HH24:MI:SS.FF')", "hextoraw('111a')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "<nil>", "<nil>", "<nil>", "null", "null", "null", "null", "hextoraw('%!x(<nil>)')"}},
	// },
	// {
	// 	name:          "oracle2redshift_wide",
	// 	source:        oracleTestConnection,
	// 	target:        redshiftTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "oracle_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from oracle_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybinary_double": "FLOAT8", "mybinary_float": "FLOAT8", "myblob": "VARCHAR", "mychar": "VARCHAR", "myclob": "VARCHAR", "mydate": "DATE", "mylong": "VARCHAR", "mynchar": "VARCHAR", "mynumber": "FLOAT8", "mynvarchar2": "VARCHAR", "mytimestamp": "TIMESTAMP", "mytimestamptz": "TIMESTAMP", "mytimestampwithlocaltz": "TIMESTAMP", "myvarchar": "VARCHAR", "myvarchar2": "VARCHAR"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "12.5", "47.5", "900.2", "'2005-09-16 00:00:00.000000 +0000'", "'2021-07-22 10:18:59.194681 +0000'", "'2021-07-22 10:18:59.194681 +0000'", "'2021-07-22 09:18:59.194681 +0000'", "'\x11\x1a'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "null", "null", "null", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "oracle2snowflake_wide",
	// 	source:        oracleTestConnection,
	// 	target:        snowflakeTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "oracle_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from oracle_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBINARY_DOUBLE": "REAL", "MYBINARY_FLOAT": "REAL", "MYBLOB": "BINARY", "MYCHAR": "TEXT", "MYCLOB": "TEXT", "MYDATE": "DATE", "MYLONG": "TEXT", "MYNCHAR": "TEXT", "MYNUMBER": "REAL", "MYNVARCHAR2": "TEXT", "MYTIMESTAMP": "TIMESTAMP_NTZ", "MYTIMESTAMPTZ": "TIMESTAMP_NTZ", "MYTIMESTAMPWITHLOCALTZ": "TIMESTAMP_NTZ", "MYVARCHAR": "TEXT", "MYVARCHAR2": "TEXT"}, Rows: []interface{}{"'chr'", "'my vr''c\",hr'", "'my vrchr2'", "'ncr'", "'mynvarch2'", "'myclob'", "'wow such long text wow'", "12.500000", "47.500000", "900.200000", "'2005-09-16 00:00:00.000000'", "'2021-07-22 10:18:59.194681'", "'2021-07-22 10:18:59.194681'", "'2021-07-22 09:18:59.194681'", "to_binary('111a')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "null", "null", "null", "null", "to_binary('%!x(<nil>)')"}},
	// },
	// // Redshift Transfers
	// {
	// 	name:          "redshift2postgresql_wide",
	// 	source:        redshiftTestConnection,
	// 	target:        postgresqlTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "redshift_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from redshift_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "myboolean": "BOOL", "mychar": "VARCHAR", "mydate": "TIMESTAMPTZ", "mydoubleprecision": "FLOAT8", "myinteger": "INT4", "mynumeric": "FLOAT8", "myreal": "FLOAT4", "mysmallint": "INT4", "mytext": "VARCHAR", "mytime": "TIME", "mytimestamp": "TIMESTAMPTZ", "mytimestamptz": "TIMESTAMPTZ", "mytimetz": "1266", "myvarchar": "VARCHAR"}, Rows: []interface{}{"9223372036854775800", "true", "'car'", "'hey its ''  \"varchar\",'", "'2014-01-10 00:00:00.000000 +0000'", "435.544", "435345", "5466.453", "45.220001220703125", "3", "'helloooooo san diego'", "'03:46:38.765594'", "'22:46:38.765594+00'", "'2014-01-10 10:05:04.000000 +0000'", "'2014-01-10 18:05:04.000000 +0000'", "%!d(<nil>)", "%!t(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "%!d(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null"}},
	// },
	// {
	// 	name:          "redshift2mysql_wide",
	// 	source:        redshiftTestConnection,
	// 	target:        mysqlTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "redshift_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from redshift_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "myboolean": "TINYINT", "mychar": "TEXT", "mydate": "DATETIME", "mydoubleprecision": "DOUBLE", "myinteger": "INT", "mynumeric": "DOUBLE", "myreal": "FLOAT", "mysmallint": "INT", "mytext": "VARCHAR", "mytime": "TIME", "mytimestamp": "DATETIME", "mytimestamptz": "DATETIME", "mytimetz": "TEXT", "myvarchar": "VARCHAR"}, Rows: []interface{}{"9223372036854775800", "1", "'car'", "'hey its ''  \"varchar\",'", "'2014-01-10 00:00:00'", "435.544", "435345", "5466.453", "45.22", "3", "'helloooooo san diego'", "'03:46:39'", "'22:46:38.765594+00'", "'2014-01-10 10:05:04'", "'2014-01-10 18:05:04'", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "redshift2mssql_wide",
	// 	source:        redshiftTestConnection,
	// 	target:        mssqlTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "redshift_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from redshift_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "myboolean": "BIT", "mychar": "NVARCHAR", "mydate": "DATETIME2", "mydoubleprecision": "FLOAT", "myinteger": "INT", "mynumeric": "FLOAT", "myreal": "REAL", "mysmallint": "INT", "mytext": "NVARCHAR", "mytime": "TIME", "mytimestamp": "DATETIME2", "mytimestamptz": "DATETIME2", "mytimetz": "NVARCHAR", "myvarchar": "NVARCHAR"}, Rows: []interface{}{"9223372036854775800", "1", "'car'", "'hey its ''  \"varchar\",'", "CONVERT(DATETIME2, '2014-01-10 00:00:00.0000000', 121)", "435.544", "435345", "5466.453", "45.220001220703125", "3", "'helloooooo san diego'", "CONVERT(DATETIME2, '0001-01-01 03:46:38.7655940', 121)", "'22:46:38.765594+00'", "CONVERT(DATETIME2, '2014-01-10 10:05:04.0000000', 121)", "CONVERT(DATETIME2, '2014-01-10 18:05:04.0000000', 121)", "%!d(<nil>)", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "%!d(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "null", "'%!s(<nil>)'", "null", "null"}},
	// },
	// {
	// 	name:          "redshift2oracle_wide",
	// 	source:        redshiftTestConnection,
	// 	target:        oracleTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "redshift_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from redshift_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBIGINT": "NUMBER", "MYBOOLEAN": "NUMBER", "MYCHAR": "NCHAR", "MYDATE": "TimeStampDTY", "MYDOUBLEPRECISION": "IBDouble", "MYINTEGER": "NUMBER", "MYNUMERIC": "IBDouble", "MYREAL": "IBFloat", "MYSMALLINT": "NUMBER", "MYTEXT": "NCHAR", "MYTIME": "NCHAR", "MYTIMESTAMP": "TimeStampDTY", "MYTIMESTAMPTZ": "TimeStampDTY", "MYTIMETZ": "NCHAR", "MYVARCHAR": "NCHAR"}, Rows: []interface{}{"9223372036854775800", "1", "'car'", "'hey its ''  \"varchar\",'", "TO_TIMESTAMP('2014-01-10 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "435.544", "435345", "5466.453", "45.22", "3", "'helloooooo san diego'", "'03:46:38.765594'", "'22:46:38.765594+00'", "TO_TIMESTAMP('2014-01-10 10:05:04.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2014-01-10 18:05:04.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "<nil>", "<nil>", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null"}},
	// },
	// {
	// 	name:          "redshift2redshift_wide",
	// 	source:        redshiftTestConnection,
	// 	target:        redshiftTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "redshift_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from redshift_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "myboolean": "BOOL", "mychar": "VARCHAR", "mydate": "TIMESTAMP", "mydoubleprecision": "FLOAT8", "myinteger": "INT4", "mynumeric": "FLOAT8", "myreal": "FLOAT4", "mysmallint": "INT4", "mytext": "VARCHAR", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytimestamptz": "TIMESTAMP", "mytimetz": "1266", "myvarchar": "VARCHAR"}, Rows: []interface{}{"9223372036854775800", "true", "'car'", "'hey its ''  \"varchar\",'", "'2014-01-10 00:00:00.000000 +0000'", "435.544", "435345", "5466.453", "45.220001220703125", "3", "'helloooooo san diego'", "'03:46:38.765594'", "'22:46:38.765594+00'", "'2014-01-10 10:05:04.000000 +0000'", "'2014-01-10 18:05:04.000000 +0000'", "%!d(<nil>)", "%!t(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "%!d(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null"}},
	// },
	// {
	// 	name:          "redshift2snowflake_wide",
	// 	source:        redshiftTestConnection,
	// 	target:        snowflakeTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "redshift_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from redshift_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYBIGINT": "FIXED", "MYBOOLEAN": "BOOLEAN", "MYCHAR": "TEXT", "MYDATE": "TIMESTAMP_NTZ", "MYDOUBLEPRECISION": "REAL", "MYINTEGER": "FIXED", "MYNUMERIC": "REAL", "MYREAL": "REAL", "MYSMALLINT": "FIXED", "MYTEXT": "TEXT", "MYTIME": "TEXT", "MYTIMESTAMP": "TIMESTAMP_NTZ", "MYTIMESTAMPTZ": "TIMESTAMP_NTZ", "MYTIMETZ": "TEXT", "MYVARCHAR": "TEXT"}, Rows: []interface{}{"9223372036854775800", "true", "'car'", "'hey its ''  \"varchar\",'", "'2014-01-10 00:00:00.000000'", "435.544000", "435345", "5466.453000", "45.220001", "3", "'helloooooo san diego'", "'03:46:38.765594'", "'22:46:38.765594+00'", "'2014-01-10 10:05:04.000000'", "'2014-01-10 18:05:04.000000'", "%!s(<nil>)", "%!t(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null"}},
	// },
	// // Snowflake Transfers
	// {
	// 	name:          "snowflake2postgresql_wide",
	// 	source:        snowflakeTestConnection,
	// 	target:        postgresqlTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "snowflake_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from snowflake_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"myarray": "VARCHAR", "mybinary": "BYTEA", "myboolean": "BOOL", "mydate": "TIMESTAMPTZ", "myfloat": "FLOAT8", "mygeography": "VARCHAR", "myint": "INT8", "mynumber": "FLOAT8", "myobject": "VARCHAR", "mytime": "TIMESTAMPTZ", "mytimestamp_ltz": "TIMESTAMPTZ", "mytimestamp_ntz": "TIMESTAMPTZ", "mytimestamp_tz": "TIMESTAMPTZ", "myvarchar": "VARCHAR", "myvariant": "VARCHAR"}, Rows: []interface{}{"25.5", "22", "42.5", "'hellooooo h''er\"es ,my varchar value'", "'\\x0011'", "true", "'2000-10-15 01:00:00.000000 +0100'", "'0001-01-01 23:17:16.000000 -0036'", "'2000-10-16 07:54:01.345673 +0100'", "'2000-10-16 00:54:01.345673 +0100'", "'2000-10-15 23:54:01.345673 +0100'", "'{  \"mykey\": \"this is \\\"my'' v,al\"}'", "'{  \"key3\": \"value3\",  \"key4\": \"value4\"}'", "'[  true,  1,  -1.200000000000000e-03,  \"Abc\",  [    \"x\",    \"y\"  ],  {    \"a\": 1  }]'", "'{  \"coordinates\": [    -122.35,    37.55  ],  \"type\": \"Point\"}'", "%!g(<nil>)", "%!d(<nil>)", "%!g(<nil>)", "'%!s(<nil>)'", "'\\x%!x(<nil>)'", "%!t(<nil>)", "null", "null", "null", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "snowflake2mysql_wide",
	// 	source:        snowflakeTestConnection,
	// 	target:        mysqlTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "snowflake_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from snowflake_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYARRAY": "TEXT", "MYBINARY": "BLOB", "MYBOOLEAN": "TINYINT", "MYDATE": "DATETIME", "MYFLOAT": "DOUBLE", "MYGEOGRAPHY": "TEXT", "MYINT": "BIGINT", "MYNUMBER": "DOUBLE", "MYOBJECT": "TEXT", "MYTIME": "DATETIME", "MYTIMESTAMP_LTZ": "DATETIME", "MYTIMESTAMP_NTZ": "DATETIME", "MYTIMESTAMP_TZ": "DATETIME", "MYVARCHAR": "TEXT", "MYVARIANT": "TEXT"}, Rows: []interface{}{"25.5", "22", "42.5", "'hellooooo h''er\"es ,my varchar value'", "x'0011'", "1", "'2000-10-15 00:00:00'", "'0001-01-01 23:54:01'", "'2000-10-16 06:54:01'", "'2000-10-15 23:54:01'", "'2000-10-15 22:54:01'", "'{  \"mykey\": \"this is \"my'' v,al\"}'", "'{  \"key3\": \"value3\",  \"key4\": \"value4\"}'", "'[  true,  1,  -1.200000000000000e-03,  \"Abc\",  [    \"x\",    \"y\"  ],  {    \"a\": 1  }]'", "'{  \"coordinates\": [    -122.35,    37.55  ],  \"type\": \"Point\"}'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "x'%!x(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "snowflake2mssql_wide",
	// 	source:        snowflakeTestConnection,
	// 	target:        mssqlTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "snowflake_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from snowflake_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYARRAY": "NVARCHAR", "MYBINARY": "VARBINARY", "MYBOOLEAN": "BIT", "MYDATE": "DATETIME2", "MYFLOAT": "FLOAT", "MYGEOGRAPHY": "NVARCHAR", "MYINT": "BIGINT", "MYNUMBER": "FLOAT", "MYOBJECT": "NVARCHAR", "MYTIME": "DATETIME2", "MYTIMESTAMP_LTZ": "DATETIME2", "MYTIMESTAMP_NTZ": "DATETIME2", "MYTIMESTAMP_TZ": "DATETIME2", "MYVARCHAR": "NVARCHAR", "MYVARIANT": "NVARCHAR"}, Rows: []interface{}{"25.5", "22", "42.5", "'hellooooo h''er\"es ,my varchar value'", "CONVERT(VARBINARY(8000), '0x0011', 1)", "1", "CONVERT(DATETIME2, '2000-10-15 00:00:00.0000000', 121)", "CONVERT(DATETIME2, '0001-01-01 23:54:01.0000000', 121)", "CONVERT(DATETIME2, '2000-10-16 07:54:01.3456730', 121)", "CONVERT(DATETIME2, '2000-10-15 23:54:01.3456730', 121)", "CONVERT(DATETIME2, '2000-10-15 23:54:01.3456730', 121)", "'{  \"mykey\": \"this is \\\"my'' v,al\"}'", "'{  \"key3\": \"value3\",  \"key4\": \"value4\"}'", "'[  true,  1,  -1.200000000000000e-03,  \"Abc\",  [    \"x\",    \"y\"  ],  {    \"a\": 1  }]'", "'{  \"coordinates\": [    -122.35,    37.55  ],  \"type\": \"Point\"}'", "%!g(<nil>)", "%!d(<nil>)", "%!g(<nil>)", "'%!s(<nil>)'", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "null", "null", "null", "null", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "snowflake2oracle_wide",
	// 	source:        snowflakeTestConnection,
	// 	target:        oracleTestConnection,
	// 	overwrite:     true,
	// 	targetTable:   "snowflake_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from snowflake_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYARRAY": "NCHAR", "MYBINARY": "OCIBlobLocator", "MYBOOLEAN": "NUMBER", "MYDATE": "TimeStampDTY", "MYFLOAT": "IBDouble", "MYGEOGRAPHY": "NCHAR", "MYINT": "NUMBER", "MYNUMBER": "IBDouble", "MYOBJECT": "NCHAR", "MYTIME": "TimeStampDTY", "MYTIMESTAMP_LTZ": "TimeStampDTY", "MYTIMESTAMP_NTZ": "TimeStampDTY", "MYTIMESTAMP_TZ": "TimeStampDTY", "MYVARCHAR": "NCHAR", "MYVARIANT": "NCHAR"}, Rows: []interface{}{"25.5", "22", "42.5", "'hellooooo h''er\"es ,my varchar value'", "hextoraw('0011')", "1", "TO_TIMESTAMP('2000-10-15 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('0001-01-01 23:54:01.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2000-10-16 07:54:01.345673', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2000-10-15 23:54:01.345673', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2000-10-15 23:54:01.345673', 'YYYY-MM-DD HH24:MI:SS.FF')", "'{  \"mykey\": \"this is \\\"my'' v,al\"}'", "'{  \"key3\": \"value3\",  \"key4\": \"value4\"}'", "'[  true,  1,  -1.200000000000000e-03,  \"Abc\",  [    \"x\",    \"y\"  ],  {    \"a\": 1  }]'", "'{  \"coordinates\": [    -122.35,    37.55  ],  \"type\": \"Point\"}'", "<nil>", "<nil>", "<nil>", "'%!s(<nil>)'", "hextoraw('%!x(<nil>)')", "<nil>", "null", "null", "null", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "snowflake2redshift_wide",
	// 	source:        snowflakeTestConnection,
	// 	target:        redshiftTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "snowflake_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from snowflake_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"myarray": "VARCHAR", "mybinary": "VARCHAR", "myboolean": "BOOL", "mydate": "TIMESTAMP", "myfloat": "FLOAT8", "mygeography": "VARCHAR", "myint": "INT8", "mynumber": "FLOAT8", "myobject": "VARCHAR", "mytime": "TIMESTAMP", "mytimestamp_ltz": "TIMESTAMP", "mytimestamp_ntz": "TIMESTAMP", "mytimestamp_tz": "TIMESTAMP", "myvarchar": "VARCHAR", "myvariant": "VARCHAR"}, Rows: []interface{}{"25.5", "22", "42.5", "'hellooooo h''er\"es ,my varchar value'", "'0011'", "true", "'2000-10-15 00:00:00.000000 +0000'", "'0001-01-01 23:54:01.000000 +0000'", "'2000-10-16 07:54:01.345673 +0000'", "'2000-10-15 23:54:01.345673 +0000'", "'2000-10-15 23:54:01.345673 +0000'", "'{  \"mykey\": \"this is \"my'' v,al\"}'", "'{  \"key3\": \"value3\",  \"key4\": \"value4\"}'", "'[  true,  1,  -1.200000000000000e-03,  \"Abc\",  [    \"x\",    \"y\"  ],  {    \"a\": 1  }]'", "'{  \"coordinates\": [    -122.35,    37.55  ],  \"type\": \"Point\"}'", "%!g(<nil>)", "%!d(<nil>)", "%!g(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!t(<nil>)", "null", "null", "null", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
	// {
	// 	name:          "snowflake2snowflake_wide",
	// 	source:        snowflakeTestConnection,
	// 	target:        snowflakeTestConnection,
	// 	overwrite:     true,
	// 	targetSchema:  "public",
	// 	targetTable:   "snowflake_wide_table",
	// 	transferQuery: "select * from wide_table",
	// 	checkQuery:    "select * from snowflake_wide_table",
	// 	checkResult:   QueryResult{ColumnTypes: map[string]string{"MYARRAY": "ARRAY", "MYBINARY": "BINARY", "MYBOOLEAN": "BOOLEAN", "MYDATE": "TIMESTAMP_NTZ", "MYFLOAT": "REAL", "MYGEOGRAPHY": "OBJECT", "MYINT": "FIXED", "MYNUMBER": "REAL", "MYOBJECT": "OBJECT", "MYTIME": "TIMESTAMP_NTZ", "MYTIMESTAMP_LTZ": "TIMESTAMP_NTZ", "MYTIMESTAMP_NTZ": "TIMESTAMP_NTZ", "MYTIMESTAMP_TZ": "TIMESTAMP_NTZ", "MYVARCHAR": "TEXT", "MYVARIANT": "VARIANT"}, Rows: []interface{}{"25.500000", "22", "42.500000", "'hellooooo h''er\"es ,my varchar value'", "to_binary('0011')", "true", "'2000-10-15 00:00:00.000000'", "'0001-01-01 23:54:01.000000'", "'2000-10-16 07:54:01.345673'", "'2000-10-15 23:54:01.345673'", "'2000-10-15 23:54:01.345673'", "'{\n  \"mykey\": \"this is \\\\\"my'' v,al\"\n}'", "'{\n  \"key3\": \"value3\",\n  \"key4\": \"value4\"\n}'", "'[\n  true,\n  1,\n  -1.200000000000000e-03,\n  \"Abc\",\n  [\n    \"x\",\n    \"y\"\n  ],\n  {\n    \"a\": 1\n  }\n]'", "'{\n  \"coordinates\": [\n    -122.35,\n    37.55\n  ],\n  \"type\": \"Point\"\n}'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "to_binary('%!x(<nil>)')", "%!t(<nil>)", "null", "null", "null", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}},
	// },
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
