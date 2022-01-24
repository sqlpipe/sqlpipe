package engine

import (
	"os"

	"github.com/calmitchell617/sqlpipe/internal/data"
)

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

// PostgreSQL Setup
var (
	postgresqlWideTableDropErrProperties = map[string]string{
		"dsType": "postgresql",
		"error":  `ERROR: relation "wide_table" does not exist (SQLSTATE 42P01)`,
		"query":  "select * from wide_table",
	}

	postgresqlWideTableCreateQuery = `
	create table wide_table(
		mybigint bigint,
		mybit bit(5),
		mybitvarying varbit,
		myboolean boolean,
		mybox box,
		mybytea bytea,
		mychar char(3),
		myvarchar varchar(100),
		mycidr cidr,
		mycircle circle,
		mydate date,
		mydoubleprecision double precision,
		myinet inet,
		myinteger integer,
		myinterval interval,
		myjson json,
		myjsonb jsonb,
		myline line,
		mylseg lseg,
		mymacaddr macaddr,
		mymoney money,
		mynumeric numeric(10,5),
		mypath path,
		mypg_lsn pg_lsn,
		mypoint point,
		mypolygon polygon,
		myreal real,
		mysmallint smallint,
		mytext text,
		mytime time,
		mytimetz timetz,
		mytimestamp timestamp,
		mytimestamptz timestamptz,
		mytsquery tsquery,
		mytsvector tsvector,
		myuuid uuid,
		myxml xml
	);
	`
	postgresqlWideTableCreateResult = QueryResult{
		ColumnTypes: map[string]string{"mybigint": "INT8",
			"mybit":             "BIT",
			"mybitvarying":      "VARBIT",
			"myboolean":         "BOOL",
			"mybox":             "BOX",
			"mybytea":           "BYTEA",
			"mychar":            "BPCHAR",
			"mycidr":            "CIDR",
			"mycircle":          "CIRCLE",
			"mydate":            "DATE",
			"mydoubleprecision": "FLOAT8",
			"myinet":            "INET",
			"myinteger":         "INT4",
			"myinterval":        "INTERVAL",
			"myjson":            "JSON",
			"myjsonb":           "JSONB",
			"myline":            "LINE",
			"mylseg":            "LSEG",
			"mymacaddr":         "MACADDR",
			"mymoney":           "790",
			"mynumeric":         "NUMERIC",
			"mypath":            "PATH",
			"mypg_lsn":          "3220",
			"mypoint":           "POINT",
			"mypolygon":         "POLYGON",
			"myreal":            "FLOAT4",
			"mysmallint":        "INT2",
			"mytext":            "TEXT",
			"mytime":            "TIME",
			"mytimestamp":       "TIMESTAMP",
			"mytimestamptz":     "TIMESTAMPTZ",
			"mytimetz":          "1266",
			"mytsquery":         "3615",
			"mytsvector":        "3614",
			"myuuid":            "UUID",
			"myvarchar":         "VARCHAR",
			"myxml":             "142",
		},
		Rows: []interface{}{},
	}

	postgresqlWideTableInsertQuery = `
	INSERT INTO wide_table(
		mybigint,
		mybit,
		mybitvarying,
		myboolean,
		mybox,
		mybytea,
		mychar,
		myvarchar,
		mycidr,
		mycircle,
		mydate,
		mydoubleprecision,
		myinet,
		myinteger,
		myinterval,
		myjson,
		myjsonb,
		myline,
		mylseg,
		mymacaddr,
		mymoney ,
		mynumeric,
		mypath,
		mypg_lsn,
		mypoint,
		mypolygon,
		myreal,
		mysmallint,
		mytext,
		mytime,
		mytimetz,
		mytimestamp,
		mytimestamptz,
		mytsquery,
		mytsvector,
		myuuid,
		myxml
	) values (
		6514798382812790784,
		B'10001',
		B'1001',
		true,
		'(8,9), (1,3)',
		'\xAAAABBBB',
		'abc',
		'"my"varch''ar,123@gmail.com',
		'192.168.100.128/25',
		'(( 1 , 5 ), 5)',
		'2014-01-10 20:14:54.140332'::date,
		529.56218983375436,
		'192.168.100.128',
		745910651,
		(timestamptz '2014-01-20 20:00:00 PST' - timestamptz '2014-01-10 10:00:00 PST'),
		'{"mykey": "this\"  ''is'' m,y val"}',
		'{"mykey": "this is my val"}',
		'{1, 5, 20}',
		'[(5, 4), (2, 1)]',
		'08:00:2b:01:02:03',
		'$35,244.33'::money,
		449.82115,
		'[( 1, 4), (8, 7)]',
		'16/B374D848'::pg_lsn,
		'(5, 7)',
		'((5, 8), (6, 10), (7, 20))',
		9673.1094,
		24345,
		'myte",xt123@gmail.com',
		'03:46:38.765594+05',
		'03:46:38.765594+05',
		'2014-01-10 10:05:04 PST',
		'2014-01-10 10:05:04 PST',
		'fat & rat'::tsquery,
		'a fat cat sat on a mat and ate a fat rat'::tsvector,
		'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid,
		'<foo>bar</foo>'),(
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null);
	`
	postgresqlWideTableInsertResult = QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybit": "BIT", "mybitvarying": "VARBIT", "myboolean": "BOOL", "mybox": "BOX", "mybytea": "BYTEA", "mychar": "BPCHAR", "mycidr": "CIDR", "mycircle": "CIRCLE", "mydate": "DATE", "mydoubleprecision": "FLOAT8", "myinet": "INET", "myinteger": "INT4", "myinterval": "INTERVAL", "myjson": "JSON", "myjsonb": "JSONB", "myline": "LINE", "mylseg": "LSEG", "mymacaddr": "MACADDR", "mymoney": "790", "mynumeric": "NUMERIC", "mypath": "PATH", "mypg_lsn": "3220", "mypoint": "POINT", "mypolygon": "POLYGON", "myreal": "FLOAT4", "mysmallint": "INT2", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytimestamptz": "TIMESTAMPTZ", "mytimetz": "1266", "mytsquery": "3615", "mytsvector": "3614", "myuuid": "UUID", "myvarchar": "VARCHAR", "myxml": "142"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "true", "'(8,9),(1,3)'", "'\\xaaaabbbb'", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "'2014-01-10 00:00:00.000000 +0000'", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\\\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.109375", "24345", "'myte\",xt123@gmail.com'", "'03:46:38.765594'", "'03:46:38.765594+05'", "'2014-01-10 10:05:04.000000 +0000'", "'2014-01-10 18:05:04.000000 +0000'", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!t(<nil>)", "'%!s(<nil>)'", "'\\x%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}}
)

// MySQL Setup
var (
	mysqlWideTableDropErrProperties = map[string]string{"dsType": "mysql", "error": "Error 1146: Table 'testing.wide_table' doesn't exist", "query": "select * from wide_table"}
	mysqlWideTableCreateQuery       = `
	create table wide_table(
		myserial serial,
		mybit bit,
		mybit5 bit(5),
		mybit64 bit(64),
		mytinyint tinyint,
		mysmallint smallint,
		mymediumint mediumint,
		myint int,
		mybigint bigint,
		mydecimal decimal(10, 5),
		myfloat float,
		mydouble double,
		mydate date,
		mytime time,
		mydatetime datetime,
		mytimestamp timestamp,
		myyear year,
		mychar char(3),
		myvarchar varchar(200),
		mynchar nchar(3),
		mynvarchar nvarchar(200),
		mybinary binary(3),
		myvarbinary varbinary(200),
		mytinyblob tinyblob,
		mymediumblob mediumblob,
		myblob blob,
		mylongblob longblob,
		mytinytext tinytext,
		mytext text,
		mymediumtext mediumtext,
		mylongtext longtext,
		myenum ENUM('enumval1', 'enumval2'),
		myset SET('setval1', 'setval2'),
		mygeometry geometry,
		mypoint point,
		mylinestring linestring,
		mypolygon polygon,
		mymultipoint multipoint,
		mymultilinestring multilinestring,
		mymultipolygon multipolygon,
		mygeometrycollection geometrycollection,
		myjson json
	);`
	mysqlWideTableCreateResult = QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybinary": "BINARY", "mybit": "BIT", "mybit5": "BIT", "mybit64": "BIT", "myblob": "BLOB", "mychar": "CHAR", "mydate": "DATE", "mydatetime": "DATETIME", "mydecimal": "DECIMAL", "mydouble": "DOUBLE", "myenum": "CHAR", "myfloat": "FLOAT", "mygeometry": "GEOMETRY", "mygeometrycollection": "GEOMETRY", "myint": "INT", "myjson": "JSON", "mylinestring": "GEOMETRY", "mylongblob": "BLOB", "mylongtext": "TEXT", "mymediumblob": "BLOB", "mymediumint": "MEDIUMINT", "mymediumtext": "TEXT", "mymultilinestring": "GEOMETRY", "mymultipoint": "GEOMETRY", "mymultipolygon": "GEOMETRY", "mynchar": "CHAR", "mynvarchar": "VARCHAR", "mypoint": "GEOMETRY", "mypolygon": "GEOMETRY", "myserial": "BIGINT", "myset": "CHAR", "mysmallint": "SMALLINT", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytinyblob": "BLOB", "mytinyint": "TINYINT", "mytinytext": "TEXT", "myvarbinary": "VARBINARY", "myvarchar": "VARCHAR", "myyear": "YEAR"}, Rows: []interface{}{}}

	mysqlWideTableInsertQuery = `
	insert into wide_table (
		mybit,
		mybit5,
		mybit64,
		mytinyint,
		mysmallint,
		mymediumint,
		myint,
		mybigint,
		mydecimal,
		myfloat,
		mydouble,
		mydate,
		mytime,
		mydatetime,
		mytimestamp,
		myyear,
		mychar,
		myvarchar,
		mynchar,
		mynvarchar,
		mybinary,
		myvarbinary,
		mytinyblob,
		mymediumblob,
		myblob,
		mylongblob,
		mytinytext,
		mytext,
		mymediumtext,
		mylongtext,
		myenum,
		myset,
		mygeometry,
		mypoint,
		mylinestring,
		mypolygon,
		mymultipoint,
		mymultilinestring,
		mymultipolygon,
		mygeometrycollection,
		myjson
	) VALUES 
	(
		1,
		b'01010',
		b'1111111111111111111111111111111111111111111111111111111111111111',
		2,
		5,
		50,
		4595435,
		392809438543,
		30.5,
		45.9,
		54.3,
		'2009-05-28',
		'14:23:54.105302',
		'2010-10-24 20:52:51.969491',
		'1989-02-22 3:17:21.243061',
		1905,
		'chr',
		'my varchar ''st"ri,ng wheeeee',
		'ncr',
		'my nvarchar string wheeeee',
		'bnr',
		'my binary string wahooooo',
		'blob city bb',
		'blob city bb',
		'blob city bb',
		'blob city bb',
		'text city bb',
		'text city bb',
		'text city bb',
		'text city bb',
		'enumval1',
		'setval1',
		ST_GeomFromText('POINT(1 1)'),
		ST_GeomFromText('POINT(1 1)'),
		ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'),
		ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))'),
		ST_GEOMFROMTEXT('MultiPoint( 1 1, 2 2, 5 3, 7 2, 9 3, 8 4, 6 6, 6 9, 4 9, 1 5 )'),
		ST_GEOMFROMTEXT('MultiLineString((1 1,2 2,3 3),(4 4,5 5))'),
		ST_GEOMFROMTEXT('MultiPolygon(((0 0,0 3,3 3,3 0,0 0),(1 1,1 2,2 2,2 1,1 1)))'),
		ST_GEOMFROMTEXT('MultiPolygon(((0 0,0 3,3 3,3 0,0 0),(1 1,1 2,2 2,2 1,1 1)))'),
		'{"mykey": "this is\\" m\\"y, ''val''"}'
	),(
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null
	);
	`

	mysqlWideTableInsertResult = QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybinary": "BINARY", "mybit": "BIT", "mybit5": "BIT", "mybit64": "BIT", "myblob": "BLOB", "mychar": "CHAR", "mydate": "DATE", "mydatetime": "DATETIME", "mydecimal": "DECIMAL", "mydouble": "DOUBLE", "myenum": "CHAR", "myfloat": "FLOAT", "mygeometry": "GEOMETRY", "mygeometrycollection": "GEOMETRY", "myint": "INT", "myjson": "JSON", "mylinestring": "GEOMETRY", "mylongblob": "BLOB", "mylongtext": "TEXT", "mymediumblob": "BLOB", "mymediumint": "MEDIUMINT", "mymediumtext": "TEXT", "mymultilinestring": "GEOMETRY", "mymultipoint": "GEOMETRY", "mymultipolygon": "GEOMETRY", "mynchar": "CHAR", "mynvarchar": "VARCHAR", "mypoint": "GEOMETRY", "mypolygon": "GEOMETRY", "myserial": "BIGINT", "myset": "CHAR", "mysmallint": "SMALLINT", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytinyblob": "BLOB", "mytinyint": "TINYINT", "mytinytext": "TEXT", "myvarbinary": "VARBINARY", "myvarchar": "VARCHAR", "myyear": "YEAR"}, Rows: []interface{}{"1", "x'01'", "x'0a'", "x'ffffffffffffffff'", "2", "5", "50", "4595435", "392809438543", "30.50000", "45.9", "54.3", "'2009-05-28'", "'14:23:54'", "'2010-10-24 20:52:52'", "'1989-02-22 03:17:21'", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "x'626e72'", "x'6d792062696e61727920737472696e67207761686f6f6f6f6f'", "x'626c6f622063697479206262'", "x'626c6f622063697479206262'", "x'626c6f622063697479206262'", "x'626c6f622063697479206262'", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "'\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'\x00\x00\x00\x00\x01\x02\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@'", "'\x00\x00\x00\x00\x01\x03\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@'", "'\x00\x00\x00\x00\x01\x04\x00\x00\x00\n\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\b@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x00@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\"@\x00\x00\x00\x00\x00\x00\b@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00 @\x00\x00\x00\x00\x00\x00\x10@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x18@\x00\x00\x00\x00\x00\x00\x18@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x18@\x00\x00\x00\x00\x00\x00\"@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\"@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x14@'", "'\x00\x00\x00\x00\x01\x05\x00\x00\x00\x02\x00\x00\x00\x01\x02\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@'", "'\x00\x00\x00\x00\x01\x06\x00\x00\x00\x01\x00\x00\x00\x01\x03\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'\x00\x00\x00\x00\x01\x06\x00\x00\x00\x01\x00\x00\x00\x01\x03\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'{\"mykey\": \"this is\\\\\" m\\\\\"y, ''val''\"}'", "2", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}}
)

// MSSQL Setup
var (
	mssqlTestingDbDropErrProperties   = map[string]string{"dsType": "mssql", "error": "mssql: Cannot drop the database 'testing', because it does not exist or you do not have permission.", "query": "drop database testing; commit;"}
	mssqlTestingDbCreateErrProperties = map[string]string{"dsType": "mssql", "error": "mssql: Database 'testing' already exists. Choose a different database name.", "query": "create database testing;"}
)

var (
	// PostgreSQL Transfers
	postgresql2postgresql_wide_result = QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybit": "VARBIT", "mybitvarying": "VARBIT", "myboolean": "BOOL", "mybox": "BOX", "mybytea": "BYTEA", "mychar": "VARCHAR", "mycidr": "CIDR", "mycircle": "CIRCLE", "mydate": "TIMESTAMPTZ", "mydoubleprecision": "FLOAT8", "myinet": "INET", "myinteger": "INT4", "myinterval": "INTERVAL", "myjson": "JSON", "myjsonb": "JSONB", "myline": "LINE", "mylseg": "LSEG", "mymacaddr": "MACADDR", "mymoney": "VARCHAR", "mynumeric": "FLOAT8", "mypath": "PATH", "mypg_lsn": "3220", "mypoint": "POINT", "mypolygon": "POLYGON", "myreal": "FLOAT4", "mysmallint": "INT4", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMPTZ", "mytimestamptz": "TIMESTAMPTZ", "mytimetz": "1266", "mytsquery": "3615", "mytsvector": "3614", "myuuid": "UUID", "myvarchar": "VARCHAR", "myxml": "142"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "true", "'(8,9),(1,3)'", "'\\xaaaabbbb'", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "'2014-01-10 00:00:00.000000 +0000'", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\\\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.109375", "24345", "'myte\",xt123@gmail.com'", "'03:46:38.765594'", "'03:46:38.765594+05'", "'2014-01-10 10:05:04.000000 +0000'", "'2014-01-10 18:05:04.000000 +0000'", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!t(<nil>)", "'%!s(<nil>)'", "'\\x%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}}
	postgresql2mysql_wide_result      = QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybit": "TEXT", "mybitvarying": "TEXT", "myboolean": "TINYINT", "mybox": "TEXT", "mybytea": "BLOB", "mychar": "TEXT", "mycidr": "TEXT", "mycircle": "TEXT", "mydate": "DATETIME", "mydoubleprecision": "DOUBLE", "myinet": "TEXT", "myinteger": "INT", "myinterval": "TEXT", "myjson": "JSON", "myjsonb": "JSON", "myline": "TEXT", "mylseg": "TEXT", "mymacaddr": "TEXT", "mymoney": "TEXT", "mynumeric": "DOUBLE", "mypath": "TEXT", "mypg_lsn": "TEXT", "mypoint": "TEXT", "mypolygon": "TEXT", "myreal": "FLOAT", "mysmallint": "INT", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "DATETIME", "mytimestamptz": "DATETIME", "mytimetz": "TIME", "mytsquery": "TEXT", "mytsvector": "TEXT", "myuuid": "TEXT", "myvarchar": "VARCHAR", "myxml": "TEXT"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "1", "'(8,9),(1,3)'", "x'786161616162626262'", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "'2014-01-10 00:00:00'", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\\\\\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.11", "24345", "'myte\",xt123@gmail.com'", "'03:46:39'", "'03:46:39'", "'2014-01-10 10:05:04'", "'2014-01-10 18:05:04'", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "x'%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}}
	postgresql2mssql_wide_result      = QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybit": "VARCHAR", "mybitvarying": "VARCHAR", "myboolean": "BIT", "mybox": "VARCHAR", "mybytea": "VARBINARY", "mychar": "NVARCHAR", "mycidr": "VARCHAR", "mycircle": "VARCHAR", "mydate": "DATETIME2", "mydoubleprecision": "FLOAT", "myinet": "VARCHAR", "myinteger": "INT", "myinterval": "VARCHAR", "myjson": "NVARCHAR", "myjsonb": "NVARCHAR", "myline": "VARCHAR", "mylseg": "VARCHAR", "mymacaddr": "VARCHAR", "mymoney": "VARCHAR", "mynumeric": "FLOAT", "mypath": "VARCHAR", "mypg_lsn": "VARCHAR", "mypoint": "VARCHAR", "mypolygon": "VARCHAR", "myreal": "REAL", "mysmallint": "INT", "mytext": "NTEXT", "mytime": "TIME", "mytimestamp": "DATETIME2", "mytimestamptz": "DATETIME2", "mytimetz": "VARCHAR", "mytsquery": "NVARCHAR", "mytsvector": "NVARCHAR", "myuuid": "UNIQUEIDENTIFIER", "myvarchar": "NVARCHAR", "myxml": "XML"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "1", "'(8,9),(1,3)'", "CONVERT(VARBINARY(8000), '0xaaaabbbb', 1)", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "CONVERT(DATETIME2, '2014-01-10 00:00:00.0000000', 121)", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\\\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.109375", "24345", "'myte\",xt123@gmail.com'", "CONVERT(TIME, '03:46:38.765', 121)", "'03:46:38.765594+05'", "CONVERT(DATETIME2, '2014-01-10 10:05:04.0000000', 121)", "CONVERT(DATETIME2, '2014-01-10 18:05:04.0000000', 121)", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "N'A0EEBC99-9CB-4EF8-BB6D-6BB9BD380A11'", "'<foo>bar</foo>'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "'%!s(<nil>)'", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "null", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "'%!s(<nil>)'"}}
	postgresql2oracle_wide_result     = QueryResult{ColumnTypes: map[string]string{"MYBIGINT": "NUMBER", "MYBIT": "NCHAR", "MYBITVARYING": "NCHAR", "MYBOOLEAN": "NUMBER", "MYBOX": "NCHAR", "MYBYTEA": "OCIBlobLocator", "MYCHAR": "NCHAR", "MYCIDR": "NCHAR", "MYCIRCLE": "NCHAR", "MYDATE": "TimeStampDTY", "MYDOUBLEPRECISION": "IBDouble", "MYINET": "NCHAR", "MYINTEGER": "NUMBER", "MYINTERVAL": "NCHAR", "MYJSON": "NCHAR", "MYJSONB": "NCHAR", "MYLINE": "NCHAR", "MYLSEG": "NCHAR", "MYMACADDR": "NCHAR", "MYMONEY": "NCHAR", "MYNUMERIC": "IBDouble", "MYPATH": "NCHAR", "MYPG_LSN": "NCHAR", "MYPOINT": "NCHAR", "MYPOLYGON": "NCHAR", "MYREAL": "IBFloat", "MYSMALLINT": "NUMBER", "MYTEXT": "NCHAR", "MYTIME": "NCHAR", "MYTIMESTAMP": "TimeStampDTY", "MYTIMESTAMPTZ": "TimeStampDTY", "MYTIMETZ": "NCHAR", "MYTSQUERY": "NCHAR", "MYTSVECTOR": "NCHAR", "MYUUID": "NCHAR", "MYVARCHAR": "NCHAR", "MYXML": "NCHAR"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "1", "'(8,9),(1,3)'", "hextoraw('aaaabbbb')", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "TO_TIMESTAMP('2014-01-10 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\\\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.109", "24345", "'myte\",xt123@gmail.com'", "'03:46:38.765594'", "'03:46:38.765594+05'", "TO_TIMESTAMP('2014-01-10 10:05:04.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('2014-01-10 18:05:04.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "<nil>", "'%!s(<nil>)'", "'%!s(<nil>)'", "<nil>", "'%!s(<nil>)'", "hextoraw('%!x(<nil>)')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "<nil>", "'%!s(<nil>)'", "<nil>", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "<nil>", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "<nil>", "<nil>", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}}
	postgresql2redshift_wide_result   = QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybit": "VARCHAR", "mybitvarying": "VARCHAR", "myboolean": "BOOL", "mybox": "VARCHAR", "mybytea": "VARCHAR", "mychar": "VARCHAR", "mycidr": "VARCHAR", "mycircle": "VARCHAR", "mydate": "TIMESTAMP", "mydoubleprecision": "FLOAT8", "myinet": "VARCHAR", "myinteger": "INT4", "myinterval": "VARCHAR", "myjson": "VARCHAR", "myjsonb": "VARCHAR", "myline": "VARCHAR", "mylseg": "VARCHAR", "mymacaddr": "VARCHAR", "mymoney": "VARCHAR", "mynumeric": "FLOAT8", "mypath": "VARCHAR", "mypg_lsn": "VARCHAR", "mypoint": "VARCHAR", "mypolygon": "VARCHAR", "myreal": "FLOAT4", "mysmallint": "INT4", "mytext": "VARCHAR", "mytime": "VARCHAR", "mytimestamp": "TIMESTAMP", "mytimestamptz": "TIMESTAMP", "mytimetz": "1266", "mytsquery": "VARCHAR", "mytsvector": "VARCHAR", "myuuid": "VARCHAR", "myvarchar": "VARCHAR", "myxml": "VARCHAR"}, Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "true", "'(8,9),(1,3)'", "'aaaabbbb'", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "'2014-01-10 00:00:00.000000 +0000'", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.109375", "24345", "'myte\",xt123@gmail.com'", "'03:46:38.765594'", "'22:46:38.765594+00'", "'2014-01-10 10:05:04.000000 +0000'", "'2014-01-10 18:05:04.000000 +0000'", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!t(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}}
	postgresql2snowflake_wide_result  = QueryResult{ColumnTypes: map[string]string{"MYBIGINT": "FIXED", "MYBIT": "TEXT", "MYBITVARYING": "TEXT", "MYBOOLEAN": "BOOLEAN", "MYBOX": "TEXT", "MYBYTEA": "BINARY", "MYCHAR": "TEXT", "MYCIDR": "TEXT", "MYCIRCLE": "TEXT", "MYDATE": "TIMESTAMP_NTZ", "MYDOUBLEPRECISION": "REAL", "MYINET": "TEXT", "MYINTEGER": "FIXED", "MYINTERVAL": "TEXT", "MYJSON": "VARIANT", "MYJSONB": "VARIANT", "MYLINE": "TEXT", "MYLSEG": "TEXT", "MYMACADDR": "TEXT", "MYMONEY": "TEXT", "MYNUMERIC": "REAL", "MYPATH": "TEXT", "MYPG_LSN": "TEXT", "MYPOINT": "TEXT", "MYPOLYGON": "TEXT", "MYREAL": "REAL", "MYSMALLINT": "FIXED", "MYTEXT": "TEXT", "MYTIME": "TIME", "MYTIMESTAMP": "TIMESTAMP_NTZ", "MYTIMESTAMPTZ": "TIMESTAMP_NTZ", "MYTIMETZ": "TEXT", "MYTSQUERY": "TEXT", "MYTSVECTOR": "TEXT", "MYUUID": "TEXT", "MYVARCHAR": "TEXT", "MYXML": "TEXT"}, Rows: []interface{}{"%!g(string=6514798382812790784)", "'10001'", "'1001'", "true", "'(8,9),(1,3)'", "to_binary('aaaabbbb')", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "'2014-01-10 00:00:00.000000'", "%!g(string=529.562190)", "'192.168.100.128'", "%!g(string=745910651)", "'10 days 10:00:00'", "'{\n  \"mykey\": \"this\\\\\"  ''is'' m,y val\"\n}'", "'{\n  \"mykey\": \"this is my val\"\n}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "%!g(string=449.821150)", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "%!g(string=9673.109375)", "%!g(string=24345)", "'myte\",xt123@gmail.com'", "'0001-01-01 03:46:38.765594'", "'03:46:38.765594+05'", "'2014-01-10 10:05:04.000000'", "'2014-01-10 18:05:04.000000'", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "%!g(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!t(<nil>)", "'%!s(<nil>)'", "to_binary('%!x(<nil>)')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "%!g(<nil>)", "'%!s(<nil>)'", "%!g(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!g(<nil>)", "'%!s(<nil>)'", "null", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}}
	// MySQL Transfers
	mysql2postgresql_wide_result = QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybinary": "BYTEA", "mybit": "VARBIT", "mybit5": "VARBIT", "mybit64": "VARBIT", "myblob": "BYTEA", "mychar": "VARCHAR", "mydate": "DATE", "mydatetime": "TIMESTAMP", "mydecimal": "NUMERIC", "mydouble": "FLOAT8", "myenum": "VARCHAR", "myfloat": "FLOAT4", "mygeometry": "BYTEA", "mygeometrycollection": "BYTEA", "myint": "INT4", "myjson": "JSON", "mylinestring": "BYTEA", "mylongblob": "BYTEA", "mylongtext": "TEXT", "mymediumblob": "BYTEA", "mymediumint": "INT4", "mymediumtext": "TEXT", "mymultilinestring": "BYTEA", "mymultipoint": "BYTEA", "mymultipolygon": "BYTEA", "mynchar": "VARCHAR", "mynvarchar": "VARCHAR", "mypoint": "BYTEA", "mypolygon": "BYTEA", "myserial": "INT8", "myset": "VARCHAR", "mysmallint": "INT2", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytinyblob": "BYTEA", "mytinyint": "INT2", "mytinytext": "TEXT", "myvarbinary": "BYTEA", "myvarchar": "VARCHAR", "myyear": "INT4"}, Rows: []interface{}{"1", "'1'", "'1010'", "'1111111111111111111111111111111111111111111111111111111111111111'", "2", "5", "50", "4595435", "392809438543", "30.50000", "45.900001525878906", "54.3", "'2009-05-28 00:00:00.000000 +0000'", "'14:23:54'", "'2010-10-24 20:52:52.000000 +0000'", "'1989-02-22 03:17:21.000000 +0000'", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "'\\x626e72'", "'\\x6d792062696e61727920737472696e67207761686f6f6f6f6f'", "'\\x626c6f622063697479206262'", "'\\x626c6f622063697479206262'", "'\\x626c6f622063697479206262'", "'\\x626c6f622063697479206262'", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "'\\x000000000101000000000000000000f03f000000000000f03f'", "'\\x000000000101000000000000000000f03f000000000000f03f'", "'\\x0000000001020000000300000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000400000000000000040'", "'\\x0000000001030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000005000000000000000000144000000000000014400000000000001c4000000000000014400000000000001c400000000000001c4000000000000014400000000000001c4000000000000014400000000000001440'", "'\\x0000000001040000000a0000000101000000000000000000f03f000000000000f03f01010000000000000000000040000000000000004001010000000000000000001440000000000000084001010000000000000000001c4000000000000000400101000000000000000000224000000000000008400101000000000000000000204000000000000010400101000000000000000000184000000000000018400101000000000000000000184000000000000022400101000000000000000000104000000000000022400101000000000000000000f03f0000000000001440'", "'\\x00000000010500000002000000010200000003000000000000000000f03f000000000000f03f00000000000000400000000000000040000000000000084000000000000008400102000000020000000000000000001040000000000000104000000000000014400000000000001440'", "'\\x0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f'", "'\\x0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f'", "'{\"mykey\": \"this is\\\" m\\\"y, ''val''\"}'", "2", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!s(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "'%!s(<nil>)'", "null", "null", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'\\x%!x(<nil>)'", "'%!s(<nil>)'"}}
	mysql2mysql_wide_result      = QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybinary": "BLOB", "mybit": "BIT", "mybit5": "BIT", "mybit64": "BIT", "myblob": "BLOB", "mychar": "TEXT", "mydate": "DATE", "mydatetime": "DATETIME", "mydecimal": "DECIMAL", "mydouble": "DOUBLE", "myenum": "TEXT", "myfloat": "FLOAT", "mygeometry": "GEOMETRY", "mygeometrycollection": "GEOMETRY", "myint": "INT", "myjson": "JSON", "mylinestring": "GEOMETRY", "mylongblob": "BLOB", "mylongtext": "TEXT", "mymediumblob": "BLOB", "mymediumint": "MEDIUMINT", "mymediumtext": "TEXT", "mymultilinestring": "GEOMETRY", "mymultipoint": "GEOMETRY", "mymultipolygon": "GEOMETRY", "mynchar": "TEXT", "mynvarchar": "TEXT", "mypoint": "GEOMETRY", "mypolygon": "GEOMETRY", "myserial": "BIGINT", "myset": "TEXT", "mysmallint": "SMALLINT", "mytext": "TEXT", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytinyblob": "BLOB", "mytinyint": "TINYINT", "mytinytext": "TEXT", "myvarbinary": "BLOB", "myvarchar": "TEXT", "myyear": "YEAR"}, Rows: []interface{}{"1", "x'0000000000000001'", "x'000000000000000a'", "x'ffffffffffffffff'", "2", "5", "50", "4595435", "392809438543", "30.50000", "45.9", "54.3", "'2009-05-28'", "'14:23:54'", "'2010-10-24 20:52:52'", "'1989-02-22 03:17:21'", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "x'626e72'", "x'6d792062696e61727920737472696e67207761686f6f6f6f6f'", "x'626c6f622063697479206262'", "x'626c6f622063697479206262'", "x'626c6f622063697479206262'", "x'626c6f622063697479206262'", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "'\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'\x00\x00\x00\x00\x01\x02\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@'", "'\x00\x00\x00\x00\x01\x03\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@'", "'\x00\x00\x00\x00\x01\x04\x00\x00\x00\n\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\b@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00\x00@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\"@\x00\x00\x00\x00\x00\x00\b@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00 @\x00\x00\x00\x00\x00\x00\x10@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x18@\x00\x00\x00\x00\x00\x00\x18@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x18@\x00\x00\x00\x00\x00\x00\"@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\"@\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x14@'", "'\x00\x00\x00\x00\x01\x05\x00\x00\x00\x02\x00\x00\x00\x01\x02\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@'", "'\x00\x00\x00\x00\x01\x06\x00\x00\x00\x01\x00\x00\x00\x01\x03\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'\x00\x00\x00\x00\x01\x06\x00\x00\x00\x01\x00\x00\x00\x01\x03\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\b@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'", "'{\"mykey\": \"this is\\\\\" m\\\\\"y, ''val''\"}'", "2", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "x'%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}}
	mysql2mssql_wide_result      = QueryResult{ColumnTypes: map[string]string{"mybigint": "BIGINT", "mybinary": "VARBINARY", "mybit": "VARCHAR", "mybit5": "VARCHAR", "mybit64": "VARCHAR", "myblob": "VARBINARY", "mychar": "NVARCHAR", "mydate": "DATE", "mydatetime": "DATETIME2", "mydecimal": "DECIMAL", "mydouble": "FLOAT", "myenum": "NVARCHAR", "myfloat": "REAL", "mygeometry": "VARBINARY", "mygeometrycollection": "VARBINARY", "myint": "INT", "myjson": "NVARCHAR", "mylinestring": "VARBINARY", "mylongblob": "VARBINARY", "mylongtext": "NTEXT", "mymediumblob": "VARBINARY", "mymediumint": "INT", "mymediumtext": "NTEXT", "mymultilinestring": "VARBINARY", "mymultipoint": "VARBINARY", "mymultipolygon": "VARBINARY", "mynchar": "NVARCHAR", "mynvarchar": "NVARCHAR", "mypoint": "VARBINARY", "mypolygon": "VARBINARY", "myserial": "BIGINT", "myset": "NVARCHAR", "mysmallint": "SMALLINT", "mytext": "NTEXT", "mytime": "TIME", "mytimestamp": "DATETIME2", "mytinyblob": "VARBINARY", "mytinyint": "TINYINT", "mytinytext": "NTEXT", "myvarbinary": "VARBINARY", "myvarchar": "NVARCHAR", "myyear": "INT"}, Rows: []interface{}{"1", "'[1]'", "'[1010]'", "'[11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111]'", "2", "5", "50", "4595435", "392809438543", "30.50000", "45.900001525878906", "54.3", "CONVERT(DATETIME2, '2009-05-28 00:00:00.0000000', 121)", "CONVERT(TIME, '14:23:54.000', 121)", "CONVERT(DATETIME2, '2010-10-24 20:52:52.0000000', 121)", "CONVERT(DATETIME2, '1989-02-22 03:17:21.0000000', 121)", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "CONVERT(VARBINARY(8000), '0x626e72', 1)", "CONVERT(VARBINARY(8000), '0x6d792062696e61727920737472696e67207761686f6f6f6f6f', 1)", "CONVERT(VARBINARY(8000), '0x626c6f622063697479206262', 1)", "CONVERT(VARBINARY(8000), '0x626c6f622063697479206262', 1)", "CONVERT(VARBINARY(8000), '0x626c6f622063697479206262', 1)", "CONVERT(VARBINARY(8000), '0x626c6f622063697479206262', 1)", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "CONVERT(VARBINARY(8000), '0x000000000101000000000000000000f03f000000000000f03f', 1)", "CONVERT(VARBINARY(8000), '0x000000000101000000000000000000f03f000000000000f03f', 1)", "CONVERT(VARBINARY(8000), '0x0000000001020000000300000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000400000000000000040', 1)", "CONVERT(VARBINARY(8000), '0x0000000001030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000005000000000000000000144000000000000014400000000000001c4000000000000014400000000000001c400000000000001c4000000000000014400000000000001c4000000000000014400000000000001440', 1)", "CONVERT(VARBINARY(8000), '0x0000000001040000000a0000000101000000000000000000f03f000000000000f03f01010000000000000000000040000000000000004001010000000000000000001440000000000000084001010000000000000000001c4000000000000000400101000000000000000000224000000000000008400101000000000000000000204000000000000010400101000000000000000000184000000000000018400101000000000000000000184000000000000022400101000000000000000000104000000000000022400101000000000000000000f03f0000000000001440', 1)", "CONVERT(VARBINARY(8000), '0x00000000010500000002000000010200000003000000000000000000f03f000000000000f03f00000000000000400000000000000040000000000000084000000000000008400102000000020000000000000000001040000000000000104000000000000014400000000000001440', 1)", "CONVERT(VARBINARY(8000), '0x0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f', 1)", "CONVERT(VARBINARY(8000), '0x0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f', 1)", "'{\"mykey\": \"this is\\\" m\\\"y, ''val''\"}'", "2", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!s(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "null", "null", "null", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)", "'%!s(<nil>)'"}}
	mysql2oracle_wide_result     = QueryResult{ColumnTypes: map[string]string{"MYBIGINT": "NUMBER", "MYBINARY": "OCIBlobLocator", "MYBIT": "OCIBlobLocator", "MYBIT5": "OCIBlobLocator", "MYBIT64": "OCIBlobLocator", "MYBLOB": "OCIBlobLocator", "MYCHAR": "NCHAR", "MYDATE": "DATE", "MYDATETIME": "TimeStampDTY", "MYDECIMAL": "NUMBER", "MYDOUBLE": "IBDouble", "MYENUM": "NCHAR", "MYFLOAT": "IBFloat", "MYGEOMETRY": "OCIBlobLocator", "MYGEOMETRYCOLLECTION": "OCIBlobLocator", "MYINT": "NUMBER", "MYJSON": "NCHAR", "MYLINESTRING": "OCIBlobLocator", "MYLONGBLOB": "OCIBlobLocator", "MYLONGTEXT": "NCHAR", "MYMEDIUMBLOB": "OCIBlobLocator", "MYMEDIUMINT": "NUMBER", "MYMEDIUMTEXT": "NCHAR", "MYMULTILINESTRING": "OCIBlobLocator", "MYMULTIPOINT": "OCIBlobLocator", "MYMULTIPOLYGON": "OCIBlobLocator", "MYNCHAR": "NCHAR", "MYNVARCHAR": "NCHAR", "MYPOINT": "OCIBlobLocator", "MYPOLYGON": "OCIBlobLocator", "MYSERIAL": "NUMBER", "MYSET": "NCHAR", "MYSMALLINT": "NUMBER", "MYTEXT": "NCHAR", "MYTIME": "NCHAR", "MYTIMESTAMP": "TimeStampDTY", "MYTINYBLOB": "OCIBlobLocator", "MYTINYINT": "NUMBER", "MYTINYTEXT": "NCHAR", "MYVARBINARY": "OCIBlobLocator", "MYVARCHAR": "NCHAR", "MYYEAR": "NUMBER"}, Rows: []interface{}{"1", "hextoraw('01')", "hextoraw('0a')", "hextoraw('ffffffffffffffff')", "2", "5", "50", "4595435", "392809438543", "30.5", "45.9", "54.3", "TO_DATE('2009-05-28', 'YYYY-MM-DD')", "'14:23:54'", "TO_TIMESTAMP('2010-10-24 20:52:52.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "TO_TIMESTAMP('1989-02-22 03:17:21.000000', 'YYYY-MM-DD HH24:MI:SS.FF')", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "hextoraw('626e72')", "hextoraw('6d792062696e61727920737472696e67207761686f6f6f6f6f')", "hextoraw('626c6f622063697479206262')", "hextoraw('626c6f622063697479206262')", "hextoraw('626c6f622063697479206262')", "hextoraw('626c6f622063697479206262')", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "hextoraw('000000000101000000000000000000f03f000000000000f03f')", "hextoraw('000000000101000000000000000000f03f000000000000f03f')", "hextoraw('0000000001020000000300000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000400000000000000040')", "hextoraw('0000000001030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000005000000000000000000144000000000000014400000000000001c4000000000000014400000000000001c400000000000001c4000000000000014400000000000001c4000000000000014400000000000001440')", "hextoraw('0000000001040000000a0000000101000000000000000000f03f000000000000f03f01010000000000000000000040000000000000004001010000000000000000001440000000000000084001010000000000000000001c4000000000000000400101000000000000000000224000000000000008400101000000000000000000204000000000000010400101000000000000000000184000000000000018400101000000000000000000184000000000000022400101000000000000000000104000000000000022400101000000000000000000f03f0000000000001440')", "hextoraw('00000000010500000002000000010200000003000000000000000000f03f000000000000f03f00000000000000400000000000000040000000000000084000000000000008400102000000020000000000000000001040000000000000104000000000000014400000000000001440')", "hextoraw('0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f')", "hextoraw('0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f')", "'{\"mykey\": \"this is\\\" m\\\"y, ''val''\"}'", "2", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "null", "'%!s(<nil>)'", "null", "null", "<nil>", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "hextoraw('%!x(<nil>)')", "'%!s(<nil>)'"}}
	mysql2redshift_wide_result   = QueryResult{ColumnTypes: map[string]string{"mybigint": "INT8", "mybinary": "VARCHAR", "mybit": "VARCHAR", "mybit5": "VARCHAR", "mybit64": "VARCHAR", "myblob": "VARCHAR", "mychar": "VARCHAR", "mydate": "DATE", "mydatetime": "TIMESTAMP", "mydecimal": "NUMERIC", "mydouble": "FLOAT8", "myenum": "VARCHAR", "myfloat": "FLOAT4", "mygeometry": "VARCHAR", "mygeometrycollection": "VARCHAR", "myint": "INT4", "myjson": "VARCHAR", "mylinestring": "VARCHAR", "mylongblob": "VARCHAR", "mylongtext": "VARCHAR", "mymediumblob": "VARCHAR", "mymediumint": "INT4", "mymediumtext": "VARCHAR", "mymultilinestring": "VARCHAR", "mymultipoint": "VARCHAR", "mymultipolygon": "VARCHAR", "mynchar": "VARCHAR", "mynvarchar": "VARCHAR", "mypoint": "VARCHAR", "mypolygon": "VARCHAR", "myserial": "INT8", "myset": "VARCHAR", "mysmallint": "INT2", "mytext": "VARCHAR", "mytime": "TIME", "mytimestamp": "TIMESTAMP", "mytinyblob": "VARCHAR", "mytinyint": "INT2", "mytinytext": "VARCHAR", "myvarbinary": "VARCHAR", "myvarchar": "VARCHAR", "myyear": "INT4"}, Rows: []interface{}{"1", "'1'", "'1010'", "'1111111111111111111111111111111111111111111111111111111111111111'", "2", "5", "50", "4595435", "392809438543", "%!g(string=30.50000)", "45.900001525878906", "54.3", "'2009-05-28 00:00:00.000000 +0000'", "'14:23:54'", "'2010-10-24 20:52:52.000000 +0000'", "'1989-02-22 03:17:21.000000 +0000'", "1905", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "'bnr'", "'my binary string wahooooo'", "'626c6f622063697479206262'", "'626c6f622063697479206262'", "'626c6f622063697479206262'", "'626c6f622063697479206262'", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "'000000000101000000000000000000f03f000000000000f03f'", "'000000000101000000000000000000f03f000000000000f03f'", "'0000000001020000000300000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000400000000000000040'", "'0000000001030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000005000000000000000000144000000000000014400000000000001c4000000000000014400000000000001c400000000000001c4000000000000014400000000000001c4000000000000014400000000000001440'", "'0000000001040000000a0000000101000000000000000000f03f000000000000f03f01010000000000000000000040000000000000004001010000000000000000001440000000000000084001010000000000000000001c4000000000000000400101000000000000000000224000000000000008400101000000000000000000204000000000000010400101000000000000000000184000000000000018400101000000000000000000184000000000000022400101000000000000000000104000000000000022400101000000000000000000f03f0000000000001440'", "'00000000010500000002000000010200000003000000000000000000f03f000000000000f03f00000000000000400000000000000040000000000000084000000000000008400102000000020000000000000000001040000000000000104000000000000014400000000000001440'", "'0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f'", "'0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f'", "'{\"mykey\": \"this is\" m\"y, ''val''\"}'", "2", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!d(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "'%!s(<nil>)'", "null", "null", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"}}
	mysql2snowflake_wide_result  = QueryResult{ColumnTypes: map[string]string{"MYBIGINT": "FIXED", "MYBINARY": "BINARY", "MYBIT": "BINARY", "MYBIT5": "BINARY", "MYBIT64": "BINARY", "MYBLOB": "BINARY", "MYCHAR": "TEXT", "MYDATE": "DATE", "MYDATETIME": "TIMESTAMP_NTZ", "MYDECIMAL": "FIXED", "MYDOUBLE": "REAL", "MYENUM": "TEXT", "MYFLOAT": "REAL", "MYGEOMETRY": "BINARY", "MYGEOMETRYCOLLECTION": "BINARY", "MYINT": "FIXED", "MYJSON": "VARIANT", "MYLINESTRING": "BINARY", "MYLONGBLOB": "BINARY", "MYLONGTEXT": "TEXT", "MYMEDIUMBLOB": "BINARY", "MYMEDIUMINT": "FIXED", "MYMEDIUMTEXT": "TEXT", "MYMULTILINESTRING": "BINARY", "MYMULTIPOINT": "BINARY", "MYMULTIPOLYGON": "BINARY", "MYNCHAR": "TEXT", "MYNVARCHAR": "TEXT", "MYPOINT": "BINARY", "MYPOLYGON": "BINARY", "MYSERIAL": "FIXED", "MYSET": "TEXT", "MYSMALLINT": "FIXED", "MYTEXT": "TEXT", "MYTIME": "TIME", "MYTIMESTAMP": "TIMESTAMP_NTZ", "MYTINYBLOB": "BINARY", "MYTINYINT": "FIXED", "MYTINYTEXT": "TEXT", "MYVARBINARY": "BINARY", "MYVARCHAR": "TEXT", "MYYEAR": "FIXED"}, Rows: []interface{}{"%!g(string=1)", "to_binary('01')", "to_binary('0a')", "to_binary('ffffffffffffffff')", "%!g(string=2)", "%!g(string=5)", "%!g(string=50)", "%!g(string=4595435)", "%!g(string=392809438543)", "%!g(string=30.500000)", "%!g(string=45.900000)", "%!g(string=54.300000)", "'2009-05-28 00:00:00.000000'", "'0001-01-01 14:23:54.000000'", "'2010-10-24 20:52:52.000000'", "'1989-02-22 03:17:21.000000'", "%!g(string=1905)", "'chr'", "'my varchar ''st\"ri,ng wheeeee'", "'ncr'", "'my nvarchar string wheeeee'", "to_binary('626e72')", "to_binary('6d792062696e61727920737472696e67207761686f6f6f6f6f')", "to_binary('626c6f622063697479206262')", "to_binary('626c6f622063697479206262')", "to_binary('626c6f622063697479206262')", "to_binary('626c6f622063697479206262')", "'text city bb'", "'text city bb'", "'text city bb'", "'text city bb'", "'enumval1'", "'setval1'", "to_binary('000000000101000000000000000000f03f000000000000f03f')", "to_binary('000000000101000000000000000000f03f000000000000f03f')", "to_binary('0000000001020000000300000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000400000000000000040')", "to_binary('0000000001030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000005000000000000000000144000000000000014400000000000001c4000000000000014400000000000001c400000000000001c4000000000000014400000000000001c4000000000000014400000000000001440')", "to_binary('0000000001040000000a0000000101000000000000000000f03f000000000000f03f01010000000000000000000040000000000000004001010000000000000000001440000000000000084001010000000000000000001c4000000000000000400101000000000000000000224000000000000008400101000000000000000000204000000000000010400101000000000000000000184000000000000018400101000000000000000000184000000000000022400101000000000000000000104000000000000022400101000000000000000000f03f0000000000001440')", "to_binary('00000000010500000002000000010200000003000000000000000000f03f000000000000f03f00000000000000400000000000000040000000000000084000000000000008400102000000020000000000000000001040000000000000104000000000000014400000000000001440')", "to_binary('0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f')", "to_binary('0000000001060000000100000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000084000000000000008400000000000000840000000000000084000000000000000000000000000000000000000000000000005000000000000000000f03f000000000000f03f000000000000f03f0000000000000040000000000000004000000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f')", "'{\n  \"mykey\": \"this is\\\\\" m\\\\\"y, ''val''\"\n}'", "%!g(string=2)", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "%!g(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "%!g(<nil>)", "null", "null", "null", "null", "%!g(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "to_binary('%!x(<nil>)')", "'%!s(<nil>)'"}}
)
