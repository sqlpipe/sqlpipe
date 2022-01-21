package engine

// postgreSQLWideTableDrop
var (
	postgreSQLWideTableDropErrProperties = map[string]string{
		"dsType": "postgresql",
		"error":  `ERROR: relation "wide_table" does not exist (SQLSTATE 42P01)`,
		"query":  "select * from wide_table",
	}
)

// postgresqlWideTableCreate
var (
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
)

// postgresqlWideTableInsert
var (
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
	postgresqlWideTableInsertResult = QueryResult{
		ColumnTypes: map[string]string{
			"mybigint":          "INT8",
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
		Rows: []interface{}{"6514798382812790784", "'10001'", "'1001'", "true", "'(8,9),(1,3)'", "'\\xaaaabbbb'", "'abc'", "'\"my\"varch''ar,123@gmail.com'", "'192.168.100.128/25'", "'<(1,5),5>'", "'2014-01-10'", "529.5621898337544", "'192.168.100.128'", "745910651", "'10 days 10:00:00'", "'{\"mykey\": \"this\\\"  ''is'' m,y val\"}'", "'{\"mykey\": \"this is my val\"}'", "'{1,5,20}'", "'[(5,4),(2,1)]'", "'08:00:2b:01:02:03'", "'$35,244.33'", "449.82115", "'[(1,4),(8,7)]'", "'16/B374D848'", "'(5,7)'", "'((5,8),(6,10),(7,20))'", "9673.109375", "24345", "'myte\",xt123@gmail.com'", "'03:46:38.765594'", "'03:46:38.765594+05'", "'2014-01-10 10:05:04.000000 +0000'", "'2014-01-10 18:05:04.000000 +0000'", "'''fat'' & ''rat'''", "'''a'' ''and'' ''ate'' ''cat'' ''fat'' ''mat'' ''on'' ''rat'' ''sat'''", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'<foo>bar</foo>'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!t(<nil>)", "'%!s(<nil>)'", "'\\x%!x(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "'%!s(<nil>)'", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!s(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "%!g(<nil>)", "%!d(<nil>)", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "null", "null", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'", "'%!s(<nil>)'"},
	}
)
