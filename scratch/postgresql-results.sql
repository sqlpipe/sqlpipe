drop table if exists my_table;

CREATE TABLE my_table (
    my_char CHAR,
    my_varchar VARCHAR,
    my_text TEXT,
    my_int8 INT8,
    my_int4 INT4,
    my_int2 INT2,
    my_float8 FLOAT8,
    my_float4 FLOAT4,
    my_numeric NUMERIC,
    my_timestamp TIMESTAMP,
    my_timestamptz TIMESTAMPTZ,
    my_date DATE,
    my_time TIME,
    my_interval INTERVAL,
    my_bytea BYTEA,
    my_box BOX,
    my_circle CIRCLE,
    my_line LINE,
    my_path PATH,
    my_point POINT,
    my_polygon POLYGON,
    my_lseg LSEG,
    my_bool BOOL,
    my_bit BIT,
    my_varbit VARBIT,
    my_uuid UUID,
    my_json JSON,
    my_jsonb JSONB,
    my_inet INET,
    my_macaddr MACADDR,
    my_macaddr8 MACADDR8,
    my_cidr CIDR,
    my_xml xml,
    my_timetz TIMETZ,
    my_pg_lsn pg_lsn,
    my_pg_snapshot pg_snapshot,
    my_tsquery tsquery,
    my_tsvector tsvector,
    my_txid_snapshot txid_snapshot,
    my_smallserial SMALLSERIAL,
    my_serial SERIAL,
    my_bigserial BIGSERIAL
);

INSERT INTO my_table (
    my_char,
    my_varchar,
    my_text,
    my_int8,
    my_int4,
    my_int2,
    my_float8,
    my_float4,
    my_numeric,
    my_timestamp,
    my_timestamptz,
    my_date,
    my_time,
    my_interval,
    my_bytea,
    my_box,
    my_circle,
    my_line,
    my_path,
    my_point,
    my_polygon,
    my_lseg,
    my_bool,
    my_bit,
    my_varbit,
    my_uuid,
    my_json,
    my_jsonb,
    my_inet,
    my_macaddr,
    my_cidr,
    my_xml,
    my_timetz,
    my_tsquery,
    my_tsvector,
    my_pg_lsn
)
VALUES (
    'A',
    '"Hello" there',
    'This ''is a text',
    1234567890123456,
    12345678,
    1234,
    12345678.12345678,
    1234.1234,
    123.456,
    '2023-07-23 12:34:56',
    '2023-07-23 12:34:56+00',
    '2023-07-23',
    '12:34:56',
    '1 day',
    E'\\xDEADBEEF',
    '((1,2),(3,4))',
    '<(1,2),3>',
    '{1,-2,3}',
    '((1,2),(3,4))',
    '(1,2)',
    '((1,2),(3,4))',
    '((1,2),(3,4))',
    true,
    B'1',
    B'101',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    '{"key":"value"}',
    '{"key":"value"}',
    '192.168.1.1',
    '08:00:2B:01:02:03',
    '192.168.1.0/24',
    '<root><test>Some Content</test></root>',
    '12:34:56+00',
    'super & rat',
    'super',
    '1/3B9ACA00'
);

INSERT INTO my_table DEFAULT VALUES;


{name:my_char databaseType:BPCHAR scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_varchar databaseType:VARCHAR scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:-5 nullableOk:false nullable:false}
{name:my_text databaseType:TEXT scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:9223372036854775807 nullableOk:false nullable:false}
{name:my_int8 databaseType:INT8 scanType:int64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_int4 databaseType:INT4 scanType:int32 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_int2 databaseType:INT2 scanType:int16 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_float8 databaseType:FLOAT8 scanType:float64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_float4 databaseType:FLOAT4 scanType:float32 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_numeric databaseType:NUMERIC scanType:float64 decimalOk:true precision:65535 scale:65531 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_timestamp databaseType:TIMESTAMP scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_timestamptz databaseType:TIMESTAMPTZ scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_date databaseType:DATE scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_time databaseType:TIME scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_interval databaseType:INTERVAL scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_bytea databaseType:BYTEA scanType:[]uint8 decimalOk:false precision:0 scale:0 lengthOk:true length:9223372036854775807 nullableOk:false nullable:false}
{name:my_box databaseType:BOX scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_circle databaseType:CIRCLE scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_line databaseType:LINE scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_path databaseType:PATH scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_point databaseType:POINT scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_polygon databaseType:POLYGON scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_lseg databaseType:LSEG scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_bool databaseType:BOOL scanType:bool decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_bit databaseType:BIT scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_varbit databaseType:VARBIT scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_uuid databaseType:UUID scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_json databaseType:JSON scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_jsonb databaseType:JSONB scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_inet databaseType:INET scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_macaddr databaseType:MACADDR scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_macaddr8 databaseType:774 scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_cidr databaseType:CIDR scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_xml databaseType:142 scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_timetz databaseType:1266 scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_pg_lsn databaseType:3220 scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_pg_snapshot databaseType:5038 scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_tsquery databaseType:3615 scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_tsvector databaseType:3614 scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_txid_snapshot databaseType:2970 scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_smallserial databaseType:INT2 scanType:int16 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_serial databaseType:INT4 scanType:int32 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}
{name:my_bigserial databaseType:INT8 scanType:int64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:false nullable:false}