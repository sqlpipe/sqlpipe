DROP TABLE IF EXISTS my_table;

CREATE TABLE my_table(
    my_number number(10, 5),
    my_int int,
    my_float float,
    my_varchar varchar,
    my_binary BINARY,
    my_boolean boolean,
    my_date date,
    my_time time,
    my_timestamp_ltz timestamp_ltz,
    my_timestamp_ntz timestamp_ntz,
    my_timestamp_tz timestamptz,
    my_variant variant,
    my_object object,
    my_array ARRAY,
    my_geography geography,
    my_geometry geometry
);

INSERT INTO my_table(my_number, my_int, my_float, my_varchar, my_binary, my_boolean, my_date, my_time, my_timestamp_ltz, my_timestamp_ntz, my_timestamp_tz, my_variant, my_object, my_array, my_geography, my_geometry)
SELECT
    column1,
    column2,
    column3,
    column4,
    column5,
    column6,
    column7,
    column8,
    column9,
    column10,
    column11,
    parse_json(column12),
    parse_json(column13),
    parse_json(column14),
    column15,
    column16
FROM
VALUES (25.5,
    22,
    42.5,
    'hellooooo h''er"es ,my varchar value',
    to_binary('0011'),
    TRUE,
    '2000-10-15',
    '23:54:01',
    '2000-10-15 23:54:01.345673',
    '2000-10-15 23:54:01.345673',
    '2000-10-15 23:54:01.345673 +0100',
    '{"mykey": "this is \\"my'' v,al"}',
    '{"key3": "value3", "key4": "value4"}',
    '[true, 1, -1.2e-3, "Abc", ["x","y"], {"a":1}]',
    'POINT(-122.35 37.55)',
    'POINT(-122.35 37.55)'),
(NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL);

{name:MY_NUMBER databaseType:FIXED scanType:float64 decimalOk:true precision:10 scale:5 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_INT databaseType:FIXED scanType:int64 decimalOk:true precision:38 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_FLOAT databaseType:REAL scanType:float64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_VARCHAR databaseType:TEXT scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:16777216 nullableOk:true nullable:true}
{name:MY_BINARY databaseType:BINARY scanType:[]uint8 decimalOk:false precision:0 scale:0 lengthOk:true length:8388608 nullableOk:true nullable:true}
{name:MY_BOOLEAN databaseType:BOOLEAN scanType:bool decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_DATE databaseType:DATE scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_TIME databaseType:TIME scanType:time.Time decimalOk:true precision:9 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_TIMESTAMP_LTZ databaseType:TIMESTAMP_LTZ scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_TIMESTAMP_NTZ databaseType:TIMESTAMP_NTZ scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_TIMESTAMP_TZ databaseType:TIMESTAMP_TZ scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_VARIANT databaseType:VARIANT scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:0 nullableOk:true nullable:true}
{name:MY_OBJECT databaseType:OBJECT scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:0 nullableOk:true nullable:true}
{name:MY_ARRAY databaseType:ARRAY scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:0 nullableOk:true nullable:true}
{name:MY_GEOGRAPHY databaseType:OBJECT scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:0 nullableOk:true nullable:true}
{name:MY_GEOMETRY databaseType:OBJECT scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:0 nullableOk:true nullable:true}