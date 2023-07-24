drop table if exists my_table;

CREATE TABLE my_table (
    my_char CHAR(10),
    my_varchar VARCHAR(255),
    my_longtext LONGTEXT,
    my_mediumtext MEDIUMTEXT,
    my_text TEXT,
    my_tinytext TINYTEXT,
    my_enum ENUM('value1', 'value2'),
    my_unsigned_bigint BIGINT UNSIGNED,
    my_bigint BIGINT,
    my_unsigned_int INT UNSIGNED,
    my_int INT,
    my_unsigned_mediumint MEDIUMINT UNSIGNED,
    my_mediumint MEDIUMINT,
    my_unsigned_smallint SMALLINT UNSIGNED,
    my_smallint SMALLINT,
    my_unsigned_tinyint TINYINT UNSIGNED,
    my_tinyint TINYINT,
    my_double DOUBLE,
    my_float FLOAT,
    my_decimal DECIMAL(18, 5),
    my_datetime DATETIME,
    my_timestamp TIMESTAMP,
    my_date DATE,
    my_time TIME,
    my_year YEAR,
    my_binary BINARY(50),
    my_varbinary VARBINARY(255),
    my_longblob LONGBLOB,
    my_mediumblob MEDIUMBLOB,
    my_blob BLOB,
    my_tinyblob TINYBLOB,
    my_geometry GEOMETRY,
    my_bit BIT(1),
    my_json JSON,
    my_set SET('value1', 'value2')
);

-- Insert a row with data
INSERT INTO my_table (
    my_char, my_varchar, my_longtext, my_mediumtext, my_text, my_tinytext,
    my_enum, my_unsigned_bigint, my_bigint, my_unsigned_int, my_int,
    my_unsigned_mediumint, my_mediumint, my_unsigned_smallint, my_smallint,
    my_unsigned_tinyint, my_tinyint, my_double, my_float, my_decimal,
    my_datetime, my_timestamp, my_date, my_time, my_year, my_binary,
    my_varbinary, my_longblob, my_mediumblob, my_blob, my_tinyblob, my_geometry,
    my_bit, my_json, my_set
)
VALUES (
    'abc', 'abc', 'This is a test', 'This is a test', 'This is a test', 'Test',
    'value1', 123456789012345678, 123456789012345678, 12345678, 12345678,
    1234567, 1234567, 12345, 12345, 123, 123, 1.23456789012345678,
    1.2345678, 123.456, '2023-07-22 12:34:56', '2023-07-22 12:34:56',
    '2023-07-22', '12:34:56', 2023, 0b110011, 0b110011, 
    0x89504E470D0A1A0A, 0x89504E470D0A1A0A, 0x89504E470D0A1A0A, 0x89504E470D0A1A0A, 
    ST_GeomFromText('POINT(1 1)'), 1, '{"key": "value"}', 'value1'
);


-- Insert a row with all NULLs
INSERT INTO my_table (
    my_char, my_varchar, my_longtext, my_mediumtext, my_text, my_tinytext,
    my_enum, my_unsigned_bigint, my_bigint, my_unsigned_int, my_int,
    my_unsigned_mediumint, my_mediumint, my_unsigned_smallint, my_smallint,
    my_unsigned_tinyint, my_tinyint, my_double, my_float, my_decimal,
    my_datetime, my_timestamp, my_date, my_time, my_year, my_binary,
    my_varbinary, my_longblob, my_mediumblob, my_blob, my_tinyblob, my_geometry,
    my_bit, my_json, my_set
)
VALUES (
    NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL
);

{name:my_char databaseType:CHAR scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_varchar databaseType:VARCHAR scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_longtext databaseType:TEXT scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_mediumtext databaseType:TEXT scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_text databaseType:TEXT scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_tinytext databaseType:TEXT scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_enum databaseType:CHAR scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_unsigned_bigint databaseType:UNSIGNED BIGINT scanType:sql.NullInt64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_bigint databaseType:BIGINT scanType:sql.NullInt64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_unsigned_int databaseType:UNSIGNED INT scanType:sql.NullInt64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_int databaseType:INT scanType:sql.NullInt64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_unsigned_mediumint databaseType:MEDIUMINT scanType:sql.NullInt64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_mediumint databaseType:MEDIUMINT scanType:sql.NullInt64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_unsigned_smallint databaseType:UNSIGNED SMALLINT scanType:sql.NullInt64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_smallint databaseType:SMALLINT scanType:sql.NullInt64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_unsigned_tinyint databaseType:UNSIGNED TINYINT scanType:sql.NullInt64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_tinyint databaseType:TINYINT scanType:sql.NullInt64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_double databaseType:DOUBLE scanType:sql.NullFloat64 decimalOk:true precision:9223372036854775807 scale:9223372036854775807 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_float databaseType:FLOAT scanType:sql.NullFloat64 decimalOk:true precision:9223372036854775807 scale:9223372036854775807 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_decimal databaseType:DECIMAL scanType:sql.RawBytes decimalOk:true precision:18 scale:5 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_datetime databaseType:DATETIME scanType:sql.NullTime decimalOk:true precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_timestamp databaseType:TIMESTAMP scanType:sql.NullTime decimalOk:true precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_date databaseType:DATE scanType:sql.NullTime decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_time databaseType:TIME scanType:sql.RawBytes decimalOk:true precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_year databaseType:YEAR scanType:sql.NullInt64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_binary databaseType:BINARY scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_varbinary databaseType:VARBINARY scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_longblob databaseType:BLOB scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_mediumblob databaseType:BLOB scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_blob databaseType:BLOB scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_tinyblob databaseType:BLOB scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_geometry databaseType:GEOMETRY scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_bit databaseType:BIT scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_json databaseType:JSON scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_set databaseType:CHAR scanType:sql.RawBytes decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}