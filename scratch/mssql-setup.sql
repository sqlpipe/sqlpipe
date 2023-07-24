drop table if exists my_table;

CREATE TABLE my_table (
    my_nchar NCHAR(10),
    my_char CHAR(10),
    my_nvarchar_max NVARCHAR(MAX),
    my_nvarchar NVARCHAR(50),
    my_varchar_max VARCHAR(MAX),
    my_varchar VARCHAR(50),
    my_ntext NTEXT,
    my_text TEXT,
    my_bigint BIGINT,
    my_int INT,
    my_smallint SMALLINT,
    my_tinyint TINYINT,
    my_float FLOAT,
    my_real REAL,
    my_decimal DECIMAL(18, 0),
    my_money MONEY,
    my_smallmoney SMALLMONEY,
    my_datetime2 DATETIME2,
    my_datetime DATETIME,
    my_smalldatetime SMALLDATETIME,
    my_datetimeoffset DATETIMEOFFSET,
    my_date DATE,
    my_time TIME,
    my_binary BINARY(50),
    my_varbinary VARBINARY(MAX),
    my_image IMAGE,
    my_bit BIT,
    my_uniqueidentifier UNIQUEIDENTIFIER,
    my_xml XML,
    my_hierarchyid HIERARCHYID,
    my_sql_variant SQL_VARIANT,
    my_rowversion ROWVERSION,
    my_geometry GEOMETRY,
    my_geography GEOGRAPHY
);



-- First row with data
INSERT INTO my_table (
    my_nchar, my_char, my_nvarchar_max, my_nvarchar, my_varchar_max, 
    my_varchar, my_ntext, my_text, my_bigint, my_int, my_smallint, 
    my_tinyint, my_float, my_real, my_decimal, my_money, my_smallmoney, 
    my_datetime2, my_datetime, my_smalldatetime, my_datetimeoffset, 
    my_date, my_time, my_binary, my_varbinary, my_image, my_bit, 
    my_uniqueidentifier, my_xml, my_hierarchyid, my_sql_variant, 
    my_geometry, my_geography) 
VALUES (
    N'ABCD', 
    'ABCD', 
    N'This is a test message', 
    N'Test message',
    'This is a test message', 
    'Test message',  
    N'Test ntext', 
    'Test text', 
    123456789, 
    12345, 
    123, 
    12, 
    123.45, 
    123.45, 
    123, 
    123.45, 
    123.45, 
    '2023-07-23T14:30:00', 
    '2023-07-23T14:30:00', 
    '2023-07-23T14:30:00', 
    '2023-07-23T14:30:00+00:00', 
    '2023-07-23', 
    '14:30:00', 
    0x010101, 
    0x010101, 
    0x010101, 
    1, 
    NEWID(), 
    '<root><test>Some XML data</test></root>', 
    hierarchyid::GetRoot(), 
    123, 
    geometry::STGeomFromText('POINT (100 100)', 4326), 
    geography::STGeomFromText('POINT (-50 50)', 4326) -- corrected
);

-- Second row with all NULLs
INSERT INTO my_table (
    my_nchar, my_char, my_nvarchar_max, my_nvarchar, my_varchar_max, 
    my_varchar, my_ntext, my_text, my_bigint, my_int, my_smallint, 
    my_tinyint, my_float, my_real, my_decimal, my_money, my_smallmoney, 
    my_datetime2, my_datetime, my_smalldatetime, my_datetimeoffset, 
    my_date, my_time, my_binary, my_varbinary, my_image, my_bit, 
    my_uniqueidentifier, my_xml, my_hierarchyid, my_sql_variant, 
    my_geometry, my_geography) 
VALUES (
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, NULL
);

{name:my_nchar databaseType:NCHAR scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:10 nullableOk:true nullable:true}
{name:my_char databaseType:CHAR scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:10 nullableOk:true nullable:true}
{name:my_nvarchar_max databaseType:NVARCHAR scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:1073741822 nullableOk:true nullable:true}
{name:my_nvarchar databaseType:NVARCHAR scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:50 nullableOk:true nullable:true}
{name:my_varchar_max databaseType:VARCHAR scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:2147483645 nullableOk:true nullable:true}
{name:my_varchar databaseType:VARCHAR scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:50 nullableOk:true nullable:true}
{name:my_ntext databaseType:NTEXT scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:1073741823 nullableOk:true nullable:true}
{name:my_text databaseType:TEXT scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:2147483647 nullableOk:true nullable:true}
{name:my_bigint databaseType:BIGINT scanType:int64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_int databaseType:INT scanType:int64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_smallint databaseType:SMALLINT scanType:int64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_tinyint databaseType:TINYINT scanType:int64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_float databaseType:FLOAT scanType:float64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_real databaseType:REAL scanType:float64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_decimal databaseType:DECIMAL scanType:[]uint8 decimalOk:true precision:18 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_money databaseType:MONEY scanType:[]uint8 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_smallmoney databaseType:SMALLMONEY scanType:[]uint8 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_datetime2 databaseType:DATETIME2 scanType:time.Time decimalOk:true precision:0 scale:7 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_datetime databaseType:DATETIME scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_smalldatetime databaseType:SMALLDATETIME scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_datetimeoffset databaseType:DATETIMEOFFSET scanType:time.Time decimalOk:true precision:0 scale:7 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_date databaseType:DATE scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_time databaseType:TIME scanType:time.Time decimalOk:true precision:0 scale:7 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_binary databaseType:BINARY scanType:[]uint8 decimalOk:false precision:0 scale:0 lengthOk:true length:50 nullableOk:true nullable:true}
{name:my_varbinary databaseType:VARBINARY scanType:[]uint8 decimalOk:false precision:0 scale:0 lengthOk:true length:2147483645 nullableOk:true nullable:true}
{name:my_image databaseType:IMAGE scanType:[]uint8 decimalOk:false precision:0 scale:0 lengthOk:true length:2147483647 nullableOk:true nullable:true}
{name:my_bit databaseType:BIT scanType:bool decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_uniqueidentifier databaseType:UNIQUEIDENTIFIER scanType:[]uint8 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:my_xml databaseType:XML scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:1073741822 nullableOk:true nullable:true}