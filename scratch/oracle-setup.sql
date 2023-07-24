DROP TABLE my_table

CREATE TABLE sys.my_table (
    my_nchar NCHAR,
    my_number NUMBER,
    my_float FLOAT,
    my_long LONG,
    my_varchar2 VARCHAR2(2000),
    my_rowid ROWID,
    my_date DATE,
    my_binaryfloat BINARY_FLOAT,
    my_binarydouble BINARY_DOUBLE,
    my_raw RAW(2000),
    my_char CHAR(1),
    my_timestamp TIMESTAMP,
    my_timestamptz TIMESTAMP WITH TIME ZONE,
    my_intervalym INTERVAL YEAR TO MONTH,
    my_intervalds INTERVAL DAY TO SECOND,
    my_urowid UROWID,
    my_timestampltz TIMESTAMP WITH LOCAL TIME ZONE,
    my_clob CLOB,
    my_blob BLOB,
    my_nclob NCLOB,
    my_bfile BFILE,
    my_varchar VARCHAR(2000)
);

-- Inserting a row with data
INSERT INTO sys.my_table (
    my_nchar, 
    my_number, 
    my_float, 
    my_long, 
    my_varchar2, 
    my_rowid, 
    my_date, 
    my_binaryfloat, 
    my_binarydouble, 
    my_raw, 
    my_char, 
    my_timestamp, 
    my_timestamptz, 
    my_intervalym, 
    my_intervalds, 
    my_urowid, 
    my_timestampltz, 
    my_clob, 
    my_blob, 
    my_nclob, 
    my_bfile, 
    my_varchar
) VALUES (
    N'A', 
    123, 
    123.45, 
    'This is a long column', 
    'VARCHAR2 column', 
    NULL, 
    SYSDATE, 
    123.45, 
    123.45, 
    hextoraw('53756D6D6572'), 
    'C', 
    CURRENT_TIMESTAMP, 
    CURRENT_TIMESTAMP, 
    INTERVAL '5' YEAR, 
    INTERVAL '10' DAY, 
    NULL, 
    CURRENT_TIMESTAMP, 
    'This is a CLOB', 
    EMPTY_BLOB(), 
    N'This is a NCLOB', 
    NULL, 
    'VARCHAR column'
);

-- Inserting a row with all NULLs
INSERT INTO sys.my_table (
    my_nchar, 
    my_number, 
    my_float, 
    my_long, 
    my_varchar2, 
    my_rowid, 
    my_date, 
    my_binaryfloat, 
    my_binarydouble, 
    my_raw, 
    my_char, 
    my_timestamp, 
    my_timestamptz, 
    my_intervalym, 
    my_intervalds, 
    my_urowid, 
    my_timestampltz, 
    my_clob, 
    my_blob, 
    my_nclob, 
    my_bfile, 
    my_varchar
) VALUES (
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
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL, 
    NULL
);

{name:MY_NCHAR databaseType:CHAR scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:1 nullableOk:true nullable:true}
{name:MY_NUMBER databaseType:NUMBER scanType:float64 decimalOk:true precision:38 scale:255 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_FLOAT databaseType:NUMBER scanType:float64 decimalOk:true precision:38 scale:255 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_LONG databaseType:LONG scanType: decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_VARCHAR2 databaseType:NCHAR scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:2000 nullableOk:true nullable:true}
{name:MY_ROWID databaseType:ROWID scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_DATE databaseType:DATE scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_BINARYFLOAT databaseType:IBFloat scanType:float32 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_BINARYDOUBLE databaseType:IBDouble scanType:float64 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_RAW databaseType:RAW scanType:[]uint8 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_CHAR databaseType:CHAR scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:1 nullableOk:true nullable:true}
{name:MY_TIMESTAMP databaseType:TimeStampDTY scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_TIMESTAMPTZ databaseType:TimeStampTZ_DTY scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_INTERVALYM databaseType:IntervalYM_DTY scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_INTERVALDS databaseType:IntervalDS_DTY scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_UROWID databaseType:UROWID scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_TIMESTAMPLTZ databaseType:TimeStampLTZ_DTY scanType:time.Time decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_CLOB databaseType:OCIClobLocator scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_BLOB databaseType:OCIBlobLocator scanType:[]uint8 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_NCLOB databaseType:OCIClobLocator scanType:string decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_BFILE databaseType:OCIFileLocator scanType:[]uint8 decimalOk:false precision:0 scale:0 lengthOk:false length:0 nullableOk:true nullable:true}
{name:MY_VARCHAR databaseType:NCHAR scanType:string decimalOk:false precision:0 scale:0 lengthOk:true length:2000 nullableOk:true nullable:true}