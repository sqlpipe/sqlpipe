package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func setupMssql() (ConnectionInfo, error) {

	var connectionInfo ConnectionInfo

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := mssqlDB.ExecContext(ctx, "DROP DATABASE mydb")
	if err != nil {
		logger.Warn(fmt.Sprintf("error dropping mydb in mssql :: %v", err))
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = mssqlDB.ExecContext(ctx, "CREATE DATABASE mydb")
	if err != nil {
		return connectionInfo, fmt.Errorf("error creating database :: %v", err)
	}

	dsn := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%v;database=mydb", mssqlHost, mssqlUser, mssqlPassword, mssqlPort)
	mssqlDB, err = sql.Open("mssql", dsn)
	if err != nil {
		return connectionInfo, fmt.Errorf("error creating mssql connection pool :: %v", err)
	}

	err = mssqlDB.Ping()
	if err != nil {
		return connectionInfo, fmt.Errorf("error pinging mssql :: %v", err)
	}

	connectionInfo = ConnectionInfo{
		SystemType:       "mssql",
		Host:             mssqlHost,
		User:             mssqlUser,
		Password:         mssqlPassword,
		DBName:           mssqlDBName,
		ConnectionString: dsn,
		Schema:           mssqlSchema,
		Table:            mssqlTable,
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = mssqlDB.ExecContext(ctx, `
CREATE TABLE my_table (
    my_id bigINT IDENTITY(1,1) PRIMARY KEY,
    my_nchar NCHAR(4),
    my_char CHAR(4),
    my_nvarchar_max NVARCHAR(MAX),
    my_nvarchar NVARCHAR(50),
    my_varchar_max VARCHAR(MAX),
    my_varchar VARCHAR(50),
    my_empty_varchar VARCHAR(50),
    my_bigint BIGINT,
    my_int INT,
    my_smallint SMALLINT,
    my_tinyint TINYINT,
    my_float FLOAT,
    my_real REAL,
    my_decimal DECIMAL(10, 5),
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
    my_bit BIT,
    my_uniqueidentifier UNIQUEIDENTIFIER,
    my_xml XML,
    my_timestamp TIMESTAMP,
    last_modified DATETIME DEFAULT GETDATE()
)`)
	if err != nil {
		return connectionInfo, fmt.Errorf("error creating my_table :: %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = mssqlDB.ExecContext(ctx, `
CREATE TRIGGER tr_my_table_last_modified
ON my_table
AFTER UPDATE
AS
BEGIN
    UPDATE my_table
    SET last_modified = GETDATE()
    WHERE my_id IN (SELECT DISTINCT my_id FROM inserted)
END`)
	if err != nil {
		return connectionInfo, fmt.Errorf("error creating tr_my_table_last_modified function :: %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		_, err = mssqlDB.ExecContext(ctx, `
INSERT INTO my_table (my_nchar, my_char, my_nvarchar_max, my_nvarchar, my_varchar_max, my_varchar, my_empty_varchar, my_bigint, my_int, my_smallint, my_tinyint, my_float, my_real, my_decimal, my_money, my_smallmoney, my_datetime2, my_datetime, my_smalldatetime, my_datetimeoffset, my_date, my_time, my_binary, my_varbinary, my_bit, my_uniqueidentifier, my_xml)
VALUES
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>'),
(N'ABCD', 'ABCD', N'This is a "test" message', N'Test message', 'This is a ''test'' message', 'Test message', '', 123456789, 12345, 123, 12, 123.45, 123.45, 123.43, 123.45, 123.45, '2023-07-23T14:30:00.7654321', '2023-07-23T14:30:00.765', '2023-07-23T14:30:00', '2023-07-23T14:30:00.7654321-07:00', '2023-07-23', '14:30:00', 0x010101, 0x010101, 1, NEWID(), '<root><test>Some XML data</test></root>');`)
		if err != nil {
			return connectionInfo, fmt.Errorf("failed to insert data: %w", err)
		}
	}

	logger.Info("mssql setup successful")

	return connectionInfo, nil
}
