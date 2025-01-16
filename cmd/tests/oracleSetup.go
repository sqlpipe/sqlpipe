package main

// import (
// 	"context"
// 	"database/sql"
// 	"fmt"
// 	"time"

// 	go_ora "github.com/sijms/go-ora/v2"
// )

// func setupOracle() (ConnectionInfo, error) {

// 	connectionInfo := ConnectionInfo{}

// 	var ctx context.Context
// 	var cancel context.CancelFunc
// 	var err error

// 	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	_, err = oracleDB.ExecContext(ctx, fmt.Sprintf("CREATE PLUGGABLE DATABASE mydb ADMIN USER mydb_admin IDENTIFIED BY %v FILE_NAME_CONVERT = ('/opt/oracle/oradata/XE/pdbseed/', '/opt/oracle/oradata/XE/mydb/')", oraclePassword))
// 	if err != nil {
// 		logger.Warn(fmt.Sprintf("error creating oracle user :: %v", err))
// 	}

// 	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	_, err = oracleDB.ExecContext(ctx, "ALTER PLUGGABLE DATABASE mydb OPEN")
// 	if err != nil {
// 		logger.Warn(fmt.Sprintf("error opening mydb :: %v", err))
// 	}

// 	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	_, err = oracleDB.ExecContext(ctx, "ALTER PLUGGABLE DATABASE mydb SAVE STATE")
// 	if err != nil {
// 		logger.Warn(fmt.Sprintf("error saving mydb state :: %v", err))
// 	}

// 	urlOptions := map[string]string{
// 		"dba privilege": "sysdba",
// 	}

// 	oracleDB, err = sql.Open("oracle", go_ora.BuildUrl(oracleHost, oraclePort, "mydb", oracleUser, oraclePassword, urlOptions))
// 	if err != nil {
// 		logger.Error(fmt.Sprintf("error creating Oracle connection pool :: %v", err))
// 		return connectionInfo, err
// 	}

// 	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	_, err = oracleDB.ExecContext(ctx, "CREATE TABLESPACE mydb_tablespace DATAFILE '/opt/oracle/oradata/XE/mydb/mydb_tablespace.dbf' SIZE 500M AUTOEXTEND ON NEXT 100M")
// 	if err != nil {
// 		logger.Warn(fmt.Sprintf("error creating mydb_tablespace :: %v", err))
// 	}

// 	_, err = oracleDB.ExecContext(ctx, "ALTER USER mydb_admin DEFAULT TABLESPACE mydb_tablespace")
// 	if err != nil {
// 		logger.Warn(fmt.Sprintf("error setting default tablespace :: %v", err))
// 	}

// 	_, err = oracleDB.ExecContext(ctx, "ALTER USER mydb_admin QUOTA UNLIMITED ON mydb_tablespace")
// 	if err != nil {
// 		logger.Warn(fmt.Sprintf("error granting quota on tablespace :: %v", err))
// 	}

// 	_, err = oracleDB.ExecContext(ctx, "GRANT SYSDBA TO mydb_admin")
// 	if err != nil {
// 		logger.Warn(fmt.Sprintf("error granting sysdba :: %v", err))
// 	}

// 	_, err = oracleDB.ExecContext(ctx, "grant create table to mydb_admin")
// 	if err != nil {
// 		logger.Warn(fmt.Sprintf("error granting creat table :: %v", err))
// 	}

// 	dsn := go_ora.BuildUrl(oracleHost, oraclePort, "mydb", "mydb_admin", oraclePassword, urlOptions)

// 	oracleDB, err = sql.Open("oracle", dsn)
// 	if err != nil {
// 		return connectionInfo, fmt.Errorf("error creating Oracle connection pool :: %v", err)
// 	}

// 	err = oracleDB.Ping()
// 	if err != nil {
// 		return connectionInfo, fmt.Errorf("error pinging Oracle :: %v", err)
// 	}

// 	connectionInfo = ConnectionInfo{
// 		SystemType:       "oracle",
// 		Host:             oracleHost,
// 		Port:             oraclePort,
// 		User:             oracleUser,
// 		Password:         oraclePassword,
// 		DBName:           oracleDBName,
// 		ConnectionString: dsn,
// 		Schema:           "mydb_admin",
// 		Table:            oracleTable,
// 	}

// 	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	_, err = oracleDB.ExecContext(ctx, "CREATE TABLE mydb_admin.my_table (my_id NUMBER GENERATED AS IDENTITY primary key, my_nchar NCHAR(10), my_number NUMBER(10,5), my_float FLOAT, my_varchar2 VARCHAR2(128), my_date DATE, my_binaryfloat BINARY_FLOAT, my_binarydouble BINARY_DOUBLE, my_raw RAW(128), my_char CHAR(8), my_timestamp TIMESTAMP, my_timestamptz TIMESTAMP WITH TIME ZONE, my_intervalym INTERVAL YEAR TO MONTH, my_intervalds INTERVAL DAY TO SECOND, my_urowid urowid, my_timestampltz TIMESTAMP WITH LOCAL TIME ZONE, my_clob CLOB, my_blob BLOB, my_nclob NCLOB, my_varchar VARCHAR(128), last_modified TIMESTAMP DEFAULT SYSTIMESTAMP)")
// 	if err != nil {
// 		logger.Warn(fmt.Sprintf("error creating oracle table :: %v", err))
// 	}

// 	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	_, err = oracleDB.ExecContext(ctx, "delete from mydb_admin.my_table")
// 	if err != nil {
// 		return connectionInfo, fmt.Errorf("error deleting from my_table :: %v", err)
// 	}

// 	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	_, err = oracleDB.ExecContext(ctx, `
// CREATE OR REPLACE TRIGGER mydb_admin.trg_my_table_last_modified
// BEFORE UPDATE ON mydb_admin.my_table
// FOR EACH ROW
// BEGIN
//     :NEW.last_modified := SYSTIMESTAMP;
// END;
// `)
// 	if err != nil {
// 		return connectionInfo, fmt.Errorf("error creating trg_my_table_last_modified :: %v", err)
// 	}

// 	for i := 0; i < 1000; i++ {
// 		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
// 		defer cancel()

// 		_, err = oracleDB.ExecContext(ctx, `
// INSERT INTO mydb_admin.my_table (my_nchar, my_number, my_float, my_varchar2, my_date, my_binaryfloat, my_binarydouble, my_raw, my_char, my_timestamp, my_timestamptz, my_intervalym, my_intervalds, my_timestampltz, my_clob, my_blob, my_nclob, my_varchar) VALUES (N'A CHAR', 123.45, 123.45, 'VARCHAR2 column', TO_DATE('2015-09-29', 'YYYY-MM-DD'), 123.45, 123.45, hextoraw('53756D6D6572'), 'C', TO_TIMESTAMP('2015-08-10 12:34:56.765432', 'YYYY-MM-DD HH24:MI:SS.FF6'), TO_TIMESTAMP_TZ('2015-08-10 12:34:56.765432 -07:00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'), INTERVAL '5-2' YEAR TO MONTH, INTERVAL '4 03:02:01' DAY TO SECOND, TO_TIMESTAMP('2015-08-10 12:34:56.765432', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'This is a CLOB', TO_BLOB(HEXTORAW('4D7953616D706C6544617461')), N'This ''is'' a NCLOB', 'VARCHAR column')`)
// 		if err != nil {
// 			return connectionInfo, fmt.Errorf("failed to insert data: %w", err)
// 		}
// 	}

// 	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	_, err = oracleDB.ExecContext(ctx, `
// INSERT INTO mydb_admin.my_table (
//     my_nchar,
//     my_number,
//     my_float,
//     my_varchar2,
//     my_date,
//     my_binaryfloat,
//     my_binarydouble,
//     my_raw,
//     my_char,
//     my_timestamp,
//     my_timestamptz,
//     my_intervalym,
//     my_intervalds,
//     my_urowid,
//     my_timestampltz,
//     my_clob,
//     my_blob,
//     my_nclob,
//     my_varchar
// ) VALUES (
//     NULL,
//     NULL,
//     NULL,
//     'one not null',
//     NULL,
//     NULL,
//     NULL,
//     NULL,
//     NULL,
//     NULL,
//     NULL,
//     NULL,
//     NULL,
//     NULL,
//     NULL,
//     NULL,
//     NULL,
//     NULL,
//     NULL
// )`)
// 	if err != nil {
// 		return connectionInfo, fmt.Errorf("failed to insert data: %w", err)
// 	}

// 	logger.Info("Oracle setup successful")

// 	return connectionInfo, nil
// }
