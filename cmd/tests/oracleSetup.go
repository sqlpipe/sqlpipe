package main

import (
	"context"
	"fmt"
	"time"
)

func setupOracle() error {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := "CREATE USER free_user IDENTIFIED BY :1"
	_, err := oracleDB.ExecContext(ctx, query, oraclePassword)
	if err != nil {
		logger.Warn(fmt.Sprintf("error creating oracle user :: %v", err))
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = oracleDB.ExecContext(ctx, "ALTER USER FREE_USER QUOTA UNLIMITED ON USERS")
	if err != nil {
		logger.Warn(fmt.Sprintf("error altering oracle user :: %v", err))
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = oracleDB.ExecContext(ctx, "CREATE TABLE free_user.my_table (my_id NUMBER GENERATED AS IDENTITY primary key, my_nchar NCHAR(10), my_number NUMBER(10,5), my_float FLOAT, my_varchar2 VARCHAR2(128), my_date DATE, my_binaryfloat BINARY_FLOAT, my_binarydouble BINARY_DOUBLE, my_raw RAW(128), my_char CHAR(8), my_timestamp TIMESTAMP, my_timestamptz TIMESTAMP WITH TIME ZONE, my_intervalym INTERVAL YEAR TO MONTH, my_intervalds INTERVAL DAY TO SECOND, my_urowid urowid, my_timestampltz TIMESTAMP WITH LOCAL TIME ZONE, my_clob CLOB, my_blob BLOB, my_nclob NCLOB, my_varchar VARCHAR(128), last_modified TIMESTAMP DEFAULT SYSTIMESTAMP)")
	if err != nil {
		logger.Warn(fmt.Sprintf("error creating oracle table :: %v", err))
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = oracleDB.ExecContext(ctx, "delete from free_user.my_table")
	if err != nil {
		return fmt.Errorf("error deleting from my_table :: %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = oracleDB.ExecContext(ctx, `
CREATE OR REPLACE TRIGGER free_user.trg_my_table_last_modified
BEFORE UPDATE ON free_user.my_table
FOR EACH ROW
BEGIN
    :NEW.last_modified := SYSTIMESTAMP;
END;
`)
	if err != nil {
		return fmt.Errorf("error creating trg_my_table_last_modified :: %v", err)
	}

	for i := 0; i < 1000; i++ {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err = oracleDB.ExecContext(ctx, `
INSERT INTO free_user.my_table (my_nchar, my_number, my_float, my_varchar2, my_date, my_binaryfloat, my_binarydouble, my_raw, my_char, my_timestamp, my_timestamptz, my_intervalym, my_intervalds, my_timestampltz, my_clob, my_blob, my_nclob, my_varchar) VALUES (N'A CHAR', 123.45, 123.45, 'VARCHAR2 column', TO_DATE('2015-09-29', 'YYYY-MM-DD'), 123.45, 123.45, hextoraw('53756D6D6572'), 'C', TO_TIMESTAMP('2015-08-10 12:34:56.765432', 'YYYY-MM-DD HH24:MI:SS.FF6'), TO_TIMESTAMP_TZ('2015-08-10 12:34:56.765432 -07:00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'), INTERVAL '5-2' YEAR TO MONTH, INTERVAL '4 03:02:01' DAY TO SECOND, TO_TIMESTAMP('2015-08-10 12:34:56.765432', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'This is a CLOB', TO_BLOB(HEXTORAW('4D7953616D706C6544617461')), N'This ''is'' a NCLOB', 'VARCHAR column')`)
		if err != nil {
			return fmt.Errorf("failed to insert data: %w", err)
		}
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = oracleDB.ExecContext(ctx, `
INSERT INTO free_user.my_table (
    my_nchar, 
    my_number, 
    my_float, 
    my_varchar2, 
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
    my_varchar
) VALUES (
    NULL, 
    NULL, 
    NULL, 
    'one not null',
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
)`)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	logger.Info("Oracle setup successful")

	return nil
}
