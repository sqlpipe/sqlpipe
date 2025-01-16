package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func setupSnowflake() (ConnectionInfo, error) {

	connectionInfo := ConnectionInfo{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := snowflakeDB.ExecContext(ctx, fmt.Sprintf("DROP DATABASE %v", snowflakeDBName))
	if err != nil {
		logger.Warn(fmt.Sprintf("error dropping mydb in snowflake :: %v", err))
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = snowflakeDB.ExecContext(ctx, "CREATE DATABASE mydb")
	if err != nil {
		return connectionInfo, fmt.Errorf("error creating database :: %v", err)
	}

	dsn := fmt.Sprintf("%v:%v@%v/%v/%v", snowflakeUser, snowflakePassword, snowflakeAccount, snowflakeDBName, snowflakeSchema)

	snowflakeDB, err = sql.Open("snowflake", dsn)
	if err != nil {
		return connectionInfo, fmt.Errorf("error creating snowflake connection pool :: %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = snowflakeDB.PingContext(ctx)
	if err != nil {
		return connectionInfo, fmt.Errorf("error pinging snowflake :: %v", err)
	}

	connectionInfo = ConnectionInfo{
		SystemType:       "snowflake",
		Account:          snowflakeAccount,
		User:             snowflakeUser,
		Password:         snowflakePassword,
		DBName:           snowflakeDBName,
		ConnectionString: dsn,
		Schema:           snowflakeSchema,
		Table:            snowflakeTable,
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = snowflakeDB.ExecContext(ctx, "DROP SEQUENCE IF EXISTS my_table_seq")
	if err != nil {
		return connectionInfo, fmt.Errorf("error dropping sequence :: %v", err)
	}

	_, err = snowflakeDB.ExecContext(ctx, "CREATE SEQUENCE my_table_seq START 1 INCREMENT 1;")
	if err != nil {
		return connectionInfo, fmt.Errorf("error creating sequence :: %v", err)
	}

	_, err = snowflakeDB.ExecContext(ctx, `
CREATE TABLE my_table (
	my_serial 	   	  BIGINT DEFAULT my_table_seq.NEXTVAL,
    my_varchar        VARCHAR(100),
    my_number         NUMBER(10, 5),
    my_int            INTEGER,
    my_bigint         BIGINT,
    my_float          FLOAT,
    my_double         DOUBLE,
    my_boolean        BOOLEAN,
    my_date           DATE,
    my_time           TIME,
    my_timestamp      TIMESTAMP,
    my_timestampltz   TIMESTAMP_LTZ,
    my_timestampntz   TIMESTAMP_NTZ,
    my_timestamptz    TIMESTAMP_TZ,
    my_variant        VARIANT,
    my_object         OBJECT,
    my_array          ARRAY,
    my_binary         BINARY,
    my_geography      GEOGRAPHY
)`)
	if err != nil {
		return connectionInfo, fmt.Errorf("error creating my_table :: %v", err)
	}

	for i := 0; i < 10; i++ {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err = snowflakeDB.ExecContext(ctx, `
INSERT INTO my_table (
    my_varchar,
    my_number,
    my_int,
    my_bigint,
    my_float,
    my_double,
    my_boolean,
    my_date,
    my_time,
    my_timestamp,
    my_timestampltz,
    my_timestampntz,
    my_timestamptz,
    my_variant,
    my_object,
    my_array,
    my_binary,
    my_geography
)
SELECT
    'Sample Text' AS my_varchar,
    12345.67890 AS my_number,
    42 AS my_int,
    1234567890123 AS my_bigint,
    3.14159 AS my_float,
    2.7182818284 AS my_double,
    TRUE AS my_boolean,
    '2023-01-01'::DATE AS my_date,
    '12:34:56'::TIME AS my_time,
    '2023-01-01 12:34:56'::TIMESTAMP AS my_timestamp,
    '2023-01-01 12:34:56 -05:00'::TIMESTAMP_LTZ AS my_timestampltz,
    '2023-01-01 12:34:56'::TIMESTAMP_NTZ AS my_timestampntz,
    '2023-01-01 12:34:56 -05:00'::TIMESTAMP_TZ AS my_timestamptz,
    PARSE_JSON('{"key": "value"}') AS my_variant,
    OBJECT_CONSTRUCT('key1', 'value1') AS my_object,
    ARRAY_CONSTRUCT(1, 2, 3) AS my_array,
    TO_BINARY('FFEE', 'HEX') AS my_binary,
    TO_GEOGRAPHY('POINT(-122.35 47.65)') AS my_geography`)
	}
	if err != nil {
		return connectionInfo, fmt.Errorf("error inserting data :: %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = snowflakeDB.ExecContext(ctx, `
INSERT INTO my_table (
    my_varchar,
    my_number,
    my_int,
    my_bigint,
    my_float,
    my_double,
    my_boolean,
    my_date,
    my_time,
    my_timestamp,
    my_timestampltz,
    my_timestampntz,
    my_timestamptz,
    my_variant,
    my_object,
    my_array,
    my_binary,
    my_geography
)
VALUES (
    NULL, -- my_varchar
    NULL, -- my_number
    NULL, -- my_int
    NULL, -- my_bigint
    NULL, -- my_float
    NULL, -- my_double
    NULL, -- my_boolean
    NULL, -- my_date
    NULL, -- my_time
    NULL, -- my_timestamp
    NULL, -- my_timestampltz
    NULL, -- my_timestampntz
    NULL, -- my_timestamptz
    NULL, -- my_variant
    NULL, -- my_object
    NULL, -- my_array
    NULL, -- my_binary
    NULL  -- my_geography
)`)
	if err != nil {
		return connectionInfo, fmt.Errorf("error inserting data :: %v", err)
	}

	logger.Info("snowflake setup successful")

	return connectionInfo, nil
}
