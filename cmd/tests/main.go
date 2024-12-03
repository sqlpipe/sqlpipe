package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/snowflakedb/gosnowflake"
)

var postgresqlDB *sql.DB
var mysqlDB *sql.DB
var mssqlDB *sql.DB

// var oracleDB *sql.DB
var snowflakeDB *sql.DB
var logger *slog.Logger

var postgresqlHost string
var postgresqlPort int
var postgresqlUser string
var postgresqlPassword string
var postgresqlDBName = "mydb"
var postgresqlSchema = "public"
var postgresqlTable = "my_table"

var mysqlHost string
var mysqlPort int
var mysqlUser string
var mysqlPassword string
var mysqlDBName = "mydb"
var mysqlTable = "my_table"

var mssqlHost string
var mssqlPort int
var mssqlUser string
var mssqlPassword string
var mssqlDBName = "mydb"
var mssqlSchema = "dbo"
var mssqlTable = "my_table"

// var oracleHost string
// var oraclePort int
// var oracleUser string
// var oraclePassword string
// var oracleDBName = "XE"
// var oracleTable = "my_table"

var snowflakeAccount string
var snowflakeUser string
var snowflakePassword string
var snowflakeDBName = "mydb"
var snowflakeSchema = "public"
var snowflakeTable = "my_table"

var serverAddress string

func main() {

	flag.StringVar(&postgresqlHost, "postgresql-host", "", "PostgreSQL host")
	flag.IntVar(&postgresqlPort, "postgresql-port", 0, "PostgreSQL port")
	flag.StringVar(&postgresqlUser, "postgresql-user", "", "PostgreSQL user")
	flag.StringVar(&postgresqlPassword, "postgresql-password", "", "PostgreSQL password")

	flag.StringVar(&mysqlHost, "mysql-host", "", "MySQL host")
	flag.IntVar(&mysqlPort, "mysql-port", 0, "MySQL port")
	flag.StringVar(&mysqlUser, "mysql-user", "", "MySQL user")
	flag.StringVar(&mysqlPassword, "mysql-password", "", "MySQL password")

	flag.StringVar(&mssqlHost, "mssql-host", "", "SQL Server host")
	flag.IntVar(&mssqlPort, "mssql-port", 0, "SQL Server port")
	flag.StringVar(&mssqlUser, "mssql-user", "", "SQL Server user")
	flag.StringVar(&mssqlPassword, "mssql-password", "", "SQL Server password")

	// flag.StringVar(&oracleHost, "oracle-host", "", "Oracle host")
	// flag.IntVar(&oraclePort, "oracle-port", 0, "Oracle port")
	// flag.StringVar(&oracleUser, "oracle-user", "", "Oracle user")
	// flag.StringVar(&oraclePassword, "oracle-password", "", "Oracle password")

	flag.StringVar(&snowflakeAccount, "snowflake-account", "", "Snowflake account")
	flag.StringVar(&snowflakeUser, "snowflake-user", "", "Snowflake user")
	flag.StringVar(&snowflakePassword, "snowflake-password", "", "Snowflake password")

	flag.StringVar(&serverAddress, "server-address", "", "Server address")

	flag.Parse()

	logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

	var err error

	postgresqlDB, err = sql.Open("pgx", fmt.Sprintf("host=%s port=%v user=%s password=%s dbname=postgres sslmode=disable", postgresqlHost, postgresqlPort, postgresqlUser, postgresqlPassword))
	if err != nil {
		logger.Error(fmt.Sprintf("error creating PostgreSQL connection pool :: %v", err))
		os.Exit(1)
	}

	err = postgresqlDB.Ping()
	if err != nil {
		logger.Error(fmt.Sprintf("Error pinging PostgreSQL :: %v", err))
		os.Exit(1)
	}

	mysqlDB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%v)/mysql", mysqlUser, mysqlPassword, mysqlHost, mysqlPort))
	if err != nil {
		logger.Error(fmt.Sprintf("error creating MySQL connection pool :: %v", err))
		os.Exit(1)
	}

	err = mysqlDB.Ping()
	if err != nil {
		logger.Error(fmt.Sprintf("Error pinging MySQL :: %v", err))
		os.Exit(1)
	}

	mssqlDB, err = sql.Open("mssql", fmt.Sprintf("server=%s;user id=%s;password=%s;port=%v;database=master", mssqlHost, mssqlUser, mssqlPassword, mssqlPort))
	if err != nil {
		logger.Error(fmt.Sprintf("error creating SQL Server connection pool :: %v", err))
		os.Exit(1)
	}

	err = mssqlDB.Ping()
	if err != nil {
		logger.Error(fmt.Sprintf("Error pinging SQL Server :: %v", err))
		os.Exit(1)
	}

	// urlOptions := map[string]string{
	// 	"dba privilege": "sysdba",
	// }

	// connStr := go_ora.BuildUrl(oracleHost, oraclePort, oracleDBName, oracleUser, oraclePassword, urlOptions)

	// oracleDB, err = sql.Open("oracle", connStr)
	// if err != nil {
	// 	logger.Error(fmt.Sprintf("error creating Oracle connection pool :: %v", err))
	// 	os.Exit(1)
	// }

	// err = oracleDB.Ping()
	// if err != nil {
	// 	logger.Error(fmt.Sprintf("error pinging Oracle :: %v", err))
	// 	os.Exit(1)
	// }

	snowflakeDsn := fmt.Sprintf("%s:%s@%s/%s/%v", snowflakeUser, snowflakePassword, snowflakeAccount, snowflakeDBName, snowflakeSchema)

	snowflakeDB, err = sql.Open("snowflake", snowflakeDsn)
	if err != nil {
		logger.Error(fmt.Sprintf("error creating Snowflake connection pool :: %v", err))
		os.Exit(1)
	}

	err = snowflakeDB.Ping()
	if err != nil {
		logger.Error(fmt.Sprintf("Error pinging Snowflake :: %v", err))
		os.Exit(1)
	}

	logger.Info("All connections successful")

	postgresqlConnectionInfo, err := setupPostgreSQL()
	if err != nil {
		logger.Info(fmt.Sprintf("error setting up PostgreSQL :: %v", err))
	}

	mysqlConnectionInfo, err := setupMySQL()
	if err != nil {
		logger.Info(fmt.Sprintf("error setting up MySQL :: %v", err))
	}

	mssqlConnectionInfo, err := setupMssql()
	if err != nil {
		logger.Info(fmt.Sprintf("error setting up SQL Server :: %v", err))
	}

	// oracleConnectionInfo, err := setupOracle()
	// if err != nil {
	// 	logger.Info(fmt.Sprintf("error setting up Oracle :: %v", err))
	// }

	snowflakeConnectionInfo, err := setupSnowflake()
	if err != nil {
		logger.Info(fmt.Sprintf("error setting up Snowflake :: %v", err))
	}

	// sources := []ConnectionInfo{postgresqlConnectionInfo, mysqlConnectionInfo, mssqlConnectionInfo, oracleConnectionInfo, snowflakeConnectionInfo}
	// targets := []ConnectionInfo{postgresqlConnectionInfo, mysqlConnectionInfo, mssqlConnectionInfo, oracleConnectionInfo, snowflakeConnectionInfo}

	sources := []ConnectionInfo{postgresqlConnectionInfo, mysqlConnectionInfo, mssqlConnectionInfo, snowflakeConnectionInfo}
	targets := []ConnectionInfo{postgresqlConnectionInfo, mysqlConnectionInfo, mssqlConnectionInfo, snowflakeConnectionInfo}

	for _, source := range sources {
		for _, target := range targets {
			err = makeSqlpipeTransfer(source, target)
			if err != nil {
				logger.Error(fmt.Sprintf("error transferring data from %v to %v :: %v", source.SystemType, target.SystemType, err))
			}
			logger.Info(fmt.Sprintf("transferred data from %v to %v", source.SystemType, target.SystemType))
		}
	}
}
