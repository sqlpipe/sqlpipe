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
	go_ora "github.com/sijms/go-ora/v2"
	_ "github.com/snowflakedb/gosnowflake"
)

var postgresqlDB *sql.DB
var mysqlDB *sql.DB
var mssqlDB *sql.DB
var oracleDB *sql.DB
var snowflakeDB *sql.DB
var logger *slog.Logger

var postgresqlHost string
var postgresqlPort int
var postgresqlUser string
var postgresqlPassword string
var postgresqlDBName string

var mysqlHost string
var mysqlPort int
var mysqlUser string
var mysqlPassword string
var mysqlDBName string

var mssqlHost string
var mssqlPort int
var mssqlUser string
var mssqlPassword string
var mssqlDBName string

var oracleHost string
var oraclePort int
var oracleUser string
var oraclePassword string
var oracleDBName string

var snowflakeAccount string
var snowflakeUser string
var snowflakePassword string
var snowflakeDBName string

func main() {

	flag.StringVar(&postgresqlHost, "postgresql-host", "", "PostgreSQL host")
	flag.IntVar(&postgresqlPort, "postgresql-port", 0, "PostgreSQL port")
	flag.StringVar(&postgresqlUser, "postgresql-user", "", "PostgreSQL user")
	flag.StringVar(&postgresqlPassword, "postgresql-password", "", "PostgreSQL password")
	flag.StringVar(&postgresqlDBName, "postgresql-db", "", "PostgreSQL database")

	flag.StringVar(&mysqlHost, "mysql-host", "", "MySQL host")
	flag.IntVar(&mysqlPort, "mysql-port", 0, "MySQL port")
	flag.StringVar(&mysqlUser, "mysql-user", "", "MySQL user")
	flag.StringVar(&mysqlPassword, "mysql-password", "", "MySQL password")
	flag.StringVar(&mysqlDBName, "mysql-db", "", "MySQL database")

	flag.StringVar(&mssqlHost, "mssql-host", "", "SQL Server host")
	flag.IntVar(&mssqlPort, "mssql-port", 0, "SQL Server port")
	flag.StringVar(&mssqlUser, "mssql-user", "", "SQL Server user")
	flag.StringVar(&mssqlPassword, "mssql-password", "", "SQL Server password")
	flag.StringVar(&mssqlDBName, "mssql-db", "", "SQL Server database")

	flag.StringVar(&oracleHost, "oracle-host", "", "Oracle host")
	flag.IntVar(&oraclePort, "oracle-port", 0, "Oracle port")
	flag.StringVar(&oracleUser, "oracle-user", "", "Oracle user")
	flag.StringVar(&oraclePassword, "oracle-password", "", "Oracle password")
	flag.StringVar(&oracleDBName, "oracle-db", "", "Oracle database")

	flag.StringVar(&snowflakeAccount, "snowflake-account", "", "Snowflake account")
	flag.StringVar(&snowflakeUser, "snowflake-user", "", "Snowflake user")
	flag.StringVar(&snowflakePassword, "snowflake-password", "", "Snowflake password")
	flag.StringVar(&snowflakeDBName, "snowflake-db", "", "Snowflake database")

	flag.Parse()

	logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

	var err error

	postgresqlDB, err = sql.Open("pgx", fmt.Sprintf("host=%s port=%v user=%s password=%s dbname=%s sslmode=disable", postgresqlHost, postgresqlPort, postgresqlUser, postgresqlPassword, postgresqlDBName))
	if err != nil {
		logger.Info(fmt.Sprintf("error connecting to PostgreSQL :: %v", err))

	}

	err = postgresqlDB.Ping()
	if err != nil {
		logger.Info(fmt.Sprintf("Error connecting to PostgreSQL :: %v", err))
	}

	mysqlDB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%v)/%s", mysqlUser, mysqlPassword, mysqlHost, mysqlPort, mysqlDBName))
	if err != nil {
		logger.Info(fmt.Sprintf("error connecting to MySQL :: %v", err))
	}

	err = mysqlDB.Ping()
	if err != nil {
		logger.Info(fmt.Sprintf("Error connecting to MySQL :: %v", err))
	}

	mssqlDB, err = sql.Open("mssql", fmt.Sprintf("server=%s;user id=%s;password=%s;port=%v;database=%s", mssqlHost, mssqlUser, mssqlPassword, mssqlPort, mssqlDBName))
	if err != nil {
		logger.Info(fmt.Sprintf("error connecting to SQL Server :: %v", err))
	}

	err = mssqlDB.Ping()
	if err != nil {
		logger.Info(fmt.Sprintf("Error connecting to SQL Server :: %v", err))
	}

	urlOptions := map[string]string{
		"dba privilege": "sysdba",
	}

	connStr := go_ora.BuildUrl(oracleHost, oraclePort, oracleDBName, oracleUser, oraclePassword, urlOptions)

	oracleDB, err = sql.Open("oracle", connStr)
	if err != nil {
		logger.Info(fmt.Sprintf("error connecting to Oracle :: %v", err))
	}

	err = oracleDB.Ping()
	if err != nil {
		logger.Info(fmt.Sprintf("Error connecting to Oracle :: %v", err))
	}

	snowflakeDB, err = sql.Open("snowflake", fmt.Sprintf("%s:%s@%s/%s", snowflakeUser, snowflakePassword, snowflakeAccount, snowflakeDBName))
	if err != nil {
		logger.Info(fmt.Sprintf("error connecting to Snowflake :: %v", err))
	}

	err = snowflakeDB.Ping()
	if err != nil {
		logger.Info(fmt.Sprintf("Error connecting to Snowflake :: %v", err))
	}

	logger.Info("All connections successful")

	err = setupPostgreSQL()
	if err != nil {
		logger.Info(fmt.Sprintf("error setting up PostgreSQL :: %v", err))
	}

	err = setupMySQL()
	if err != nil {
		logger.Info(fmt.Sprintf("error setting up MySQL :: %v", err))
	}

	err = setupMssql()
	if err != nil {
		logger.Info(fmt.Sprintf("error setting up SQL Server :: %v", err))
	}

	err = setupOracle()
	if err != nil {
		logger.Info(fmt.Sprintf("error setting up Oracle :: %v", err))
	}
}
