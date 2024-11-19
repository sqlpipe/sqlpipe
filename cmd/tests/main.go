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
var postgresqlDBName = "mydb"
var postgresqlSchema = "public"
var postgresqlTable = "my_table"
var postgresqlSchemaRequired = true

var mysqlHost string
var mysqlPort int
var mysqlUser string
var mysqlPassword string
var mysqlDBName = "mydb"
var mysqlTable = "my_table"
var mysqlSchemaRequired = false

var mssqlHost string
var mssqlPort int
var mssqlUser string
var mssqlPassword string
var mssqlDBName = "mydb"
var mssqlSchema = "dbo"
var mssqlTable = "my_table"
var mssqlSchemaRequired = true

var oracleHost string
var oraclePort int
var oracleUser string
var oraclePassword string
var oracleDBName = "FREEPDB1"
var oracleTable = "my_table"
var oracleSchemaRequired = false

var snowflakeAccount string
var snowflakeUser string
var snowflakePassword string
var snowflakeDBName = "mydb"
var snowflakeSchema = "public"
var snowflakeTable = "my_table"
var snowflakeSchemaRequired = true

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

	flag.StringVar(&oracleHost, "oracle-host", "", "Oracle host")
	flag.IntVar(&oraclePort, "oracle-port", 0, "Oracle port")
	flag.StringVar(&oracleUser, "oracle-user", "", "Oracle user")
	flag.StringVar(&oraclePassword, "oracle-password", "", "Oracle password")

	flag.StringVar(&snowflakeAccount, "snowflake-account", "", "Snowflake account")
	flag.StringVar(&snowflakeUser, "snowflake-user", "", "Snowflake user")
	flag.StringVar(&snowflakePassword, "snowflake-password", "", "Snowflake password")

	flag.StringVar(&serverAddress, "server-address", "", "Server address")

	flag.Parse()

	// mysqlConnectionInfo := ConnectionInfo{
	// 	SystemType: "mysql",
	// 	Host:       mysqlHost,
	// 	Port:       mysqlPort,
	// 	User:       mysqlUser,
	// 	Password:   mysqlPassword,
	// 	DBName:     mysqlDBName,
	// }

	// mssqlConnectionInfo := ConnectionInfo{
	// 	SystemType: "mssql",
	// 	Host:       mssqlHost,
	// 	Port:       mssqlPort,
	// 	User:       mssqlUser,
	// 	Password:   mssqlPassword,
	// 	DBName:     mssqlDBName,
	// }

	// oracleConnectionInfo := ConnectionInfo{
	// 	SystemType: "oracle",
	// 	Host:       oracleHost,
	// 	Port:       oraclePort,
	// 	User:       oracleUser,
	// 	Password:   oraclePassword,
	// 	DBName:     oracleDBName,
	// }

	logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

	var err error

	postgresqlDB, err = sql.Open("pgx", fmt.Sprintf("host=%s port=%v user=%s password=%s dbname=postgres sslmode=disable", postgresqlHost, postgresqlPort, postgresqlUser, postgresqlPassword))
	if err != nil {
		logger.Info(fmt.Sprintf("error connecting to PostgreSQL :: %v", err))

	}

	err = postgresqlDB.Ping()
	if err != nil {
		logger.Info(fmt.Sprintf("Error connecting to PostgreSQL :: %v", err))
	}

	mysqlDB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%v)/mysql", mysqlUser, mysqlPassword, mysqlHost, mysqlPort))
	if err != nil {
		logger.Info(fmt.Sprintf("error connecting to MySQL :: %v", err))
	}

	err = mysqlDB.Ping()
	if err != nil {
		logger.Info(fmt.Sprintf("Error connecting to MySQL :: %v", err))
	}

	mssqlDB, err = sql.Open("mssql", fmt.Sprintf("server=%s;user id=%s;password=%s;port=%v;database=master", mssqlHost, mssqlUser, mssqlPassword, mssqlPort))
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

	snowflakeDsn := fmt.Sprintf("%s:%s@%s/%s", snowflakeUser, snowflakePassword, snowflakeAccount, snowflakeDBName)

	snowflakeDB, err = sql.Open("snowflake", snowflakeDsn)
	if err != nil {
		logger.Info(fmt.Sprintf("error connecting to Snowflake :: %v", err))
	}

	err = snowflakeDB.Ping()
	if err != nil {
		logger.Info(fmt.Sprintf("Error connecting to Snowflake :: %v", err))
	}

	snowflakeConnectionInfo := ConnectionInfo{
		SystemType:       "snowflake",
		Host:             snowflakeAccount,
		User:             snowflakeUser,
		Password:         snowflakePassword,
		DBName:           snowflakeDBName,
		ConnectionString: snowflakeDsn,
		Schema:           snowflakeSchema,
		Table:            snowflakeTable,
	}

	logger.Info("All connections successful")

	postgresqlConnectionInfo, err := setupPostgreSQL()
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

	err = makeSqlpipeTransfer(postgresqlConnectionInfo, snowflakeConnectionInfo)
	if err != nil {
		logger.Info(fmt.Sprintf("error transferring data :: %v", err))
	}

}
