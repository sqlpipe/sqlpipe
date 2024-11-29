package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	sf "github.com/snowflakedb/gosnowflake"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
)

var (
	programVersion  = ProgramVersion()
	port            int
	infoLog         = log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime)
	warningLog      = log.New(os.Stdout, "WARNING\t", log.Ldate|log.Ltime)
	errorLog        = log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile)
	psqlAvailable   bool
	bcpAvailable    bool
	sqlldrAvailable bool
	globalTmpDir    string

	cliTransfer                                   bool
	keepFilesCliTransferInput                     bool
	sourceNameCliTransferInput                    string
	sourceTypeCliTransferInput                    string
	sourceConnectionStringCliTransferInput        string
	targetNameCliTransferInput                    string
	targetTypeCliTransferInput                    string
	targetConnectionStringCliTransferInput        string
	targetHostnameCliTransferInput                string
	targetPortCliTransferInput                    int
	targetDatabaseCliTransferInput                string
	targetUsernameCliTransferInput                string
	targetPasswordCliTransferInput                string
	dropTargetTableIfExistsCliTransferInput       bool
	createTargetSchemaIfNotExistsCliTransferInput bool
	createTargetTableIfNotExistsCliTransferInput  bool
	sourceSchemaCliTransferInput                  string
	sourceTableCliTransferInput                   string
	targetSchemaCliTransferInput                  string
	targetTableCliTransferInput                   string
	queryCliTransferInput                         string
	delimiterCliTransferInput                     string
	newlineCliTransferInput                       string
	nullCliTransferInput                          string
)

func main() {

	flag.IntVar(&port, "port", 9000, "api server port")
	displayVersion := flag.Bool("version", false, "display version and exit")

	flag.BoolVar(&cliTransfer, "cli-transfer", false, "perform a cli transfer")
	flag.BoolVar(&keepFilesCliTransferInput, "keep-files", false, "keep files after cli transfer")
	flag.StringVar(&sourceNameCliTransferInput, "source-name", "", "source name")
	flag.StringVar(&sourceTypeCliTransferInput, "source-type", "", "source type")
	flag.StringVar(&sourceConnectionStringCliTransferInput, "source-connection-string", "", "source connection string")
	flag.StringVar(&targetNameCliTransferInput, "target-name", "", "target name")
	flag.StringVar(&targetTypeCliTransferInput, "target-type", "", "target type")
	flag.StringVar(&targetConnectionStringCliTransferInput, "target-connection-string", "", "target connection string")
	flag.StringVar(&targetHostnameCliTransferInput, "target-hostname", "", "target hostname")
	flag.IntVar(&targetPortCliTransferInput, "target-port", 0, "target port")
	flag.StringVar(&targetDatabaseCliTransferInput, "target-database", "", "target database")
	flag.StringVar(&targetUsernameCliTransferInput, "target-username", "", "target username")
	flag.StringVar(&targetPasswordCliTransferInput, "target-password", "", "target password")
	flag.BoolVar(&dropTargetTableIfExistsCliTransferInput, "drop-target-table-if-exists", false, "drop target table if exists")
	flag.BoolVar(&createTargetSchemaIfNotExistsCliTransferInput, "create-target-schema-if-not-exists", false, "create target schema if not exists")
	flag.BoolVar(&createTargetTableIfNotExistsCliTransferInput, "create-target-table-if-not-exists", false, "create target table if not exists")
	flag.StringVar(&sourceSchemaCliTransferInput, "source-schema", "", "source schema")
	flag.StringVar(&sourceTableCliTransferInput, "source-table", "", "source table")
	flag.StringVar(&targetSchemaCliTransferInput, "target-schema", "", "target schema")
	flag.StringVar(&targetTableCliTransferInput, "target-table", "", "target table")
	flag.StringVar(&queryCliTransferInput, "query", "", "query")
	flag.StringVar(&delimiterCliTransferInput, "delimiter", "{dlm}", "delimiter")
	flag.StringVar(&newlineCliTransferInput, "newline", "{nwln}", "newline")
	flag.StringVar(&nullCliTransferInput, "null", "{nll}", "null")

	flag.Parse()

	if *displayVersion {
		fmt.Printf("version:\t%s\n", programVersion)
		os.Exit(0)
	}

	checkDeps()

	globalTmpDir = filepath.Join(os.TempDir(), "sqlpipe")
	err := os.MkdirAll(globalTmpDir, 0600)
	if err != nil {
		errorLog.Fatalf("failed to create tmp dir :: %v", err)
	}

	// snowflake driver logs a lot of stuff that we don't want
	sf.GetLogger().SetLogLevel("fatal")

	if cliTransfer {
		cliTransferInput := CliTransferInput{
			KeepFiles:                     keepFilesCliTransferInput,
			SourceName:                    sourceNameCliTransferInput,
			SourceType:                    sourceTypeCliTransferInput,
			SourceConnectionString:        sourceConnectionStringCliTransferInput,
			TargetName:                    targetNameCliTransferInput,
			TargetType:                    targetTypeCliTransferInput,
			TargetConnectionString:        targetConnectionStringCliTransferInput,
			TargetHostname:                targetHostnameCliTransferInput,
			TargetPort:                    targetPortCliTransferInput,
			TargetDatabase:                targetDatabaseCliTransferInput,
			TargetUsername:                targetUsernameCliTransferInput,
			TargetPassword:                targetPasswordCliTransferInput,
			DropTargetTableIfExists:       dropTargetTableIfExistsCliTransferInput,
			CreateTargetSchemaIfNotExists: createTargetSchemaIfNotExistsCliTransferInput,
			CreateTargetTableIfNotExists:  createTargetTableIfNotExistsCliTransferInput,
			SourceSchema:                  sourceSchemaCliTransferInput,
			SourceTable:                   sourceTableCliTransferInput,
			TargetSchema:                  targetSchemaCliTransferInput,
			TargetTable:                   targetTableCliTransferInput,
			Query:                         queryCliTransferInput,
			Delimiter:                     delimiterCliTransferInput,
			Newline:                       newlineCliTransferInput,
			Null:                          nullCliTransferInput,
		}

		handleCliTransfer(cliTransferInput)
		return
	}

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      routes(),
		IdleTimeout:  time.Minute,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	infoLog.Printf("now listening on port %d", port)

	err = srv.ListenAndServe()
	if err != nil {
		errorLog.Fatal(err)
	}
}
