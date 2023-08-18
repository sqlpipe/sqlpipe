package main

import (
	"embed"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
	_ "github.com/snowflakedb/gosnowflake"
)

var (
	version        = ProgramVersion()
	port           int
	infoLog        = log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime)
	warningLog     = log.New(os.Stdout, "WARNING\t", log.Ldate|log.Ltime)
	errorLog       = log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile)
	acceptLicenses = false
	psqlAvailable  = false
	psqlTmpFile    *os.File
	bcpAvailable   = false
	bcpTmpFile     *os.File
)

//go:embed deps
var depsFs embed.FS

func main() {
	flag.IntVar(&port, "port", 9000, "API server port")
	flag.BoolVar(&acceptLicenses, "accept-licenses", false, "Accept all licenses at https://sqlpipe.com/sqlpipe-licenses")
	displayVersion := flag.Bool("version", false, "Display version and exit")
	flag.Parse()

	if *displayVersion {
		fmt.Printf("Version:\t%s\n", version)
		os.Exit(0)
	}

	if platform == "" {
		errorLog.Fatal("unsupported OS / CPU platform")
	}

	if !acceptLicenses {
		errorLog.Fatal("you must provide the --accept-licenses flag to signify you all third party licenses at https://sqlpipe.com/sqlpipe-licenses")
	}

	err := loadDeps()
	if err != nil {
		errorLog.Fatalf("failed to load dependencies :: %v", err)
	}

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      routes(),
		IdleTimeout:  time.Minute,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	infoLog.Printf("starting SQLpipe on port %d\n", port)

	err = srv.ListenAndServe()
	if err != nil {
		errorLog.Fatal(err)
	}
}
