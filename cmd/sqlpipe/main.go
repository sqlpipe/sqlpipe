package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
	_ "github.com/snowflakedb/gosnowflake"
)

var (
	version         = ProgramVersion()
	port            int
	infoLog         = log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime)
	warningLog      = log.New(os.Stdout, "WARNING\t", log.Ldate|log.Ltime)
	errorLog        = log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile)
	globalTmpDir    string
	psqlAvailable   = false
	bcpAvailable    = false
	sqlLdrAvailable = false
)

func main() {
	flag.IntVar(&port, "port", 9000, "API server port")
	displayVersion := flag.Bool("version", false, "Display version and exit")
	flag.Parse()

	if *displayVersion {
		fmt.Printf("Version:\t%s\n", version)
		os.Exit(0)
	}

	checkDeps()

	// create tmp dir if not exists
	globalTmpDir = filepath.Join(os.TempDir(), "sqlpipe")
	err := os.MkdirAll(globalTmpDir, 0700)
	if err != nil {
		errorLog.Fatalf("failed to create tmp dir :: %v", err)
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
