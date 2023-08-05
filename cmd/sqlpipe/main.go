package main

import (
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
	_ "github.com/snowflakedb/gosnowflake"
)

//go:embed deps
var depsFs embed.FS

var (
	version      = ProgramVersion()
	port         int
	infoLog      = log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime)
	errorLog     = log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile)
	psqlTmpFile  *os.File
	bcpAvailable bool = false
)

func main() {
	flag.IntVar(&port, "port", 9000, "API server port")
	displayVersion := flag.Bool("version", false, "Display version and exit")
	flag.Parse()

	if *displayVersion {
		fmt.Printf("Version:\t%s\n", version)
		os.Exit(0)
	}

	// create psql tmp file
	var err error
	psqlTmpFile, err = os.CreateTemp("", "")
	if err != nil {
		errorLog.Fatalf("failed to create psql temp file :: %v", err)
	}

	psqlBytes, err := fs.ReadFile(depsFs, "deps/psql")
	if err != nil {
		errorLog.Fatalf("failed to read psql bytes :: %v", err)
	}

	_, err = psqlTmpFile.Write(psqlBytes)
	if err != nil {
		log.Fatalf("failed to write psql bytes :: %v", err)
	}

	err = os.Chmod(psqlTmpFile.Name(), 0755)
	if err != nil {
		log.Fatalf("failed to change psql permissions :: %v", err)
	}

	// get combined output
	err = exec.Command(psqlTmpFile.Name(), "--version").Run()
	if err != nil {
		errorLog.Fatalf("unable to load psql dependency :: %v", err)
	}

	infoLog.Println("psql dependency loaded")

	err = exec.Command("bcp", "--version").Run()
	if err == nil {
		bcpAvailable = true
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
