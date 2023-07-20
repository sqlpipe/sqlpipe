package main

import (
	"expvar"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

var (
	version = ProgramVersion()
)

type config struct {
	port    int
	workDir string
}

type application struct {
	config   config
	errorLog *log.Logger
	infoLog  *log.Logger
	wg       sync.WaitGroup
}

func main() {
	var cfg config

	flag.IntVar(&cfg.port, "port", 9000, "API server port")
	flag.StringVar(&cfg.workDir, "work-dir", "", "Working directory")

	displayVersion := flag.Bool("version", false, "Display version and exit")

	flag.Parse()

	if *displayVersion {
		fmt.Printf("Version:\t%s\n", version)
		os.Exit(0)
	}

	expvar.NewString("version").Set(version)

	expvar.Publish("goroutines", expvar.Func(func() any {
		return runtime.NumGoroutine()
	}))

	expvar.Publish("timestamp", expvar.Func(func() any {
		return time.Now().Unix()
	}))

	app := &application{
		config:   cfg,
		infoLog:  log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime),
		errorLog: log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile),
	}

	err := app.serve()
	if err != nil {
		app.errorLog.Fatalln(err)
	}
}
