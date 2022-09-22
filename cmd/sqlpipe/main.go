package main

import (
	"expvar"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sqlpipe/sqlpipe/internal/jsonlog"
	"github.com/sqlpipe/sqlpipe/internal/vcs"
	"github.com/sqlpipe/sqlpipe/pkg"

	_ "github.com/lib/pq"
)

var (
	version = vcs.Version()
)

type config struct {
	port    int
	token   string
	secure  bool
	limiter struct {
		enabled bool
		rps     float64
		burst   int
	}
}

type application struct {
	config config
	logger *jsonlog.Logger
	wg     sync.WaitGroup
}

func main() {
	var cfg config

	flag.IntVar(&cfg.port, "port", 4000, "API server port")

	flag.BoolVar(&cfg.limiter.enabled, "limiter-enabled", true, "Enable rate limiter")
	flag.Float64Var(&cfg.limiter.rps, "limiter-rps", 2, "Rate limiter maximum requests per second")
	flag.IntVar(&cfg.limiter.burst, "limiter-burst", 4, "Rate limiter maximum burst")

	flag.StringVar(&cfg.token, "token", "", "Auth token")
	flag.BoolVar(&cfg.secure, "secure", true, "Secure with an auth token")

	displayVersion := flag.Bool("version", false, "Display version and exit")

	flag.Parse()

	if *displayVersion {
		fmt.Printf("Version:\t%s\n", version)
		os.Exit(0)
	}

	if cfg.secure {
		if utf8.RuneCountInString(cfg.token) != 32 {
			fmt.Printf("Invalid or missing auth-token value (must be exactly 32 characters).\nRun with -secure=false to run without an auth token.\n")
			randomCharacters, err := pkg.RandomCharacters(32)
			if err != nil {
				fmt.Println("Unable to generate random characters")
				os.Exit(0)
			}
			fmt.Printf("Generating random characters. Your auth token is: %v\n", randomCharacters)
			cfg.token = randomCharacters
		}
	}

	logger := jsonlog.New(os.Stdout, jsonlog.LevelInfo)

	expvar.NewString("version").Set(version)

	expvar.Publish("goroutines", expvar.Func(func() any {
		return runtime.NumGoroutine()
	}))

	expvar.Publish("timestamp", expvar.Func(func() any {
		return time.Now().Unix()
	}))

	app := &application{
		config: cfg,
		logger: logger,
	}

	err := app.serve()
	if err != nil {
		logger.PrintFatal(err, nil)
	}
}
