package main

import (
	"crypto/tls"
	"expvar"
	"flag"
	"fmt"
	"html/template"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/jsonLog"
)

var (
	buildTime string
	version   string
)

type contextKey string

const contextKeyIsAuthenticated = contextKey("isAuthenticated")

type config struct {
	port   int
	env    string
	secret string
	db     struct {
		dsn          string
		maxOpenConns int
		maxIdleConns int
		maxIdleTime  string
	}
	limiter struct {
		rps     float64
		burst   int
		enabled bool
	}
	smtp struct {
		host     string
		port     int
		username string
		password string
		sender   string
	}
	cors struct {
		trustedOrigins []string
	}
}

type application struct {
	logger *jsonLog.Logger
	// session       *sessions.Session
	templateCache map[string]*template.Template

	config config
	// models postgresql.Models
	// mailer mailer.Mailer
	wg sync.WaitGroup

	tlsConfig *tls.Config
}

func main() {

	var err error
	var cfg config

	flag.IntVar(&cfg.port, "port", 9000, "API server port")
	flag.StringVar(&cfg.env, "env", "development", "Environment (development|staging|production)")
	flag.StringVar(&cfg.secret, "secret", "", "Secret key")

	flag.StringVar(&cfg.db.dsn, "db-dsn", "", "PostgreSQL DSN")

	flag.IntVar(&cfg.db.maxOpenConns, "db-max-open-conns", 100, "PostgreSQL max open connections")
	flag.IntVar(&cfg.db.maxIdleConns, "db-max-idle-conns", 5, "PostgreSQL max idle connections")
	flag.StringVar(&cfg.db.maxIdleTime, "db-max-idle-time", "15m", "PostgreSQL max connection idle time")

	flag.Float64Var(&cfg.limiter.rps, "limiter-rps", 100, "Rate limiter maximum requests per second")
	flag.IntVar(&cfg.limiter.burst, "limiter-burst", 200, "Rate limiter maximum burst")
	flag.BoolVar(&cfg.limiter.enabled, "limiter-enabled", true, "Enable rate limiter")

	flag.StringVar(&cfg.smtp.host, "smtp-host", "", "SMTP host")
	flag.IntVar(&cfg.smtp.port, "smtp-port", 25, "SMTP port")
	flag.StringVar(&cfg.smtp.username, "smtp-username", "", "SMTP username")
	flag.StringVar(&cfg.smtp.password, "smtp-password", "", "SMTP password")
	flag.StringVar(&cfg.smtp.sender, "smtp-sender", "", "SMTP sender")

	flag.Func("cors-trusted-origins", "Trusted CORS origins (space separated)", func(val string) error {
		cfg.cors.trustedOrigins = strings.Fields(val)
		return nil
	})

	displayVersion := flag.Bool("version", false, "Display version and exit")

	flag.Parse()

	if *displayVersion {
		fmt.Printf("Version:\t%s\n", version)
		fmt.Printf("Build time:\t%s\n", buildTime)
		os.Exit(0)
	}

	logger := jsonLog.New(os.Stdout, jsonLog.LevelInfo)

	// db, err := openDB(cfg)
	// if err != nil {
	// logger.PrintFatal(err, nil)
	// }
	// defer db.Close()
	// logger.PrintInfo("database connection pool established", nil)

	expvar.NewString("version").Set(version)
	expvar.Publish("goroutines", expvar.Func(func() interface{} {
		return runtime.NumGoroutine()
	}))
	// expvar.Publish("database", expvar.Func(func() interface{} {
	// return db.Stats()
	// }))
	expvar.Publish("timestamp", expvar.Func(func() interface{} {
		return time.Now().Unix()
	}))

	// templateCache, err := newTemplateCache()
	// if err != nil {
	// logger.PrintFatal(err, nil)
	// }

	// session := sessions.New([]byte(*secret))
	// session.Lifetime = 12 * time.Hour
	// session.Secure = true

	tlsConfig := &tls.Config{
		PreferServerCipherSuites: true,
		CurvePreferences:         []tls.CurveID{tls.X25519, tls.CurveP256},
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},
	}

	app := &application{
		logger: logger,
		// session:       session,
		// templateCache: templateCache,
		config:    cfg,
		tlsConfig: tlsConfig,
		// models:        postgresql.NewModels(db),
		// mailer:        mailer.New(cfg.smtp.host, cfg.smtp.port, cfg.smtp.username, cfg.smtp.password, cfg.smtp.sender),
	}

	err = app.serve()
	if err != nil {
		logger.PrintFatal(err, nil)
	}
}
