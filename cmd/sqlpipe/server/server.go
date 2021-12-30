package server

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"expvar"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/calmitchell617/sqlpipe/internal/jsonLog"
	"github.com/calmitchell617/sqlpipe/internal/mailer"
	"github.com/calmitchell617/sqlpipe/internal/models/postgresql"
	"github.com/golangcollege/sessions"
	"github.com/spf13/cobra"
)

var (
	Serve = &cobra.Command{
		Use:   "server",
		Short: "Starts a SQLPipe server.",
		Run:   serve,
	}

	cfg config

	buildTime string
	version   string

	displayVersion bool
)

const contextKeyIsAuthenticated = contextKey("isAuthenticated")

type config struct {
	port   int
	env    string
	secret string
	init   struct {
		createAdmin bool
		username    string
		email       string
		password    string
	}
	db struct {
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
	logger        *jsonLog.Logger
	session       *sessions.Session
	templateCache map[string]*template.Template

	config config
	models postgresql.Models
	mailer mailer.Mailer
	wg     sync.WaitGroup

	tlsConfig *tls.Config
}

func init() {
	Serve.Flags().IntVar(&cfg.port, "port", 9000, "The port SQLPipe will run on. Default 9000")
	Serve.Flags().StringVar(&cfg.env, "env", "", "Environment (development|staging|production)")
	Serve.Flags().StringVar(&cfg.secret, "secret", "", "Secret key")

	Serve.Flags().StringVar(&cfg.db.dsn, "dsn", "", "Database backend connection string")

	Serve.Flags().IntVar(&cfg.db.maxOpenConns, "max-connections", 100, "The port SQLPipe will run on. Default 9000")
	Serve.Flags().IntVar(&cfg.db.maxIdleConns, "max-idle-connections", 5, "The port SQLPipe will run on. Default 9000")
	Serve.Flags().StringVar(&cfg.db.maxIdleTime, "max-idle-time", "5m", "Database backend connection string")

	Serve.Flags().Float64Var(&cfg.limiter.rps, "limiter-rps", 100, "Rate limiter maximum requests per second")
	Serve.Flags().IntVar(&cfg.limiter.burst, "limiter-burst", 200, "Rate limiter maximum burst")
	Serve.Flags().BoolVar(&cfg.limiter.enabled, "limiter-enabled", true, "Enable rate limiter")

	Serve.Flags().StringVar(&cfg.smtp.host, "smtp-host", "", "SMTP host")
	Serve.Flags().IntVar(&cfg.smtp.port, "smtp-port", 25, "SMTP port")
	Serve.Flags().StringVar(&cfg.smtp.username, "smtp-username", "", "SMTP username")
	Serve.Flags().StringVar(&cfg.smtp.password, "smtp-password", "", "SMTP password")
	Serve.Flags().StringVar(&cfg.smtp.sender, "smtp-sender", "", "SMTP sender")

	Serve.Flags().BoolVar(&cfg.init.createAdmin, "create-admin", false, "Create admin user")
	Serve.Flags().StringVar(&cfg.init.username, "admin-username", "", "Admin username")
	Serve.Flags().StringVar(&cfg.init.email, "admin-email", "", "Admin email")
	Serve.Flags().StringVar(&cfg.init.password, "admin-password", "", "Admin password")

	Serve.Flags().BoolVar(&displayVersion, "version", false, "SMTP sender")

	Serve.Flags().StringSliceVar(&cfg.cors.trustedOrigins, "cors-trusted-origins", []string{}, "Trusted CORS origins (comma separated)")
}

func serve(cmd *cobra.Command, args []string) {

	var err error

	if displayVersion {
		fmt.Printf("Version:\t%s\n", version)
		fmt.Printf("Build time:\t%s\n", buildTime)
		os.Exit(0)
	}

	logger := jsonLog.New(os.Stdout, jsonLog.LevelInfo)

	db, err := openDB(cfg)
	if err != nil {
		logger.PrintFatal(err, nil)
	}
	defer db.Close()
	logger.PrintInfo("database connection pool established", nil)

	expvar.NewString("version").Set(version)
	expvar.Publish("goroutines", expvar.Func(func() interface{} {
		return runtime.NumGoroutine()
	}))
	expvar.Publish("database", expvar.Func(func() interface{} {
		return db.Stats()
	}))
	expvar.Publish("timestamp", expvar.Func(func() interface{} {
		return time.Now().Unix()
	}))

	templateCache, err := newTemplateCache()
	if err != nil {
		logger.PrintFatal(err, nil)
	}

	session := sessions.New([]byte(cfg.secret))
	session.Lifetime = 12 * time.Hour
	session.Secure = true

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
		logger:        logger,
		session:       session,
		templateCache: templateCache,
		config:        cfg,
		tlsConfig:     tlsConfig,
		models:        postgresql.NewModels(db),
		mailer:        mailer.New(cfg.smtp.host, cfg.smtp.port, cfg.smtp.username, cfg.smtp.password, cfg.smtp.sender),
	}

	if cfg.init.createAdmin {
		app.registerInitialUser(
			cfg.init.username,
			cfg.init.email,
			cfg.init.password,
		)
	}

	err = app.serve()
	if err != nil {
		logger.PrintFatal(err, nil)
	}
}

func (app *application) serve() error {

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", app.config.port),
		Handler:      app.routes(),
		IdleTimeout:  time.Minute,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		TLSConfig:    app.tlsConfig,
	}

	shutdownError := make(chan error)

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		s := <-quit

		app.logger.PrintInfo("shutting down server", map[string]string{
			"signal": s.String(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := srv.Shutdown(ctx)
		if err != nil {
			shutdownError <- err
		}

		app.logger.PrintInfo("completing background tasks", map[string]string{
			"addr": srv.Addr,
		})

		app.wg.Wait()
		shutdownError <- nil
	}()

	app.logger.PrintInfo("starting server", map[string]string{
		"addr": srv.Addr,
		"env":  app.config.env,
	})

	err := srv.ListenAndServeTLS("./tls/cert.pem", "./tls/key.pem")
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	err = <-shutdownError
	if err != nil {
		return err
	}

	app.logger.PrintInfo("stopped server", map[string]string{
		"addr": srv.Addr,
	})

	return nil
}

func openDB(cfg config) (*sql.DB, error) {
	db, err := sql.Open("postgres", cfg.db.dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(cfg.db.maxOpenConns)

	db.SetMaxIdleConns(cfg.db.maxIdleConns)

	duration, err := time.ParseDuration(cfg.db.maxIdleTime)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxIdleTime(duration)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return db, nil
}
