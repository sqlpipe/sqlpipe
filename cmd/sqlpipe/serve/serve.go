package serve

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"database/sql"
	"errors"
	"expvar"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/golangcollege/sessions"

	"github.com/calmitchell617/sqlpipe/internal/data"
	"github.com/calmitchell617/sqlpipe/internal/jsonLog"
	"github.com/spf13/cobra"
)

var (
	Serve = &cobra.Command{
		Use:   "serve",
		Short: "Starts a SQLPipe server.",
		Run:   serve,
	}

	cfg config

	buildTime string
	version   string

	displayVersion bool

	secret string

	tlsConfig = &tls.Config{
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
)

type config struct {
	port int
	db   struct {
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
	createAdmin      bool
	adminCredentials struct {
		username string
		password string
	}
}

type application struct {
	logger *jsonLog.Logger

	config        config
	models        data.Models
	wg            sync.WaitGroup
	session       *sessions.Session
	templateCache map[string]*template.Template

	tlsConfig *tls.Config
}

func init() {
	Serve.Flags().IntVar(&cfg.port, "port", 9000, "The port SQLPipe will run on. Default 9000")

	Serve.Flags().StringVar(&cfg.db.dsn, "dsn", "", "Database backend connection string")

	Serve.Flags().IntVar(&cfg.db.maxOpenConns, "max-connections", 100, "The port SQLPipe will run on. Default 9000")
	Serve.Flags().IntVar(&cfg.db.maxIdleConns, "max-idle-connections", 5, "The port SQLPipe will run on. Default 9000")
	Serve.Flags().StringVar(&cfg.db.maxIdleTime, "max-idle-time", "5m", "Database backend connection string")

	Serve.Flags().Float64Var(&cfg.limiter.rps, "limiter-rps", 100, "Rate limiter maximum requests per second")
	Serve.Flags().IntVar(&cfg.limiter.burst, "limiter-burst", 200, "Rate limiter maximum burst")
	Serve.Flags().BoolVar(&cfg.limiter.enabled, "limiter-enabled", true, "Enable rate limiter")

	Serve.Flags().BoolVar(&cfg.createAdmin, "create-admin", false, "Create admin user")
	Serve.Flags().StringVar(&cfg.adminCredentials.username, "admin-username", "", "Admin username")
	Serve.Flags().StringVar(&cfg.adminCredentials.password, "admin-password", "", "Admin password")

	Serve.Flags().BoolVar(&displayVersion, "version", false, "Display SQLpipe version")

	Serve.Flags().StringVar(&secret, "secret", "", "Secret key")
}

func serve(cmd *cobra.Command, args []string) {
	checkDisplayVersion()

	logger := jsonLog.New(os.Stdout, jsonLog.LevelInfo)

	db, err := openDB(cfg)
	if err != nil {
		logger.PrintFatal(err, nil)
	}
	defer db.Close()
	logger.PrintInfo("database connection pool established", nil)

	publishMetrics(db)

	templateCache, err := newTemplateCache()
	if err != nil {
		logger.PrintFatal(err, nil)
	}

	if secret == "" {
		randomCharacters(32)
	}
	session := sessions.New([]byte(secret))
	session.Lifetime = 12 * time.Hour
	session.Secure = true

	app := &application{
		logger:        logger,
		config:        cfg,
		tlsConfig:     tlsConfig,
		session:       session,
		models:        data.NewModels(db),
		templateCache: templateCache,
	}

	if cfg.createAdmin {
		app.createAdminUser(
			cfg.adminCredentials.username,
			cfg.adminCredentials.password,
		)
	}

	err = app.serve()
	if err != nil {
		logger.PrintFatal(err, nil)
	}
}

func checkDisplayVersion() {
	if displayVersion {
		fmt.Printf("Version:\t%s\n", version)
		fmt.Printf("Build time:\t%s\n", buildTime)
		os.Exit(0)
	}
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

func publishMetrics(db *sql.DB) {
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

func randomCharacters(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789!@#$%^&*()_+=-][}{;:/?.,<>`~")
	b := make([]rune, 32)

	for i := range b {
		bytes := make([]byte, 1)
		_, err := rand.Read(bytes)
		if err != nil {
			fmt.Print("unable to generate random characters for session security, please enter a 32 character string with the --secret flag")
			os.Exit(1)
		}

		randomInt := int(bytes[0])
		lettersLen := len(letters)
		randomIndex := randomInt % lettersLen
		b[i] = letters[randomIndex]
	}

	fmt.Println(string(b))
	return string(b)
}
