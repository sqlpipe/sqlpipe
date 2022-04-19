package main

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/jsonLog"

	"github.com/coreos/etcd/clientv3"
)

var (
	ServeCmd = &cobra.Command{
		Use:   "serve",
		Short: "Run independent API server",
		Run:   runServe,
	}
	buildTime string
	version   string
	cfg       config
	err       error
	etcd      *clientv3.Client
)

type config struct {
	port           int
	env            string
	displayVersion bool
	cluster        bool
	etcd           struct {
		timeout   int
		endpoints []string
	}
	limiter struct {
		enabled bool
		rps     float64
		burst   int
	}
	cors struct {
		trustedOrigins []string
	}
}

type application struct {
	config config
	logger *jsonLog.Logger
	models data.Models
	wg     sync.WaitGroup
}

func init() {
	ServeCmd.Flags().StringSliceVar(&cfg.cors.trustedOrigins, "cors-trusted-origins", []string{}, "Trusted CORS origins, comma separated no spaces")
	ServeCmd.Flags().StringVar(&cfg.env, "env", "development", "Environment (development|staging|production)")
	ServeCmd.Flags().BoolVar(&cfg.cluster, "etcd-cluster", false, "Join a SQLpipe cluster with an etcd backend (default false)")
	ServeCmd.Flags().StringSliceVar(&cfg.etcd.endpoints, "etcd-endpoints", []string{}, "etcd endpoints, comma separated no spaces")
	ServeCmd.Flags().IntVar(&cfg.etcd.timeout, "etcd-timeout", 3, "Timeout in seconds for connecting to etcd")
	ServeCmd.Flags().IntVar(&cfg.port, "port", 9000, "API server port")
	ServeCmd.Flags().BoolVar(&cfg.displayVersion, "version", false, "Display version and exit")
}

func runServe(cmd *cobra.Command, args []string) {
	if cfg.displayVersion {
		fmt.Printf("Version:\t%s\n", version)

		fmt.Printf("Build time:\t%s\n", buildTime)
		os.Exit(0)
	}

	logger := jsonLog.New(os.Stdout, jsonLog.LevelInfo)

	if cfg.cluster {
		// clientv3.SetLogger(grpclog.NewLoggerV2(os.Stderr, os.Stderr, os.Stderr))

		etcd, err = clientv3.New(clientv3.Config{
			Endpoints:   cfg.etcd.endpoints,
			DialTimeout: 5 * time.Second,
		})

		if err != nil {
			logger.PrintFatal(
				errors.New("unable to connect to etcd"),
				map[string]string{"err": err.Error(), "endpoints": fmt.Sprint(cfg.etcd.endpoints)},
			)
		}
		defer etcd.Close() // make sure to close the client
	}

	expvar.NewString("version").Set(version)

	expvar.Publish("goroutines", expvar.Func(func() interface{} {
		return runtime.NumGoroutine()
	}))

	expvar.Publish("timestamp", expvar.Func(func() interface{} {
		return time.Now().Unix()
	}))

	app := &application{
		config: cfg,
		logger: logger,
	}

	if cfg.cluster {
		app.models = data.NewModels(etcd)
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
	}

	shutdownError := make(chan error)

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		s := <-quit

		app.logger.PrintInfo("caught signal", map[string]string{
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

	err := srv.ListenAndServe()
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
