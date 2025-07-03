package main

import (
	"expvar"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/sqlpipe/sqlpipe/internal/systems"
	"github.com/sqlpipe/sqlpipe/internal/vcs"

	"gopkg.in/yaml.v3"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	version = vcs.Version()
)

type config struct {
	port           int
	systemsYamlDir string
	modelsYamlDir  string
	queueDir       string
	segmentSize    int
	stripe         struct {
		listen         bool
		apiKey         string
		endpointSecret string
	}
}

type application struct {
	config    config
	logger    *slog.Logger
	systemMap map[string]systems.System
	wg        sync.WaitGroup
}

func main() {

	var cfg config
	flag.IntVar(&cfg.port, "port", 4000, "API port")
	flag.StringVar(&cfg.systemsYamlDir, "systems-yaml-dir", "./systems", "Directory for systems YAML configuration")
	flag.StringVar(&cfg.modelsYamlDir, "models-yaml-dir", "./models", "Directory for models YAML configuration")
	flag.StringVar(&cfg.queueDir, "queue-dir", "/tmp", "Directory for queue files")
	flag.IntVar(&cfg.segmentSize, "segment-size", 100, "Size of each segment in the queue")
	displayVersion := flag.Bool("version", false, "Display version and exit")

	flag.Parse()

	cfg.stripe.apiKey = os.Getenv("STRIPE_API_KEY")

	if *displayVersion {
		fmt.Printf("Version:\t%s\n", version)
		os.Exit(0)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	expvar.NewString("version").Set(version)
	expvar.Publish("goroutines", expvar.Func(func() any {
		return runtime.NumGoroutine()
	}))
	expvar.Publish("timestamp", expvar.Func(func() any {
		return time.Now().Unix()
	}))

	systemInfoMap := make(map[string]systems.SystemInfo)

	err := filepath.Walk(cfg.systemsYamlDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Error("failed to access path", "error", err, "path", path)
			return err
		}
		if !info.IsDir() && (filepath.Ext(path) == ".yaml" || filepath.Ext(path) == ".yml") {
			f, err := os.Open(path)
			if err != nil {
				logger.Error("failed to open system yaml file", "error", err, "file", path)
				return err
			}
			defer f.Close()

			var infos map[string]systems.SystemInfo
			decoder := yaml.NewDecoder(f)
			err = decoder.Decode(&infos)
			if err != nil {
				if err == io.EOF {
					// Empty YAML file, skip it gracefully
					return nil
				}
				logger.Error("failed to decode system configuration file", "error", err, "file", path)
				return err
			}
			for name, info := range infos {
				info.Name = name
				systemInfoMap[info.Name] = info
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("failed to walk systems yaml dir", "error", err)
		os.Exit(1)
	}

	app := &application{
		config:    cfg,
		logger:    logger,
		systemMap: make(map[string]systems.System),
	}

	handler, router := app.routes()

	var systemMapMu sync.Mutex
	errCh := make(chan error, len(systemInfoMap))
	doneCh := make(chan struct{}, len(systemInfoMap))

	for systemName, systemInfo := range systemInfoMap {
		go func(systemName string, systemInfo systems.SystemInfo) {
			system, err := systems.NewSystem(systemInfo, cfg.port, router)
			if err != nil {
				logger.Error("failed to connect to system", "system", systemName, "error", err)
				errCh <- err
			} else {
				systemMapMu.Lock()
				app.systemMap[systemInfo.Name] = system
				systemMapMu.Unlock()
			}
			doneCh <- struct{}{}
		}(systemName, systemInfo)
	}

	// Wait for all goroutines to finish
	for i := 0; i < len(systemInfoMap); i++ {
		<-doneCh
	}

	close(errCh)
	close(doneCh)

	// Collect and handle errors from errCh
	var systemInitErrs []error
	for e := range errCh {
		systemInitErrs = append(systemInitErrs, e)
	}
	if len(systemInitErrs) > 0 {
		logger.Error("failed to initialize one or more systems", "errors", systemInitErrs)
		os.Exit(1)
	}

	err = app.serve(handler)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}
