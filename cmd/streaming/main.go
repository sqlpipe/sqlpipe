package main

import (
	"expvar"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
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
}

// type application struct {
// 	config    config
// 	logger    *slog.Logger
// 	systemMap map[string]systems.System
// }

func main() {

	var cfg config
	flag.IntVar(&cfg.port, "port", 4000, "API server port")
	flag.StringVar(&cfg.systemsYamlDir, "systems-yaml-dir", "./systems", "Directory for systems YAML configuration")
	flag.StringVar(&cfg.modelsYamlDir, "models-yaml-dir", "./models", "Directory for models YAML configuration")
	flag.StringVar(&cfg.queueDir, "queue-dir", "/tmp", "Directory for queue files")
	flag.IntVar(&cfg.segmentSize, "segment-size", 100, "Size of each segment in the queue")
	displayVersion := flag.Bool("version", false, "Display version and exit")

	flag.Parse()

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

	systemMap := make(map[string]systems.System)

	var system systems.System
	for systemName, systemInfo := range systemInfoMap {
		system, err = systems.NewSystem(systemInfo)
		if err != nil {
			logger.Error("failed to connect to system", "system", systemName, "error", err)
		} else {
			systemMap[systemInfo.Name] = system
		}
	}

	if err != nil {
		logger.Error("failed to initialize systems")
		os.Exit(1)
	}

	// app := &application{
	// 	config:    cfg,
	// 	logger:    logger,
	// 	systemMap: systemMap,
	// }

	// for name := range app.systemMap {
	// 	fmt.Println("Connected to system:", name)
	// }

	// fmt.Println(
	// 	"heeeere",
	// )

}
