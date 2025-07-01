package main

import (
	"expvar"
	"flag"
	"fmt"
	"log/slog"
	"os"
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

type yamlConfig struct {
	Systems []*systems.ConnectionInfo `yaml:"systems"`
}

type config struct {
	port        int
	yamlFile    string
	queueDir    string
	segmentSize int
}

type application struct {
	config    config
	logger    *slog.Logger
	wg        sync.WaitGroup
	systemMap map[string]systems.System
	decoder   *yaml.Decoder
}

func main() {
	var cfg config
	flag.IntVar(&cfg.port, "port", 4000, "API server port")
	flag.StringVar(&cfg.yamlFile, "config", "config.yaml", "Path to configuration file")
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

	q, err := dque.New(qName, qDir, segmentSize, ItemBuilder)
	if err != nil {
		logger.Error("failed to create queue", "error", err)
		os.Exit(1)
	}

	yamlFile, err := os.Open(cfg.yamlFile)
	if err != nil {
		logger.Error("failed to open configuration file", "error", err)
		os.Exit(1)
	}
	defer yamlFile.Close()

	parsedYamlConfig := yamlConfig{}
	decoder := yaml.NewDecoder(yamlFile)
	err = decoder.Decode(&parsedYamlConfig)
	if err != nil {
		logger.Error("failed to decode configuration file", "error", err)
		os.Exit(1)
	}

	systemMap := make(map[string]systems.System)

	for _, systemInfo := range parsedYamlConfig.Systems {
		system, err := systems.NewSystem(*systemInfo)
		if err != nil {
			logger.Error("failed to connect to system", "error", err, "system", systemInfo.Name)
		} else {
			systemMap[systemInfo.Name] = system
		}
	}

	app := &application{
		config:    cfg,
		logger:    logger,
		systemMap: systemMap,
		decoder:   decoder,
	}

}
