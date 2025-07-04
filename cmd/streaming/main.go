package main

import (
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/joncrlsn/dque"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/sqlpipe/sqlpipe/internal/vcs"

	"gopkg.in/yaml.v3"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	version = vcs.Version()
)

type Model struct {
	Schema *jsonschema.Schema
	Queue  *dque.DQue
}

type config struct {
	port        int
	systemsDir  string
	modelsDir   string
	queueDir    string
	segmentSize int
}

type application struct {
	config    config
	logger    *slog.Logger
	wg        sync.WaitGroup
	systemMap map[string]System
	modelMap  map[string]*Model
}

func main() {

	var cfg config
	flag.IntVar(&cfg.port, "port", 4000, "API port")
	flag.StringVar(&cfg.systemsDir, "systems-dir", "./systems", "Directory for systems configuration")
	flag.StringVar(&cfg.modelsDir, "models-dir", "./models", "Directory for models configuration")
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

	modelMap, err := createModelMap(cfg)
	if err != nil {
		logger.Error("failed to load model schemas", "error", err)
		os.Exit(1)
	}

	systemInfoMap, err := createSystemInfoMap(cfg)
	if err != nil {
		logger.Error("failed to load system configurations", "error", err)
		os.Exit(1)
	}

	app := &application{
		config:    cfg,
		logger:    logger,
		systemMap: make(map[string]System),
		modelMap:  modelMap,
	}

	serveQuitCh := make(chan struct{})

	// The server must be running to check that webhooks are valid (needed for system initialization)
	go func() {
		err = app.serve()
		if err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}
		close(serveQuitCh)
	}()

	app.setSystemMap(systemInfoMap)
	if err != nil {
		logger.Error("failed to create system map", "error", err)
		os.Exit(1)
	}

	<-serveQuitCh
	app.logger.Info("shutting down server")
}

func createSystemInfoMap(cfg config) (map[string]SystemInfo, error) {
	systemInfoMap := make(map[string]SystemInfo)

	err := filepath.Walk(cfg.systemsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error walking path %s: %w", path, err)
		}

		if !info.IsDir() && (filepath.Ext(path) == ".yaml" || filepath.Ext(path) == ".yml") {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			var infos map[string]SystemInfo
			decoder := yaml.NewDecoder(f)
			err = decoder.Decode(&infos)
			if err != nil {
				if err == io.EOF {
					// Empty YAML file, skip it
					return nil
				}
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
		return nil, fmt.Errorf("failed to walk systems dir: %w", err)
	}

	return systemInfoMap, nil
}

func (app *application) setSystemMap(systemInfoMap map[string]SystemInfo) {
	var systemMapMu sync.Mutex
	errCh := make(chan error, len(systemInfoMap))
	doneCh := make(chan struct{}, len(systemInfoMap))

	for systemName, systemInfo := range systemInfoMap {
		go func(systemName string, systemInfo SystemInfo) {
			system, err := app.NewSystem(systemInfo, app.config.port)
			if err != nil {
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

	// Collect and handle errors from errCh
	var systemInitErrs []error
	for e := range errCh {
		systemInitErrs = append(systemInitErrs, e)
	}
	if len(systemInitErrs) > 0 {
		app.logger.Error("failed to initialize one or more systems", "errors", systemInitErrs)
		os.Exit(1)
	}
}

type Location struct {
	Database string `json:"database"`
	Schema   string `json:"schema"`
	Table    string `json:"table"`
	Column   string `json:"column"`
	Object   string `json:"object"`
	Field    string `json:"field"`
}

type PropertySystemConfig struct {
	SearchKey        bool       `json:"search_key"`
	ReceiveFrom      bool       `json:"receive_from"`
	PushTo           bool       `json:"push_to"`
	RequireForCreate bool       `json:"require_for_create"`
	Locations        []Location `json:"locations"`
}

type Property struct {
	Type    string                          `json:"type"`
	Systems map[string]PropertySystemConfig `json:"systems"`
}

type SchemaRoot struct {
	Title      string              `json:"title"`
	Properties map[string]Property `json:"properties"`
}

func createModelMap(cfg config) (map[string]*Model, error) {

	jsonFiles := []string{}

	err := filepath.WalkDir(cfg.modelsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Skip directories and non-JSON files
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}
		jsonFiles = append(jsonFiles, path)
		return nil
	})
	if err != nil {
		return nil, err
	}

	modelMap := make(map[string]*Model)
	compiler := jsonschema.NewCompiler()

	systemPropertyMap := make(map[string]map[string]map[string]map[string]string)

	for _, path := range jsonFiles {

		schemaRoot := &SchemaRoot{}

		url := "file://" + filepath.ToSlash(path)
		schema, err := compiler.Compile(url)
		if err != nil {
			return nil, fmt.Errorf("compile %s: %w", path, err)
		}
		modelName := schema.Title
		if modelName == "" {
			return nil, fmt.Errorf("schema %s has no title", path)
		}

		if err := os.MkdirAll(cfg.queueDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create queue dir: %w", err)
		}

		q, err := dque.NewOrOpen(modelName, cfg.queueDir, cfg.segmentSize, func() interface{} { return &map[string]interface{}{} })
		if err != nil {
			return nil, fmt.Errorf("failed to create/open queue %s: %w", modelName, err)
		}

		modelMap[modelName] = &Model{
			Schema: schema,
			Queue:  q,
		}

		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open model file %s: %w", path, err)
		}
		defer f.Close()

		decoder := json.NewDecoder(f)
		if err := decoder.Decode(&schemaRoot); err != nil {
			return nil, fmt.Errorf("failed to decode model file %s: %w", path, err)
		}

		// system_name.object_name.model_name.key_name_from_obj.key_name_from_schema

		for propertyNameInSchema, schemaProperty := range schemaRoot.Properties {

			for systemName, system := range schemaProperty.Systems {

				if _, ok := systemPropertyMap[systemName]; !ok {
					systemPropertyMap[systemName] = make(map[string]map[string]map[string]string)
				}

				for _, location := range system.Locations {

					if location.Object != "" {
						if _, ok := systemPropertyMap[systemName][location.Object]; !ok {
							systemPropertyMap[systemName][location.Object] = make(map[string]map[string]string)
						}

						if _, ok := systemPropertyMap[systemName][location.Object][modelName]; !ok {
							systemPropertyMap[systemName][location.Object][modelName] = make(map[string]string)
						}

						systemPropertyMap[systemName][location.Object][modelName][location.Field] = propertyNameInSchema
					}
				}
			}
		}

		// pretty, err := json.MarshalIndent(schemaRoot, "", "  ")
		// if err != nil {
		// 	fmt.Printf("failed to marshal schemaRoot: %v\n", err)
		// } else {
		// 	fmt.Println(string(pretty))
		// }
	}

	pretty, err := json.MarshalIndent(systemPropertyMap, "", "  ")
	if err != nil {
		fmt.Printf("failed to marshal systemPropertyMap: %v\n", err)
	} else {
		fmt.Println(string(pretty))
	}

	return modelMap, nil
}
