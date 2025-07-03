package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/joncrlsn/dque"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// LoadSchemasDir reads every .json in dirPath, compiles it via jsonschema,
// and returns a map keyed by schema.Title (or filename if Title is empty).
func LoadSchemasDir(dirPath string, queueMap map[string]*dque.DQue, segmentSize int, queueDir string) (map[string]*jsonschema.Schema, error) {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)

	// Walk to collect JSON file paths
	jsonFiles := []string{}
	err := filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}
		jsonFiles = append(jsonFiles, path)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Compile schemas and build map
	compiled := make(map[string]*jsonschema.Schema, len(jsonFiles))
	for _, path := range jsonFiles {
		url := "file://" + filepath.ToSlash(path)
		sch, err := compiler.Compile(url)
		if err != nil {
			return nil, fmt.Errorf("compile %s: %w", path, err)
		}
		key := sch.Title
		if key == "" {
			key = strings.TrimSuffix(filepath.Base(path), ".json")
		}
		compiled[key] = sch

		if err := os.MkdirAll(queueDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create queue dir: %w", err)
		}

		// Check if queue exists, create if not. Name should be same as object (key)
		if _, exists := queueMap[key]; !exists {
			q, err := dque.NewOrOpen(key, queueDir, segmentSize, func() interface{} { return &map[string]interface{}{} })
			if err != nil {
				return nil, fmt.Errorf("failed to create/open queue %s: %w", key, err)
			}
			queueMap[key] = q
		}
	}

	return compiled, nil
}
