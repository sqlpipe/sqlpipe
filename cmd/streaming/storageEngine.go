package main

import (
	"sync"
)

type storageEngine struct {
	safeIndexMap map[string]int64
	indexMapMu   sync.RWMutex
	safeObjects  []any
	objectsMu    sync.RWMutex
}

func newStorageEngine() (*storageEngine, error) {
	storageEngine := &storageEngine{
		safeIndexMap: make(map[string]int64),
		safeObjects:  make([]any, 0),
	}
	return storageEngine, nil
}

func (s *storageEngine) setSafeIndexMap(key string, index int64) {
	s.indexMapMu.Lock()
	defer s.indexMapMu.Unlock()
	s.safeIndexMap[key] = index
}

func (s *storageEngine) getSafeIndexMap(key string) (int64, bool) {
	s.indexMapMu.RLock()
	defer s.indexMapMu.RUnlock()
	index, exists := s.safeIndexMap[key]
	return index, exists
}

func (s *storageEngine) addSafeObject(obj any, objType string) {
	s.objectsMu.Lock()
	defer s.objectsMu.Unlock()
	payload := map[string]any{
		objType: obj,
	}
	s.safeObjects = append(s.safeObjects, payload)
}

func (s *storageEngine) getSafeObjectsFromIndex(index int64) []any {
	s.objectsMu.RLock()
	defer s.objectsMu.RUnlock()

	if index < 0 || index >= int64(len(s.safeObjects)) {
		return nil
	}

	// Return a slice of safeObjects starting from the given index
	return s.safeObjects[index:]
}

func (s *storageEngine) printAllContents() {
	s.indexMapMu.RLock()
	defer s.indexMapMu.RUnlock()
	s.objectsMu.RLock()
	defer s.objectsMu.RUnlock()

	println("safeIndexMap contents:")
	for k, v := range s.safeIndexMap {
		println("  Key:", k, "Value:", v)
	}

	println("safeObjects contents:")
	for i, obj := range s.safeObjects {
		println("  Index:", i, "Value:", obj)
	}
}
