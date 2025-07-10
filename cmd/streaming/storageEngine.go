package main

import (
	"fmt"
	"sync"
)

type Object struct {
	Type      string         `json:"type"`
	Operation string         `json:"operation"`
	Payload   map[string]any `json:"payload"`
}

type storageEngine struct {
	safeIndexMap map[string]int64
	indexMapMu   sync.RWMutex
	safeObjects  []Object
	objectsMu    sync.RWMutex
}

func newStorageEngine() (*storageEngine, error) {
	storageEngine := &storageEngine{
		safeIndexMap: make(map[string]int64),
		safeObjects:  make([]Object, 0),
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

func (s *storageEngine) addSafeObject(object Object) {
	s.objectsMu.Lock()
	defer s.objectsMu.Unlock()
	s.safeObjects = append(s.safeObjects, object)
}

func (s *storageEngine) getSafeObjectsFromIndex(index int64) []Object {
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
		println("  Index:", i, "Value:", fmt.Sprintf("%v", obj))
	}
}
