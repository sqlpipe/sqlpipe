package main

import "sync"

type SafeMap struct {
	mu sync.RWMutex
	m  map[string]Transfer
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		m: make(map[string]Transfer),
	}
}

func (sm *SafeMap) Set(key string, value Transfer) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[key] = value
}

func (sm *SafeMap) Get(key string) (Transfer, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	val, ok := sm.m[key]
	return val, ok
}

func (sm *SafeMap) GetEntireMap() map[string]Transfer {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.m
}

func (sm *SafeMap) Delete(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.m, key)
}

var transferMap = NewSafeMap()
