package storage

import (
	"sync"
	"log"
)

type KVStore struct {
	dataMap map[string]interface{}

	datamu sync.Mutex
}


func NewStore() *KVStore {
	return &KVStore{
		dataMap: make(map[string]interface{}),
	}
}

func (store *KVStore) Get(key string) interface{} error {
	store.datamu.Lock()
	defer store.datamu.Unlock()

	value, ok := store.dataMap[key]
	if !ok {
		log.Printf("key %v not found in the database", key)
		return nil
	}
	return value
}

func (store *KVStore) Set(key string, value interface{}) error {
	store datamu.Lock()
	defer store.datamu.Unlock()

	store.dataMap[key] = value
	return nil
}