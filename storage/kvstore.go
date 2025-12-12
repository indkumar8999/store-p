package storage

import (
	"fmt"
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

func (store *KVStore) Get(key string) (interface{}, error) {
	store.datamu.Lock()
	defer store.datamu.Unlock()

	value, ok := store.dataMap[key]
	if !ok {
		log.Printf("key %v not found in the database", key)
		return nil, fmt.Errorf("key %v not found in the database", key)
	}
	return value, nil
}

func (store *KVStore) Set(key string, value interface{}) error {
	store.datamu.Lock()
	defer store.datamu.Unlock()

	store.dataMap[key] = value
	return nil
}

func (store *KVStore) Delete(key string) error {
	store.datamu.Lock()
	defer store.datamu.Unlock()

	_, ok := store.dataMap[key]
	if !ok {
		log.Printf("key %v not found in the database", key)
		return fmt.Errorf("key %v not found in the database", key)
	}
	delete(store.dataMap, key)
	return nil

}


func (store *KVStore) ExecuteWALEntry(wale *WALEntry) error {
	switch wale.Op {
	case "set":
		value, err := getValue(wale)
		if err != nil {
			return err
		}
		store.Set(wale.Key, value)
		return nil
	case "get":
		return nil
	case "delete":
		store.Delete(wale.Key)
		return nil
	default:
		log.Printf("unexpected operation in the wal entry")
		return nil
	}
}

func getValue(wale *WALEntry) (interface{}, error) {
	value := wale.Value
	switch value.Type {
	case "string":
		return value.StrValue, nil
	case "int":
		return value.IntValue, nil
	default:
		return nil, fmt.Errorf("Unexpected type")
	}
}

func (store *KVStore)  PrintContents() { 
	for k, v := range store.dataMap {
		log.Printf("key %v  : Value  %v", k, v)
	}
}