package main

import (
	"log"
	// "math/rand"
	"time"

	"github.com/indkumar8999/store-p/storage"
)


func main() {
	
	walDir := "wal"
	walManager, err := storage.NewWALManager(walDir)
	if err != nil {
		log.Fatalf("Failed to create WAL manager: %v", err)
		return
	}

	err = walManager.Start()
	if err != nil {
		log.Fatalf("Failed to start WAL manager: %v", err)
		return
	}

	defer walManager.Stop()


	// operations := []string {"set", "get", "delete", "expire", "tombstone"}
	// keys := []string {"key1", "key2", "key3", "key4", "key5"}
	// values := []string {"value1", "value2", "value3", "value4", "value5"}
	// timeToLive := []int {10, 20, 30, 40, 50}
	// tombstone := []bool {true, false, true, false, true}

	// for i := 0; i < 10; i++ {
	// 	operation := operations[rand.Intn(len(operations))]
	// 	key := keys[rand.Intn(len(keys))]
	// 	value := values[rand.Intn(len(values))]
	// 	timeToLive := timeToLive[rand.Intn(len(timeToLive))]
	// 	tombstone := tombstone[rand.Intn(len(tombstone))]

	// 	entry := &storage.WALEntry{
	// 		Op: operation,
	// 		Key: key,
	// 		Value: storage.Value{
	// 			Type: storage.StringType,
	// 			StrValue: value,
	// 			Version: int64(i),
	// 			TTL: int64(timeToLive),
	// 			Tombstone: tombstone,
	// 		},
	// 	}

	// 	err = walManager.WriteToWAL(entry)
	// 	if err != nil {
	// 		log.Fatalf("Failed to write to WAL: %v", err)
	// 		return
	// 	}
	// }

	time.Sleep(time.Second * 100)

}