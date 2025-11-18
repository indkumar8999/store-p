package storage

import (
	"bufio"
	"os"
	"sync"

	"encoding/json"
)

type ValueType string

const (
	StringType ValueType = "string"
	IntType ValueType = "int"
)

type WAL struct {
	mu sync.Mutex
	file *os.File
	writer *bufio.Writer
}

type Value struct {
	Type ValueType `json:"type"`
	StrValue string `json:"str_value,omitempty"`
	IntValue int64 `json:"int_value,omitempty"`

	Version int64 `json:"version"`
	TTL int64 `json:"ttl,omitempty"`
	Tombstone bool `json:"tombstone,omitempty"`
}

type WALEntry struct {
	Op string `json:"op"`
	Key string `json:"key"`
	Value Value `json:"value,omitempty"`
}

func NewWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND | os.O_CREATE | os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL {
		file: file,
		writer: bufio.NewWriter(file),
	}, nil
}

func(wal *WAL) Append(entry *WALEntry) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	_, err = wal.writer.Write(append(data, '\n'))
	if err != nil {
		return err
	}

	err = wal.writer.Flush()
	if err != nil {
		return err
	}

	return wal.file.Sync()
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Flush amy remaining data to the file
	err := wal.writer.Flush()
	if err != nil {
		return err
	}
	err = wal.file.Sync()
	if err != nil {
		return err
	}
	// And close the file
	return wal.file.Close()
}