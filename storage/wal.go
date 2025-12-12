package storage

import (
	"io"
	"bytes"
	"log"
	"bufio"
	"os"
	"sync"
	// "path/filepath"

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

func (wal *WAL) Retrieve(walDir string) ([]*WALEntry, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	wals := []*WALEntry{}
	walfile, err := os.Open(wal.file.Name())
	if err != nil {
		log.Printf("error opening file in read mode : %d %d with err :%v", walDir, wal.file.Name(), err)
		return nil, err
	}
	reader := bufio.NewReader(walfile)
	for {
		var walEntry *WALEntry
		bs, err := reader.ReadBytes('\n')
		if err == io.EOF {
			if len(bytes.TrimSpace(bs)) > 0 {
				walEntry, err  = ParseToWALEntry(bs)
				if err != nil {
					log.Printf("error processing last line in file: %v", wal.file.Name())
					return nil, err
				}
			}
			break
		}

		if err != nil {
			log.Fatalf("Error reading bytes from file: %v , err : %v", wal.file.Name(), err)
			return nil, err
		}
		walEntry, err  = ParseToWALEntry(bs)
		if err != nil {
			log.Printf("error processing last line in file: %v", wal.file.Name())
			return nil, err
		}
		wals = append(wals, walEntry)
	}
	return wals, nil
}

func ParseToWALEntry(instructionBytes []byte) (*WALEntry, error) {
	wal := &WALEntry{}
	err := json.Unmarshal(instructionBytes, wal)
	return wal, err
}

func (wal *WAL) RewriteFile(wale []*WALEntry) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	err := wal.file.Truncate(0)
	if err != nil {
		return err
	}
	wal.file.Seek(0, io.SeekStart)
	for _, waleEntry := range wale {
		data, err := json.Marshal(waleEntry)
		if err != nil {
			return err
		}

		_, err = wal.writer.Write(append(data, '\n'))
		if err != nil {
			return err
		}
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