package storage


import (
	"os"
	"fmt"
	"path/filepath"
	"sync"
	"time"
	"log"
	"strings"
	"strconv"
	"slices"
)

const (
	CompactionInterval = time.Minute * 1
)

/*
WALManager is used to manage wal files and also to do the compaction of wal files

Wal files are stored in the wal directory and are named like wal-1.wal, wal-2.wal, etc.
The wal files are created when a new write operation is performed.
The wal files are compacted when the compaction interval is reached.
The wal files are deleted when the compaction is performed.

*/
type WALManager struct{
	stopChan chan struct{}
	walFiles map[string]*WAL
	currentWalFile *WAL
	currentWALFileNumber int64
	mu sync.RWMutex
	walDir string
	compactionInProgress bool
	store *KVStore
}


func NewWALManager(walDir string) (*WALManager, error) {
	walm := &WALManager{
		walDir: walDir,
		walFiles: make(map[string]*WAL),
		currentWalFile: nil,
		currentWALFileNumber: 0,
		compactionInProgress: false,
		stopChan: make(chan struct{}),
		store: NewStore(),
	}

	go walm.runCompaction()
	return walm, nil
}

func (walm *WALManager) Start() error {
	// On startup, read all of the existing wal files and parse them into memory
	walm.mu.Lock()
	defer walm.mu.Unlock()

	walFiles, err := os.ReadDir(walm.walDir)
	if err != nil {
		return err
	}
	
	if len(walFiles) == 0 {
		log.Printf("No existing WAL files found, creating new one")
		walm.currentWALFileNumber++
		walm.currentWalFile, err = NewWAL(filepath.Join(walm.walDir, fmt.Sprintf("wal-%d.wal", walm.currentWALFileNumber)))
		if err != nil {
			return err
		}
		walm.walFiles[fmt.Sprintf("wal-%d.wal", walm.currentWALFileNumber)] = walm.currentWalFile
	} else {
		log.Printf("files are there")
		maxIndex := int64(0)
		for _, walFile := range walFiles {
			log.Printf("walFile: %v", walFile)
			walm.walFiles[walFile.Name()], err = NewWAL(filepath.Join(walm.walDir, walFile.Name()))
			if err != nil {
				log.Printf("walFile: %v", walFile)
				log.Print("Error Opening file from walDir")
			}
			log.Printf("walFile: %v", walFile)
			index := getIndexFromName(walFile.Name())
			if index > maxIndex {
				maxIndex = index
			}
			log.Printf("walFile: %v", walFile)
		}
		log.Printf("4")
		walm.currentWALFileNumber = maxIndex
		walm.currentWalFile = walm.walFiles[fmt.Sprintf("wal-%d.wal", walm.currentWALFileNumber)]
	}

	walm.BootstrapWAL()

	return nil
}

func (walm *WALManager) BootstrapWAL() {
	log.Printf("5")

	log.Println("bootstraping the wal data into memory")

	filenumslist := []int64{}
	for k, _ := range walm.walFiles {
		filenumslist = append(filenumslist, getIndexFromName(k))
	}
	slices.Sort(filenumslist)

	for _, k := range filenumslist {
		fileName := fmt.Sprintf("wal-%d.wal", k)
		walEntries, err := walm.walFiles[fileName].Retrieve(walm.walDir)
		if err != nil {
			log.Fatalf("Error retrieving wal records from wal file")
			return
		}

		for _, walEntry := range walEntries {
			walm.store.ExecuteWALEntry(walEntry)
		}
	}
	log.Printf("printing the bootstrapped values")
	walm.store.PrintContents()
}

func getIndexFromName(name string) int64 {
	// match expression wal-{}.wal
	s := strings.TrimPrefix(name, "wal-")
	s = strings.TrimSuffix(s, ".wal")
	n, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("Unexpected  wal fil : %v", name)
		return 0
	}
	return int64(n)
}

// Start the background goroutine to perform compaction
func (walm *WALManager) runCompaction() {

	compactionTicker := time.NewTicker(CompactionInterval)
	for {
		select {
		case <- compactionTicker.C:
			walm.performCompaction()
		case <- walm.stopChan:
			compactionTicker.Stop()
			return
		}
	}
}

func (walm *WALManager) performCompaction() {
	walm.mu.Lock()
	defer walm.mu.Unlock()

	if walm.compactionInProgress {
		log.Fatalf("Compaction is already in progress: Unexpected state")
		return
	}
	walm.compactionInProgress = true

	compactWALFileNumber := walm.currentWALFileNumber

	// We will change the current wal file to the next one
	walm.currentWALFileNumber++
	nextWALFilePath := filepath.Join(walm.walDir, fmt.Sprintf("wal-%d.wal", walm.currentWALFileNumber))
	nextWAL, err := NewWAL(nextWALFilePath)
	if err != nil {
		log.Fatalf("Failed to create new WAL file: %v", err)
		return
	}
	walm.currentWalFile = nextWAL
	walm.walFiles[fmt.Sprintf("wal-%d.wal", walm.currentWALFileNumber)] = nextWAL

	log.Printf("Performing compaction")
	compactWAL := walm.walFiles[fmt.Sprintf("wal-%d.wal", compactWALFileNumber)]
	compactWALEntries, err := compactWAL.Retrieve(walm.walDir)
	if err != nil {
		log.Fatalf("Failed to retrieve replay WAL entries: %v", err)
		return
	}

	// Now we will replay the WAL entries to the new current WAL file
	// Go through the drop the unncessary WAL entries
	// We don't have to update the store with the replayed WAL entries because the database is already updated with the previous WAL entries
	// We will just write the replayed WAL entries to the new current WAL file
	relevantWALEntries := []*WALEntry{}
	finalWALEntries := make(map[string]*WALEntry)
	for _, compactWALEntry := range compactWALEntries {
		if compactWALEntry.Op == "set" {
			finalWALEntries[compactWALEntry.Key] = compactWALEntry
		} else if compactWALEntry.Op == "delete" {
			finalWALEntries[compactWALEntry.Key] = compactWALEntry
			// TODO:We will have to ideally use the tombstone logic here to delete the key from the database
		}
	}
	for _, finalWALEntry := range finalWALEntries {
		relevantWALEntries = append(relevantWALEntries, finalWALEntry)
	}
	err = compactWAL.RewriteFile(relevantWALEntries)
	if err != nil {
		log.Fatalf("Failed to rewrite replay WAL file: %v", err)
		return
	}

	log.Printf("Compaction completed")
}

func (walm *WALManager) Stop() {
	close(walm.stopChan)
}

func (walm *WALManager) WriteToWAL(entry *WALEntry) error {
	walm.mu.RLock()
	defer walm.mu.RUnlock()

	if walm.currentWalFile == nil {
		return fmt.Errorf("current WAL file is not initialized")
	}
	log.Printf("Writing to WAL file: %s", walm.currentWalFile.file.Name())

	return walm.currentWalFile.Append(entry)
}