package main

import (
	"database/sql"
	"log"
	"sync"
)

func Test() error {
	print("pass\n")
	return nil
}

func RunSingle(db *sql.DB, qf *QueryFile) error {
	return qf.ExecuteSQL(db)
}

func RunMultiple(db *sql.DB, qfs []*QueryFile) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(qfs)) // Buffered channel to store errors

	for _, qf := range qfs {
		wg.Add(1)

		go func(qf *QueryFile) {
			defer wg.Done()
			if err := qf.ExecuteSQL(db); err != nil {
				log.Printf("Error executing %s: %v", qf.fileName, err)
				errChan <- err // Indicate failure
			} else {
				errChan <- err // Indicate success
			}
		}(qf)
	}
	wg.Wait() // Wait for all goroutines to finish
	close(errChan)

	// Check if any query failed
	for err := range errChan {
		return err
	}
	return nil
}
