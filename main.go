package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"path/filepath"
)

func main() {
	// Define flags
	mode := flag.String("mode", "ttl", "Mode of operation: stress or ttl (default: ttl)")
	execution := flag.String("exe", "normal", "Execution type: parallelize or normal (default: normal)")

	// Parse flags
	flag.Parse()

	// Get database connection
	db, err := GetConn()
	if err != nil {
		log.Fatalln(err)
		return
	}
	defer db.Close()

	// Execute based on flags
	switch *mode {
	case "stress":
		if *execution == "para" {
			testParStress(db)
		} else {
			testNormalStress(db)
		}
	case "ttl":
		if *execution == "para" {
			testParTTL(db)
		} else {
			testNormalTTL(db)
		}
	default:
		log.Fatalf("Unknown mode: %s", *mode)
	}

	log.Println("Test done")
}

func testNormalTTL(db *sql.DB) {
	sqlFiles := GetQueries("./result")
	for _, query := range sqlFiles {
		TestTTL(func() error {
			return RunSingle(db, &query)
		}, query.fileName)
		breakTime()
	}
}

func testParTTL(db *sql.DB) {
	// get sub dir in result
	subDirs, err := os.ReadDir("./result")
	if err != nil {
		log.Fatalf("Failed to read directory: %v", err)
	}
	for _, subDir := range subDirs {
		if !subDir.IsDir() {
			continue
		}
		sqlFiles := GetQueries(filepath.Join("./result", subDir.Name()))
		sqlFilePtrs := make([]*QueryFile, len(sqlFiles))
		for i, sqlFile := range sqlFiles {
			sqlFilePtrs[i] = &sqlFile
		}
		TestTTL(func() error {
			return RunMultiple(db, sqlFilePtrs)
		}, subDir.Name()+"par")
		breakTime()
	}
}

func testNormalStress(db *sql.DB) {
	sqlFiles := GetQueries("./result")
	for _, query := range sqlFiles {
		StressTest(func() error {
			return RunSingle(db, &query)
		}, query.fileName)
		breakTime()
	}
}

func testParStress(db *sql.DB) {
	subDirs, err := os.ReadDir("./result")
	if err != nil {
		log.Fatalf("Failed to read directory: %v", err)
	}
	for _, subDir := range subDirs {
		if !subDir.IsDir() {
			continue
		}
		sqlFiles := GetQueries(filepath.Join("./result", subDir.Name()))
		sqlFilePtrs := make([]*QueryFile, len(sqlFiles))
		for i, sqlFile := range sqlFiles {
			sqlFilePtrs[i] = &sqlFile
		}
		StressTest(func() error {
			return RunMultiple(db, sqlFilePtrs)
		}, subDir.Name()+"par")
		breakTime()
	}
}
