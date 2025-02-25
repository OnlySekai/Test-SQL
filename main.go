package main

import (
	"database/sql"
	"flag"
	"fmt"
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
	case "acc":
		{
			testAcc(db)
		}
	default:
		log.Fatalf("Unknown mode: %s", *mode)
	}

	log.Println("Test done")
}

func testNormalTTL(db *sql.DB) {
	sqlFiles := GetQueries(TestConfig.SQLDir)
	for _, query := range sqlFiles {
		TestTTL(func() error {
			return RunSingle(db, &query)
		}, query.getFileName())
		breakTime()
	}
}

func testParTTL(db *sql.DB) {
	// get sub dir in result
	subDirs, err := os.ReadDir(TestConfig.SQLDir)
	if err != nil {
		log.Fatalf("Failed to read directory: %v", err)
	}
	for _, subDir := range subDirs {
		if !subDir.IsDir() {
			continue
		}
		sqlFiles := GetQueries(filepath.Join(TestConfig.SQLDir, subDir.Name()))
		sqlFilePtrs := make([]*QueryFile, len(sqlFiles))
		for i, sqlFile := range sqlFiles {
			sqlFilePtrs[i] = &sqlFile
		}
		TestTTL(func() error {
			return RunMultiple(db, sqlFilePtrs)
		}, subDir.Name()+"_par")
		breakTime()
	}
}

func testNormalStress(db *sql.DB) {
	sqlFiles := GetQueries(
		TestConfig.SQLDir,
	)
	for _, query := range sqlFiles {
		StressTest(func() error {
			return RunSingle(db, &query)
		}, query.getFileName())
		breakTime()
	}
}

func testParStress(db *sql.DB) {
	subDirs, err := os.ReadDir(TestConfig.SQLDir)
	if err != nil {
		log.Fatalf("Failed to read directory: %v", err)
	}
	for _, subDir := range subDirs {
		if !subDir.IsDir() {
			continue
		}
		sqlFiles := GetQueries(filepath.Join(TestConfig.SQLDir, subDir.Name()))
		sqlFilePtrs := make([]*QueryFile, len(sqlFiles))
		for i, sqlFile := range sqlFiles {
			sqlFilePtrs[i] = &sqlFile
		}
		StressTest(func() error {
			return RunMultiple(db, sqlFilePtrs)
		}, subDir.Name()+"_par")
		breakTime()
	}
}

func testAcc(db *sql.DB) {
	sqlFiles := GetQueries(TestConfig.SQLDir)
	for _, query := range sqlFiles {
		fmt.Printf("-------START TEST ACC %s ---------\n", query.fileName)
		err := TestAcc(db, &query)
		if err != nil {
			log.Print(err)
		}
		fmt.Printf("--------------Test ACC %s DONE-------------\n", query.fileName)
	}
}
