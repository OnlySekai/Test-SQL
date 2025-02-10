package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/go-sql-driver/mysql"
)

const sqlFolder = "result"

type QueryFile struct {
	fileName string
	content  string
}

func GetQueries(folder string) []QueryFile {
	files, err := getFiles(folder)
	if err != nil {
		log.Fatalf("Failed to get files: %v", err)
	}

	var queries []QueryFile
	for _, path := range files {
		content, err := os.ReadFile(path)
		if err != nil {
			log.Fatalf("Failed to read file %s: %v", path, err)
			continue
		}
		queries = append(queries, QueryFile{filepath.Base(path), string(content)})
	}
	return queries
}

func getFiles(folder string) ([]string, error) {
	files, err := os.ReadDir(folder)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %v", err)
	}

	var sqlFiles []string
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".sql" {
			sqlFiles = append(sqlFiles, filepath.Join(folder, file.Name()))
		}
	}
	return sqlFiles, nil
}

var dbUser = TestConfig.DBUser
var dbPassword = TestConfig.DBPassword
var dbHost = TestConfig.DBHost
var dbPort = TestConfig.DBPort
var dbName = TestConfig.DBName

// GetConn establishes a database connection
func GetConn() (*sql.DB, error) {
	log.Println("Connecting")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
	log.Println("Successfully connected")
	return sql.Open("mysql", dsn)
}

func (q *QueryFile) ExecuteSQL(db *sql.DB) error {
	rows, err := db.Query(q.content)
	if err != nil {
		return err
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Iterate through the rows
	for rows.Next() {
		// Scan the result into the column pointers
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return err
		}

		// Create a map to store the row data
		rowData := make(map[string]interface{})
		for i, col := range cols {
			var v interface{}
			val := values[i]

			// Type switch to handle different types
			switch val.(type) {
			case []byte:
				v = string(val.([]byte))
			default:
				v = val
			}

			rowData[col] = v
		}
	}
	return nil
}
