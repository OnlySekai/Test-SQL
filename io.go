package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type QueryFile struct {
	fileName string
	content  string
}

func (q *QueryFile) getFileName() string {
	return strings.Split(q.fileName, ".")[0]
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
var sqlFolder = TestConfig.SQLDir
var sqlTestFolder = TestConfig.SQLTestDir

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

func (q *QueryFile) GetTempTableName() string {
	fileName := strings.Split(q.fileName, ".")[0]
	return "table_" + fileName
}

func (q *QueryFile) CreateTempTable(db *sql.DB) error {
	createTable := "CREATE TEMPORARY TABLE " + q.GetTempTableName() + " AS \n" + q.content
	_, err := db.Exec(createTable)
	if err != nil {
		return err
	}
	// print the create table name ok
	fmt.Printf("Create temp table %s ok\n", q.fileName)
	return nil
}

func (q *QueryFile) DropTempTable(db *sql.DB) error {
	_, err := db.Exec("DROP TEMPORARY TABLE " + q.GetTempTableName())
	if err != nil {
		return err
	}
	// print the drop table name ok
	fmt.Printf("Drop temp table %s ok\n", q.fileName)
	return nil
}

func (q *QueryFile) GetNumberOfRows(db *sql.DB) (int64, error) {
	rows, err := db.Query("SELECT COUNT(*) FROM " + q.GetTempTableName())
	if err != nil {
		return -1, err
	}
	var count int64
	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return -1, err
		}
	}
	return count, nil
}

func (q *QueryFile) GetTestSql() (*QueryFile, error) {
	// Tạo đường dẫn tới file SQL trong thư mục sqlTestFolder
	testFilePath := filepath.Join(sqlTestFolder, q.fileName)

	// Đọc nội dung của file SQL
	content, err := os.ReadFile(testFilePath)
	if err != nil {
		return nil, err
	}

	// Tạo một QueryFile mới với nội dung đã đọc
	newQueryFile := &QueryFile{
		fileName: "test_" + q.fileName,
		content:  string(content),
	}

	return newQueryFile, nil
}

func GetUnionCountQueryFromTemp(q1, q2 *QueryFile, db *sql.DB) (int64, error) {
	//tạo câu truy vấn láy tất cả và union 2 bảng tạm, không có count
	unionQuery := "SELECT * FROM " + q1.GetTempTableName() + " UNION SELECT * FROM " + q2.GetTempTableName()
	countQuery := "SELECT COUNT(*) FROM (" + unionQuery + ") AS count"
	rows, err := db.Query(countQuery)
	if err != nil {
		return -1, err
	}
	var count int64
	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return -1, err
		}
	}
	return count, nil

}
