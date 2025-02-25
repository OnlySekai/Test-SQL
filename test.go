package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

var MAX_STRESS = TestConfig.MaxStress
var TEST_TTL_TIMES = TestConfig.TestTtlTimes
var INIT_STRESS_TEST = TestConfig.InitStressTest
var X_STRESS = TestConfig.XStress
var WARMING_TIME = TestConfig.WarmingTime
var COOL_DOWN_SECOND = TestConfig.CoolDownSecond
var BREAK_TIME_SECOND = TestConfig.BreakTimeSecond
var RESULTS_DIR = TestConfig.ResultTest

// Function to pause execution for the cool-down period
func coolDown() {
	time.Sleep(time.Duration(COOL_DOWN_SECOND) * time.Second)
}

func breakTime() {
	time.Sleep(time.Duration(BREAK_TIME_SECOND) * time.Second)
}

func warming(f func() error) {
	for i := 0; i < WARMING_TIME; i++ {
		f()
		coolDown()
	}
}

func TestTTL(f func() error, id string) {
	log.Printf("-----------START TTL %v -------------", id)
	warming(f)
	times := make([]int64, TEST_TTL_TIMES)
	for i := 0; i < TEST_TTL_TIMES; i++ {
		startTime := time.Now()
		err := f()
		if err != nil {
			log.Fatalln(err)
		}
		endTime := time.Now()
		times[i] = endTime.Sub(startTime).Milliseconds()
		coolDown()
	}
	var totalDuration int64 = 0
	for i := 0; i < TEST_TTL_TIMES; i++ {
		totalDuration += times[i]
	}
	avgDuration := totalDuration / int64(TEST_TTL_TIMES)
	log.Printf("ID: %s, Average Duration: %d ms\n", id, avgDuration)
	logData := map[string]interface{}{
		"id":              id,
		"averageDuration": avgDuration,
		"times":           times,
	}

	file, err := os.Create(fmt.Sprintf("%s/ttl_%s.json", RESULTS_DIR, id))
	if err != nil {
		log.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(logData); err != nil {
		log.Printf("Error encoding JSON to file: %v\n", err)
	}
	log.Printf("-----------END TTL %v -------------", id)
}

type StressTestRs struct {
	Id            string  `json:"id"`
	Attempts      int     `json:"attempts"`
	Duration      int64   `json:"duration"`
	EachDurations []int64 `json:"eachDurations"`
}

func StressTest(f func() error, id string) {
	log.Printf("--------START STRESS %v --------------\n", id)
	k := INIT_STRESS_TEST
	results := []StressTestRs{}
	for {
		if k > MAX_STRESS {
			break
		}
		warming(f)
		startTime := time.Now()
		var eachDurations, fail = stressTest(f, id, k)
		if !fail {
			duration := time.Since(startTime).Milliseconds()
			result := StressTestRs{
				Id:            id,
				Attempts:      k,
				Duration:      duration,
				EachDurations: eachDurations,
			}
			results = append(results, result)
			log.Printf("ID: %s, Attempts: %d, Duration: %d ms\n", id, k, duration)
			k *= X_STRESS
			breakTime()
		} else {
			break
		}
	}
	file, err := os.OpenFile(fmt.Sprintf("%s/stress_%s.json", RESULTS_DIR, id), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(results); err != nil {
		log.Printf("Error encoding JSON to file: %v\n", err)
	}
	log.Printf("--------END STRESS %v --------------\n", id)
}

func stressTest(f func() error, id string, k int) ([]int64, bool) {
	var durations = make([]int64, k)
	var wg sync.WaitGroup
	var fail = 0
	mu := sync.Mutex{}
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			start := time.Now()
			err := f()
			if err != nil {
				log.Println(err)
				mu.Lock()
				fail++
				mu.Unlock()
			}
			durations[index] = time.Since(start).Milliseconds()
		}(i)
	}
	wg.Wait()
	log.Printf("Stress %s Test: %d/%d fails executions\n", id, fail, k)
	return durations, fail > 0
}

type CorrectRs struct {
	Name       string `json:"name"`
	TestCount  int64  `json:"testCount"`
	Count      int64  `json:"count"`
	UnionCount int64  `json:"unionCount"`
	Sucess     bool   `json:"sucess"`
}

func TestAcc(db *sql.DB, query *QueryFile) error {
	rs := CorrectRs{}
	err := query.CreateTempTable(db)
	defer query.DropTempTable(db)
	if err != nil {
		return err
	}
	rs.Count, err = query.GetNumberOfRows(db)
	if err != nil {
		return err
	}
	log.Printf("Number of rows: %d\n", rs.Count)
	testQuery, err := query.GetTestSql()
	if err != nil {
		return err
	}
	err = testQuery.CreateTempTable(db)
	defer testQuery.DropTempTable(db)
	if err != nil {
		return err
	}
	rs.TestCount, err = testQuery.GetNumberOfRows(db)
	if err != nil {
		return err
	}
	log.Printf("Number of rows: %d\n", rs.TestCount)
	if rs.TestCount == rs.Count {
		rs.UnionCount, err = GetUnionCountQueryFromTemp(testQuery, query, db)
		if err != nil {
			return err
		}
		log.Printf("Number of rows: %d\n", rs.UnionCount)
	}
	rs.Sucess = rs.TestCount == rs.Count && rs.Count == rs.UnionCount
	file, err := os.OpenFile(fmt.Sprintf("%s/correct_%s.json", RESULTS_DIR, query.getFileName()), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(rs); err != nil {
		log.Printf("Error encoding JSON to file: %v\n", err)
	}
	return nil
}
