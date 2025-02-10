package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

type Config struct {
	MaxStress       int    `json:"MAX_STRESS"`
	TestTtlTimes    int    `json:"TEST_TTL_TIMES"`
	InitStressTest  int    `json:"INIT_STRESS_TEST"`
	XStress         int    `json:"X_STRESS"`
	WarmingTime     int    `json:"WARMING_TIME"`
	CoolDownSecond  int    `json:"COOL_DOWN_SECOND"`
	BreakTimeSecond int    `json:"BREAK_TIME_SECOND"`
	DBUser          string `json:"DB_USER"`
	DBPassword      string `json:"DB_PASSWORD"`
	DBHost          string `json:"DB_HOST"`
	DBPort          string `json:"DB_PORT"`
	DBName          string `json:"DB_NAME"`
}

func getTestConfig() Config {
	// Open the JSON file
	file, err := os.Open("config.json")
	if err != nil {
		log.Fatalf("Failed to open config file: %s", err)
	}
	defer file.Close()

	// Read the file content
	byteValue, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Failed to read config file: %s", err)
	}

	// Unmarshal the JSON content into the Config struct
	var config Config
	err = json.Unmarshal(byteValue, &config)
	if err != nil {
		log.Fatalf("Failed to parse config file: %s", err)
	}
	return config
}

var TestConfig = getTestConfig()
