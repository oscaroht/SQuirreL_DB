package main

import (
	"fmt"
	"log/slog"
	"testing"
)

func TestMain(t *testing.T) {

	slog.SetLogLoggerLevel(slog.LevelDebug)

	var sql string
	var err error
	// var result []Tuple
	var result *QueryResult

	bm = NewBufferManager()
	tm = NewTableManager()

	sql = "CREATE TABLE sensor (sensorid smallint, location text, ts int, temperature smallint);"
	result, err = execute_sql(sql)
	fmt.Println(err)
	printFormattedResponse(result)

	sql = "INSERT INTO sensor VALUES (1, 'Amsterdam', 1, 17);"
	result, err = execute_sql(sql)
	fmt.Println(err)
	printFormattedResponse(result)

	sql = "INSERT INTO sensor VALUES (2, 'Rotterdam', 2, 17);"
	result, err = execute_sql(sql)
	fmt.Println(err)
	printFormattedResponse(result)

	sql = "INSERT INTO sensor VALUES (1, 'Amsterdam', 3, 18);"
	result, err = execute_sql(sql)
	fmt.Println(err)
	printFormattedResponse(result)

	fmt.Println("TEST 1")
	sql = "SELECT sensorid FROM sensor where sensorid = 1 limit 10"
	result, err = execute_sql(sql)
	fmt.Println(err)
	printFormattedResponse(result)

	fmt.Println("TEST 2")
	sql = "SELECT sensorid FROM sensor"
	result, err = execute_sql(sql)
	fmt.Println(err)
	printFormattedResponse(result)

	fmt.Println("TEST 3")
	sql = "SELECT sensorid, location FROM sensor"
	result, err = execute_sql(sql)
	fmt.Println(err)
	printFormattedResponse(result)

	fmt.Println("TEST 4")
	sql = "SELECT sensorid, location FROM sensor"
	result, err = execute_sql(sql)
	fmt.Println(err)
	printFormattedResponse(result)

	fmt.Println("TEST 5")
	sql = "SELECT sensorid, location FROM sensor WHERE sensorid = 1"
	result, err = execute_sql(sql)
	fmt.Println(err)
	printFormattedResponse(result)

	fmt.Println("TEST 6")
	sql = "SELECT location FROM sensor WHERE sensorid = 1"
	result, err = execute_sql(sql)
	fmt.Println(err)
	printFormattedResponse(result)

	fmt.Println("TEST 7")
	sql = "SELECT location FROM sensor WHERE sensorid = 1"
	result, err = execute_sql(sql)
	fmt.Println(err)
	printFormattedResponse(result)

	fmt.Println("TEST 9: Order by")
	sql = "SELECT ts FROM sensor order by ts"
	result, err = execute_sql(sql)
	fmt.Println(err)
	printFormattedResponse(result)

}
