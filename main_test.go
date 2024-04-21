package main

import (
	"fmt"
	"testing"
)

func TestMain(t *testing.T) {
	var sql string
	var err error
	var tuples []Tuple

	bm = NewBufferManager()
	tm = NewTableManager()

	sql = "CREATE TABLE sensor (sensorid smallint, location text, ts int, temperature smallint);"
	tuples, err = execute_sql(sql)
	fmt.Printf("Tuples: %v, errors: %v\n", tuples, err)

	sql = "INSERT INTO sensor VALUES (1, 'Amsterdam', 1, 17);"
	tuples, err = execute_sql(sql)
	fmt.Printf("Tuples: %v, errors: %v\n", tuples, err)

	sql = "INSERT INTO sensor VALUES (2, 'Rotterdam', 2, 17);"
	tuples, err = execute_sql(sql)
	fmt.Printf("Tuples: %v, errors: %v\n", tuples, err)

	sql = "INSERT INTO sensor VALUES (1, 'Amsterdam', 3, 18);"
	tuples, err = execute_sql(sql)
	fmt.Printf("Tuples: %v, errors: %v\n", tuples, err)

	fmt.Println("TEST 1")
	sql = "SELECT sensorid FROM sensor where sensorid = 1 limit 10"
	tuples, err = execute_sql(sql)
	fmt.Printf("Tuples: %v, errors: %v\n", tuples, err)

	fmt.Println("TEST 2")
	sql = "SELECT sensorid FROM sensor"
	tuples, err = execute_sql(sql)
	fmt.Printf("Tuples: %v, errors: %v\n", tuples, err)

	fmt.Println("TEST 3")
	sql = "SELECT sensorid, location FROM sensor"
	tuples, err = execute_sql(sql)
	fmt.Printf("Tuples: %v, errors: %v\n", tuples, err)

	fmt.Println("TEST 4")
	sql = "SELECT sensorid, location FROM sensor"
	tuples, err = execute_sql(sql)
	fmt.Printf("Tuples: %v, errors: %v\n", tuples, err)

	fmt.Println("TEST 5")
	sql = "SELECT sensorid, location FROM sensor WHERE sensorid = 1"
	tuples, err = execute_sql(sql)
	fmt.Printf("Tuples: %v, errors: %v\n", tuples, err)

	fmt.Println("TEST 6")
	sql = "SELECT location FROM sensor WHERE sensorid = 1"
	tuples, err = execute_sql(sql)
	fmt.Printf("Tuples: %v, errors: %v\n", tuples, err)

	fmt.Println("TEST 7")
	sql = "SELECT location FROM sensor WHERE sensorid = 1"
	tuples, err = execute_sql(sql)
	fmt.Printf("Tuples: %v, errors: %v\n", tuples, err)

	fmt.Println("TEST 8")
	sql = "CREATE TABLE sensor (sensorid smallint, location text, ts int, temperature smallint);"
	tuples, err = execute_sql(sql)
	sql = "INSERT INTO sensor VALUES (1, 'Amsterdam', 1, 17);"
	tuples, err = execute_sql(sql)
	sql = "SELECT sensorid FROM sensor where sensorid = 1 limit 10"
	tuples, err = execute_sql(sql)

}
