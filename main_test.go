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

	tests := []struct {
		sql  string
		want QueryResult
	}{
		{sql: "CREATE TABLE sensor (sensorid int, location text, ts int, temperature smallint);", want: QueryResult{table: [][]dbtype{}}},
		{sql: "INSERT INTO sensor VALUES (1, 'Amsterdam', 1, 17);", want: QueryResult{table: [][]dbtype{}}},
		{sql: "INSERT INTO sensor VALUES (2, 'Rotterdam', 2, 17);", want: QueryResult{table: [][]dbtype{}}},
		{sql: "INSERT INTO sensor VALUES (1, 'Amsterdam', 3, 18);", want: QueryResult{table: [][]dbtype{}}},
		{sql: "SELECT sensorid FROM sensor", want: QueryResult{table: [][]dbtype{{integer(1), integer(2), integer(1)}}}},
		{sql: "SELECT sensorid FROM sensor ORDER BY sensorid", want: QueryResult{table: [][]dbtype{{integer(1), integer(1), integer(2)}}}},
		{sql: "SELECT sensorid FROM sensor ORDER BY sensorid LIMIT 2", want: QueryResult{table: [][]dbtype{{integer(1), integer(1)}}}},
		{sql: "SELECT location FROM sensor", want: QueryResult{table: [][]dbtype{{text("Amsterdam"), text("Rotterdam"), text("Amsterdam")}}}},
		{sql: "SELECT location FROM sensor ORDER BY location ASC", want: QueryResult{table: [][]dbtype{{text("Amsterdam"), text("Amsterdam"), text("Rotterdam")}}}},
		{sql: "SELECT location FROM sensor ORDER BY location DESC", want: QueryResult{table: [][]dbtype{{text("Rotterdam"), text("Amsterdam"), text("Amsterdam")}}}},
		{sql: "SELECT temperature FROM sensor ORDER BY temperature DESC LIMIT 1", want: QueryResult{table: [][]dbtype{{smallint(18)}}}},
	}

	var cell dbtype
	for _, test := range tests {
		slog.Info(test.sql)
		result, err = execute_sql(test.sql)
		if err != nil {
			t.Fatalf("Error occured while execution. %v", err)
		}
		for r, _ := range result.table {
			for c, _ := range result.table[0] {
				cell = result.table[r][c]
				if cell != test.want.table[r][c] {
					t.Fatalf("Response for query %v: %v not equal to %v", test.sql, result.table, test.want.table)
				}
			}
		}
	}

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
