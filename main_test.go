package main

import (
	"log/slog"
	"testing"
)

func TestMain(t *testing.T) {

	slog.SetLogLoggerLevel(slog.LevelDebug)

	// var sql string
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

		{sql: "INSERT INTO sensor VALUES (4, 'London', 4, 15);", want: QueryResult{table: [][]dbtype{}}},
		{sql: "INSERT INTO sensor VALUES (2, 'Rotterdam', 4, 20);", want: QueryResult{table: [][]dbtype{}}},
		{sql: "INSERT INTO sensor VALUES (5, 'Paris', 5, 21);", want: QueryResult{table: [][]dbtype{}}},

		{sql: "SELECT sensorid FROM sensor WHERE sensorid=1 ORDER BY location ASC", want: QueryResult{table: [][]dbtype{{integer(1), integer(1)}}}},

		{sql: "SELECT location FROM sensor WHERE sensorid = 1 AND ts = 1", want: QueryResult{table: [][]dbtype{{text("Amsterdam")}}}},
		{sql: "SELECT location FROM sensor WHERE temperature = 20 AND ts = 4", want: QueryResult{table: [][]dbtype{{text("Rotterdam")}}}},
	}

	var cell dbtype
	for _, test := range tests {
		slog.Info("-------------------------------------------------------------------------------------------------")
		slog.Info(test.sql)
		result, err = execute_sql(test.sql)
		if err != nil {
			t.Fatalf("Error occured while execution. %v", err)
		}
		if len(result.table) != len(test.want.table) {
			t.Fatalf("Response for query %v: %v not equal to %v", test.sql, result.table, test.want.table)
		}
		if len(test.want.table) > 0 && len(result.table[0]) != len(test.want.table[0]) {
			t.Fatalf("Response for query %v: %v not equal to %v", test.sql, result.table, test.want.table)
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

}
