package main

import (
	"encoding/binary"
	"errors"
	"log/slog"
	"math/rand"
	"time"
)

// The idea of the column store is that you store only 1 column type inside 1 page
// The cardinality is very high inside 1 column, there for the data can be compressed
// easily. This is valuable because IO trips take a lot of time.
//
// Latency Pyramide:
//			/\		*1		L1 cach
//		   /  \		*10		L2 cach
//		  /    \	*100	RAM
//		 /		\	*100000	Disk
//
// A bit of CPU cost for decompressing the page is worth the amount of IO safed.
// The trade off of compression/decompression speed <-> space gained is key.
// Facebook's Zstandard appears to be the golden standard for DBs.
//
// We talked about some of the advantages of the columnar design. One of the draw
// backs is that you now need to reconstruct the table with a collection of pages
// Good luck trying to find out which row belongs to which row from another page.
// The solution is to use fixed length.
// Another issue is updating and deletion. This will be a pain. Not sure how we
// can fix this.
//
// In this sub project I try out column storage
// I will store the sensor data again

type NotImplementedError struct {
	message string
}

func (d *NotImplementedError) Error() string {
	return d.message
}

var bm *BufferManager
var tm *TableManager

// type dbtypecollection interface {
// 	integer | smallint | tinyint
// 	toBin() []byte
// 	binLen() int16
// }

// func toBin[T dbtypecollection](x T) []byte {
// 	return x.toBin()
// }

type dbtype interface {
	toBin() []byte
	binLen() int16
	// greaterThan(dbtype) bool
}

//maybe read up on the whole iota stuff and check if that would work better for generic types

type integer uint32
type smallint uint16
type tinyint uint8
type text string

func (i integer) toBin() []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(i))
	return bytes
}
func (i integer) binLen() int16 {
	return 4
}
func (i integer) greaterThan(j integer) bool {
	return i > j
}
func (i integer) gt(x dbtype) (bool, error) {
	switch x := any(x).(type) {
	case smallint:
		return i > integer(x), nil
	case tinyint:
		return i > integer(x), nil
	case text:
		return false, errors.New("integer and text not cmparable")
	default:
		return false, errors.New("integer and ?? not cmparable")
	}
}

func (i smallint) toBin() []byte {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, uint16(i))
	return bytes
}
func (i smallint) binLen() int16 {
	return 2
}
func (i smallint) greaterThan(j smallint) bool {
	return i > j
}

func (i tinyint) toBin() []byte {
	b := make([]byte, 1)
	b[0] = uint8(i) // in Go uint8 is an alias for byte so you can use them interchanably
	return b
}
func (i tinyint) binLen() int16 {
	return 1
}
func (i tinyint) greaterThan(j tinyint) bool {
	return i > j
}

func (t text) toBin() []byte {
	return []byte(t)
}
func (t text) binLen() int16 {
	return -1
}
func (i text) greaterThan(j text) bool {
	return i > j
}

type Row struct {
	RowID       smallint // hidden ID
	SensorID    smallint // primary key: page is going to be organized around this value
	Location    text     // for this example the location will be a string that described where the location is. For test purposes this will be populated with a random string
	Timestamp   integer
	Temperature smallint
}

// type TableMeta struct {
// 	TableID uint16
// 	TableName string
// 	Columns []Column
// }

// type Column struct {
// 	ColumnID   uint8 // every column has an ID. No more than 256columns allows
// 	ColumnName string
// 	ColumnType uint8 // this type is a type
// }

// func createEmptyPage(){
// 	b:= [PAGE_SIZE]byte

// }

func randomStr() string {
	rand.Seed(time.Now().Unix())
	length := rand.Intn(10)

	ran_str := make([]byte, length)

	// Generating Random string
	for i := 0; i < length; i++ {
		ran_str[i] = byte(65 + rand.Intn(25))
	}

	// Displaying the random string
	str := string(ran_str)
	return str
}

func main() {

	slog.SetLogLoggerLevel(slog.LevelDebug)
	slog.Info("Logger initiated")

	bm = NewBufferManager()
	tm = NewTableManager()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	// var sql string
	// var err error
	// var result *QueryResult

	// sql = "CREATE TABLE sensor (sensorid smallint, location text, ts int, temperature smallint);"
	// result, err = execute_sql(sql)
	// fmt.Println(err)
	// printFormattedResponse(result)

	// sql = "INSERT INTO sensor VALUES (1, 'Amsterdam', 1, 17);"
	// result, err = execute_sql(sql)
	// fmt.Println(err)
	// printFormattedResponse(result)

	// sql = "INSERT INTO sensor VALUES (2, 'Rotterdam', 2, 17);"
	// result, err = execute_sql(sql)
	// fmt.Println(err)
	// printFormattedResponse(result)

	// sql = "INSERT INTO sensor VALUES (1, 'Amsterdam', 3, 18);"
	// result, err = execute_sql(sql)
	// fmt.Println(err)
	// printFormattedResponse(result)

	// fmt.Println("TEST 1")
	// sql = "SELECT sensorid FROM sensor where sensorid = 1 limit 10"
	// result, err = execute_sql(sql)
	// fmt.Println(err)
	// printFormattedResponse(result)

	// fmt.Println("TEST 2")
	// sql = "SELECT sensorid FROM sensor"
	// result, err = execute_sql(sql)
	// fmt.Println(err)
	// printFormattedResponse(result)

	// fmt.Println("TEST 3")
	// sql = "SELECT sensorid, location FROM sensor"
	// result, err = execute_sql(sql)
	// fmt.Println(err)
	// printFormattedResponse(result)

	// fmt.Println("TEST 4")
	// sql = "SELECT sensorid, location FROM sensor"
	// result, err = execute_sql(sql)
	// fmt.Println(err)
	// printFormattedResponse(result)

	// fmt.Println("TEST 5")
	// sql = "SELECT sensorid, location FROM sensor WHERE sensorid = 1"
	// result, err = execute_sql(sql)
	// fmt.Println(err)
	// printFormattedResponse(result)

	// fmt.Println("TEST 6")
	// sql = "SELECT location FROM sensor WHERE sensorid = 1"
	// result, err = execute_sql(sql)
	// fmt.Println(err)
	// printFormattedResponse(result)

	// fmt.Println("TEST 7")
	// sql = "SELECT location FROM sensor WHERE sensorid = 1"
	// result, err = execute_sql(sql)
	// fmt.Println(err)
	// printFormattedResponse(result)

	// fmt.Println("TEST 9: Order by")
	// sql = "SELECT ts FROM sensor order by ts"
	// result, err = execute_sql(sql)
	// fmt.Println(err)
	// printFormattedResponse(result)

	// StartPromt("randomfile")
}
