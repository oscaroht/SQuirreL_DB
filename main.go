package main

import (
	"encoding/binary"
	"errors"
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
	binary.LittleEndian.PutUint32(bytes, uint32(i))
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
	binary.LittleEndian.PutUint16(bytes, uint16(i))
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
	// fmt.Println(str)
	return str
}

func main() {

	bm = NewBufferManager()
	tm = NewTableManager()

	// var table []Row

	// for t := range 1000 {
	// 	for i := range 100 {
	// 		r := Row{SensorID: smallint(i), Location: text(randomStr()), Timestamp: integer(1701 + t)}
	// 		table = append(table, r)
	// 	}
	// }

	// columns := []Column{{ColumnName: "RowID", ColumnType: "int"}, // uint8(1)}, // 16 bit int
	// 	{ColumnName: "SensorID", ColumnType: "smallint"},    //,uint8(1)},    // 16 bit int
	// 	{ColumnName: "Location", ColumnType: "text"},        // uint8(2)},    // str
	// 	{ColumnName: "Timestamp", ColumnType: "int"},        // uint8(3)},   // 32 bit int
	// 	{ColumnName: "Temperature", ColumnType: "smallint"}, // uint8(1)}} // 16 bit int
	// }

	// t := tm.CreateTable("sensor_tbl", columns)
	// bm.bufferNewTable(t)

	// rowNm := 0
	// for t := range 1000 {
	// 	for i := range 100 {
	// 		rowNm++
	// 		r := Row{RowID: smallint(rowNm), SensorID: smallint(uint16(i)), Location: text(randomStr()), Timestamp: integer(1701 + t), Temperature: smallint(rand.Intn(30))}
	// 		for pageID, col := range []string{"RowID", "SensorID", "Location", "Timestamp", "Temperature"} {
	// 			p := bm.getPage(PageID(pageID))

	// 			var tup Tuple
	// 			switch {
	// 			case col == "RowID":
	// 				tup = Tuple{RowID: uint32(rowNm), Value: r.RowID}
	// 			case col == "SensorID":
	// 				tup = Tuple{RowID: uint32(rowNm), Value: r.SensorID}
	// 			case col == "Location":
	// 				tup = Tuple{RowID: uint32(rowNm), Value: r.Location}
	// 			case col == "Timestamp":
	// 				tup = Tuple{RowID: uint32(rowNm), Value: r.Timestamp}
	// 			case col == "Temperature":
	// 				tup = Tuple{RowID: uint32(rowNm), Value: r.Temperature}
	// 			}
	// 			p.appendTuple(tup)
	// 		}
	// 	}
	// }

	// sql := "SELECT sensorid FROM sensor_tbl where sensorid = 1 limit 10"
	// sql := "CREATE TABLE sensor (sensorid smallint, location text, ts int, temperature smallint);"
	// sql := "INSERT INTO sensor VALUES (1, 'Amsterdam', 1, 17);"

	// tuples, err := execute_sql(sql)
	// fmt.Printf("Tuples: %v, errors: %v", tuples, err)

	StartPromt("randomfile")
}