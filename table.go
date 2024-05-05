package main

import (
	"encoding/binary"
	"strconv"

	"golang.org/x/exp/slog"
)

type TableError struct {
	msg string
}

func (d *TableError) Error() string {
	return d.msg
}

type TableDescription struct {
	TableID     uint16
	TableName   string
	Columns     []Column
	SerialRowID uint32
}

func (t TableDescription) getColumnByName(name string) (Column, error) {
	// fmt.Printf("Check if %v matches any of columns %v", t.Columns, name)
	for _, c := range t.Columns {
		// fmt.Printf("Check if %v matches %v\n", c.ColumnName, name)
		if c.ColumnName == name {
			return c, nil
		}
	}
	return Column{}, &TableError{"Column not found"}
}

func (t TableDescription) getColumnNames() []string {
	columnNames := []string{}
	for _, c := range t.Columns {
		columnNames = append(columnNames, c.ColumnName)
	}
	return columnNames
}

type Column struct {
	ColumnID   uint8 // every column has an ID. No more than 256columns allows
	ColumnName string
	ColumnType string // this type is a type
	PageIDs    []PageID
}

func (column *Column) getTupleByRowID(rowid uint32) (Tuple, error) {
	p := bm.getPage(column.PageIDs[0])
	// if err != nil{
	// 	slog.Error(err.Error())
	// 	return Tuple{}, err
	// }
	return p.Tuples[rowid], nil
}

func (column *Column) strToBytes(str string) ([]byte, error) {
	switch column.ColumnType {
	case "smallint":
		i, _ := strconv.Atoi(string(str))
		operand := make([]byte, 2)
		if i < 0 {
			return nil, &NotImplementedError{"Negative numbers not inplemented"}
		}
		binary.BigEndian.PutUint16(operand, uint16(i))
		return operand, nil
	case "int":
		i, _ := strconv.Atoi(string(str))
		operand := make([]byte, 4)
		if i < 0 {
			return nil, &NotImplementedError{"Negative numbers not inplemented"}
		}
		binary.BigEndian.PutUint32(operand, uint32(i))
		return operand, nil
	case "text":
		return []byte(str), nil
	default:
		return nil, &NotImplementedError{"Input string cannot be casted to []byte"}
	}

}

type TableManager struct {
	TableMap     map[string]*TableDescription // table name to table
	tableIDCount uint16
}

func NewTableManager() *TableManager {
	return &TableManager{TableMap: map[string]*TableDescription{}, tableIDCount: 0}
}

func (tm *TableManager) getTableByName(name string) (*TableDescription, error) {
	x, found := tm.TableMap[name]
	if !found {
		return nil, &TableError{"table does not exist."}
	}
	return x, nil
}

//	func (tm *TableManager) setTable(table Table) {
//		tm.TableMap[table.TableName] = table
//	}
func (tm *TableManager) insertIntoTable(t *TableDescription, colIdx int, rowid uint32, val string, typeint int) {
	// requests the buffermanager for a page
	// give tuples to the Page manager to insert
	// page will complain when the page is full
	// someone needs to instruct the bm to get
	// a new page
	slog.Debug("Insert ", "value", val, "table", t.TableName)
	c := t.Columns[colIdx]

	slog.Debug("Columns", "found", c)
	slog.Debug("Pages for this ", "column", c.PageIDs)
	pageid := c.PageIDs[len(c.PageIDs)-1]
	slog.Debug("", "Page id", pageid)
	page := bm.getPage(pageid)
	// var tupleVal dbtype
	var tupBytes []byte
	switch typeint {
	case 0:
		// tupleVal = text(val)
		tupBytes = []byte(val)
	case 1:
		i, _ := strconv.Atoi(val)
		switch c.ColumnType {
		case "int":
			tupBytes = make([]byte, 4)
			binary.BigEndian.PutUint32(tupBytes, uint32(i))
		case "smallint":
			tupBytes = make([]byte, 2)
			binary.BigEndian.PutUint16(tupBytes, uint16(i))
		case "tinyint":
			tupBytes = []byte{uint8(i)}
		default:
			slog.Error("Unrecognised type unable to cast to []byte{}.", "type", c.ColumnType)
		}
	}
	slog.Debug("Create new tuple with", "type", typeint)
	tup := Tuple{RowID: rowid, Value: tupBytes}
	slog.Debug("Append tuple\n")
	page.appendTuple(tup)

}

func (tm *TableManager) CreateTable(name string, columns []Column) *TableDescription {
	t := TableDescription{TableID: tm.tableIDCount + 1, TableName: name, Columns: columns}
	tm.TableMap[name] = &t
	tm.tableIDCount++
	return &t
}
