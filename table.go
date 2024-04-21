package main

import (
	"fmt"
	"strconv"
)

type TableDescription struct {
	TableID     uint16
	TableName   string
	Columns     []Column
	SerialRowID uint32
}

func (t TableDescription) getColumnByName(name string) (Column, bool) {
	fmt.Printf("Check if %v matches any of columns %v", t.Columns, name)
	for _, c := range t.Columns {
		fmt.Printf("Check if %v matches %v\n", c.ColumnName, name)
		if c.ColumnName == name {
			return c, true
		}
	}
	return Column{}, false
}

type Column struct {
	ColumnID   uint8 // every column has an ID. No more than 256columns allows
	ColumnName string
	ColumnType string // this type is a type
	PageIDs    []PageID
}

type DBType struct {
	name      string
	bitlength int // -1 for var length
}

type TableManager struct {
	TableMap     map[string]*TableDescription // table name to table
	tableIDCount uint16
}

func NewTableManager() *TableManager {
	return &TableManager{TableMap: map[string]*TableDescription{}, tableIDCount: 0}
}

func (tm *TableManager) getTableByName(name string) *TableDescription {
	x, found := tm.TableMap[name]
	if !found {
		fmt.Printf("TABLE %v does not exist.", name)
	}
	return x
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
	fmt.Printf("Insert value %v in table %v\n", val, t.TableName)
	c := t.Columns[colIdx]
	fmt.Printf("Columns found: %v\n", c)
	fmt.Printf("Pages for this column %v\n", c.PageIDs)
	pageid := c.PageIDs[len(c.PageIDs)-1]
	fmt.Printf("Page id %v\n", pageid)
	page := bm.getPage(pageid)
	var tupleVal dbtype
	switch typeint {
	case 0:
		tupleVal = text(val)
	case 1:
		i, _ := strconv.Atoi(val)
		tupleVal = smallint(i)
	}
	fmt.Printf("Create new tuple with type %v\n", typeint)
	tup := Tuple{RowID: rowid, Value: tupleVal}
	fmt.Printf("Append tuple\n")
	page.appendTuple(tup)

}

func (tm *TableManager) CreateTable(name string, columns []Column) *TableDescription {
	t := TableDescription{TableID: tm.tableIDCount + 1, TableName: name, Columns: columns}
	tm.TableMap[name] = &t
	tm.tableIDCount++
	return &t
}
