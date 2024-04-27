package main

import (
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
	var tupleVal dbtype
	switch typeint {
	case 0:
		tupleVal = text(val)
	case 1:
		i, _ := strconv.Atoi(val)
		tupleVal = smallint(i)
	}
	slog.Debug("Create new tuple with", "type", typeint)
	tup := Tuple{RowID: rowid, Value: tupleVal}
	slog.Debug("Append tuple\n")
	page.appendTuple(tup)

}

func (tm *TableManager) CreateTable(name string, columns []Column) *TableDescription {
	t := TableDescription{TableID: tm.tableIDCount + 1, TableName: name, Columns: columns}
	tm.TableMap[name] = &t
	tm.tableIDCount++
	return &t
}
