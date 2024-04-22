package main

import (
	"log/slog"
	"os"
)

type ColumnID uint16
type Columns []ColumnID
type TableID uint16
type Offset uint16

type DiskManager struct {
	TableMap  map[TableID]Columns
	ColumnMap map[ColumnID]PageID
	PageMap   map[PageID]Offset

	// ReadPage(PageID) (*Page, error)
	// WritePage(*Page) error
	// AllocatePage() *PageID
	// DeallocatePage(PageID)
}

func (d *DiskManager) readPage(pid PageID) *Page {
	offset := d.PageMap[pid]

	f, err := os.OpenFile("db", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		slog.Error("Unable to open/create file.", "Error", err)
		panic(err)
	}
	defer f.Close()     // after this function is done the file is closed
	fi, err := f.Stat() // when we are going to read we need to know how much we need to read. This will define the size of the buffer
	if err != nil {
		slog.Error("Unable to get file info")
	}
	slog.Debug("File", "size", fi.Size())

	maxBytes := min(int64(PAGE_SIZE), fi.Size()) // get only this page (the first page) or the EOF. Which ever comes first.

	bytes := make([]byte, maxBytes)
	n2, err := f.ReadAt(bytes, int64(offset)) // call ReadAt instead of Read because after the Write action we are at the EOF. ReadAt(.., 0) specifies we need to move to the beginning
	if err != nil {
		slog.Error("Error", "message", err)
		panic(err)
	}
	slog.Debug("Read", "bytes", n2)

	return deserializePage(bytes)

}

func (d *DiskManager) writePage(p *Page) {
	offset := d.PageMap[p.PageID]
	b := p.serialize()

	f, err := os.OpenFile("db", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close() // after this function is done the file is closed

	n, err := f.WriteAt(b, int64(offset))
	if err != nil {
		slog.Error("Error", "message", err)
		panic(err)
	}
	slog.Debug("Wrote", "bytes", n) // print number of bytes written
}
