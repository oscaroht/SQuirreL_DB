package main

import (
	"encoding/binary"
	"log/slog"
	"time"
)

const PAGE_SIZE = uint16(4096)
const HEADER_SIZE = uint8(34)
const ROWIDSIZE = uint8(4) // 4byte

var VERSION = NewVersion(uint8(0), uint8(0), uint8(0))

type semanticVersion [3]uint8

func NewVersion(a uint8, b uint8, c uint8) semanticVersion {
	return [3]uint8{a, b, c}
}

func (s *semanticVersion) serialize() []byte {
	b := []byte{}
	b = append(b, s[0], s[1], s[2]) // s is an array with length 3 of uint8. uint8 is a byte so just append.
	return b
}

func deserializeSemanticVersion(b []byte) semanticVersion {
	return [3]uint8{b[0], b[1], b[2]}
}

type Tuple struct {
	RowID uint32
	Value []byte
}

func (t *Tuple) tupleToValue(typ string) (dbtype, error) {
	switch typ {
	case "tinyint":
		return tinyint(t.Value[0]), nil
	case "smallint":
		return smallint(binary.BigEndian.Uint16(t.Value)), nil
	case "int":
		return integer(binary.BigEndian.Uint32(t.Value)), nil
	case "text":
		return text(string(t.Value)), nil
	default:
		return integer(0), &NotImplementedError{"Tuple cannot be converted to dbtype"}
	}
}

type Page struct {
	BinaryEncodingVersion semanticVersion // version of the encoding. Depending on the version the (de)serialization changes.
	HeaderLength          uint8           // length of this header
	PageOffset            uint16          // bytes offset where in the db file does this page start // the type depends on the PAGE_SIZE
	SlotOffset            uint32          // where does the slotarray start

	PageID          PageID
	TableID         TableID
	ColumnID        ColumnID
	PageContentType string
	LatestUse       uint64    // time used to check which page is LRU by the buffer manager
	TypeSize        uint8     // 0 for variable type
	Capacity        uint16    // how many of this stuff still fits in here
	Space           [2]uint16 // pointers to bytes in the page where new tulpes can be inserted.
	isDirty         bool      // does this page contain changes? If so we need to write it to disk
	pinCount        uint8     // by how many concurrent queries is this page used

	SlotArray []Slot
	Tuples    []Tuple // maybe it is a better idea not to parse the entire content by let everything be decoded untill we actually need it
}

func NewPage(pid PageID, tid TableID, pctype string, ts uint8) Page {
	p := Page{BinaryEncodingVersion: VERSION,
		HeaderLength:    HEADER_SIZE,
		PageID:          pid,
		TableID:         tid,
		LatestUse:       uint64(time.Now().UnixMilli()),
		PageContentType: pctype,
		TypeSize:        ts,

		Capacity:  0,
		Space:     [2]uint16{uint16(0), uint16(0)},
		isDirty:   true,
		pinCount:  0,
		SlotArray: []Slot{},
		Tuples:    []Tuple{},
	}
	p.Space = p.calcSpace()
	p.Capacity = p.getCapacity()

	return p

}

func deserializePage(b []byte) *Page {
	// constructs the page structure from a bytes array.

	return &Page{}
}

func (p *Page) getTuple(rowid uint32) Tuple {
	return p.Tuples[rowid]
}

func (p *Page) getDBTypesByTuples(input []Tuple) []dbtype {
	// Given an different set of Tuples give me all Tuples at the same index
	// this is useful when a different column (e.i. page) is filtered and this
	// page should return all rows that were filtered
	slog.Debug("Get all tuples for page.", "Pageid", p.PageID)
	output := []dbtype{}
	var t dbtype
	var err error
	for _, tup := range input {
		slog.Debug("Get tuple for rowid", "rowid", tup.RowID, "allTuples", p.Tuples, "theTuple", p.Tuples[tup.RowID])
		t, err = p.Tuples[tup.RowID].tupleToValue(p.PageContentType)
		if err != nil {
			slog.Error("Conversion not possible for", "type", p.PageContentType, "err", err)
		}
		output = append(output, t)
	}
	return output
}

func (p *Page) getTuplesByTuples(input []Tuple) []Tuple {
	// Given an different set of Tuples give me all Tuples at the same index
	// this is useful when a different column (e.i. page) is filtered and this
	// page should return all rows that were filtered
	slog.Debug("Get all tuples for page.", "Pageid", p.PageID)
	output := []Tuple{}
	for _, tup := range input {
		slog.Debug("Get tuple for rowid", "rowid", tup.RowID, "allTuples", p.Tuples, "theTuple", p.Tuples[tup.RowID])
		output = append(output, p.Tuples[tup.RowID])
	}
	return output
}

func (p *Page) serialize() []byte {
	b := []byte{}

	b = append(b, p.BinaryEncodingVersion.serialize()...)
	b = append(b, p.HeaderLength)
	binary.BigEndian.AppendUint16(b, uint16(p.PageOffset))
	binary.BigEndian.AppendUint16(b, uint16(p.SlotOffset))

	binary.BigEndian.AppendUint16(b, uint16(p.PageID))
	binary.BigEndian.AppendUint16(b, uint16(p.TableID))
	binary.BigEndian.AppendUint16(b, uint16(p.ColumnID))
	var typeInt int
	switch p.PageContentType {
	case "int":
		typeInt = 0
	case "smallint":
		typeInt = 1
	case "tinyint":
		typeInt = 2
	case "text":
		typeInt = 3
	}
	b = append(b, uint8(typeInt))
	binary.BigEndian.AppendUint64(b, uint64(p.LatestUse))
	b = append(b, uint8(p.TypeSize))
	binary.BigEndian.AppendUint16(b, uint16(p.Capacity))
	binary.BigEndian.AppendUint16(b, uint16(p.Space[0]))
	binary.BigEndian.AppendUint16(b, uint16(p.Space[1]))
	var dirtyInt int
	switch p.isDirty {
	case true:
		dirtyInt = 1
	case false:
		dirtyInt = 0
	}
	b = append(b, uint8(dirtyInt))
	b = append(b, p.pinCount)

	for _, s := range p.SlotArray {
		binary.BigEndian.AppendUint32(b, s.RowID)
		binary.BigEndian.AppendUint16(b, s.Offset)
	}
	for _, t := range p.Tuples {
		binary.BigEndian.AppendUint32(b, t.RowID) // remove the rowID later. Not strictly necessary, takes up a lot of space, useful in testing phase
		b = append(b, t.Value...)
	}

	return b

}

func (p *Page) getSize() uint8 {
	// gets the current size in case this page is writen to disk
	return HEADER_SIZE + uint8(SLOT_SIZE)*uint8(len(p.Tuples)) + uint8(len(p.Tuples))
}
func (p *Page) getCapacity() uint16 {
	// returns how many tuples still fit in the page
	return (PAGE_SIZE - uint16(p.getSize())) / (uint16(p.TypeSize) + uint16(ROWIDSIZE))
}
func (p *Page) calcSpace() [2]uint16 {
	// returns how many tuples still fit in the page
	a := uint16(HEADER_SIZE) + SLOT_SIZE*uint16(len(p.Tuples))
	b := PAGE_SIZE - uint16(len(p.Tuples))*(uint16(ROWIDSIZE)+uint16(p.TypeSize))
	return [2]uint16{a, b}
}

func (p *Page) appendTuple(tup Tuple) {
	if p.getCapacity() > 0 {
		p.Tuples = append(p.Tuples, tup)
	}
}
func (p *Page) updateTuple(tup Tuple) {
	p.Tuples[tup.RowID] = tup
}

// type Header struct {
// 	HeaderLength    uint8  // length of this header
// 	PageID          uint16 // id of the page. Used in the page table to find the page
// 	TableID         uint16
// 	ColumnID        uint8
// 	PageContentType uint8  // 256 types of data. every number represent a type
// 	PageOffset      uint16 // bytes offset where in the db file does this page start // the type depends on the PAGE_SIZE
// 	SlotOffset      uint32 // where does the slotarray start
// }

// func deserializeHeader(b []byte) Header {
// 	// creates a header structure from a bytes slice

// 	return Header{}
// }

// func (h *Header) serialize() []byte {
// 	b := []byte{}
// 	b = append(b, h.HeaderLength)
// 	// binary.BigEndian.PutUint16(b, h.HeaderLength)
// 	binary.BigEndian.PutUint16(b[2:], h.PageID)
// 	binary.BigEndian.PutUint16(b[4:], h.TableID)
// 	b = append(b, h.ColumnID)
// 	b = append(b, h.PageContentType)
// 	binary.BigEndian.PutUint16(b[6:], h.PageOffset)
// 	binary.BigEndian.PutUint32(b[8:], h.SlotOffset)
// 	return b
// }

const SLOT_SIZE = uint16(6)

type Slot struct {
	RowID  uint32
	Offset uint16
}

type SlotVarLength struct {
	// I would like to do this with some kind of interface. I am new to Go so need to investigate how to.
	RowID     uint32
	Offset    uint16
	Rowlength uint16
}

func slotFromByteArray(arr []byte) Slot {
	return Slot{
		RowID:  binary.BigEndian.Uint32(arr[0:]),
		Offset: binary.BigEndian.Uint16(arr[2:]),
	}
}
func (s Slot) toByteArray() []byte {
	buffer := make([]byte, SLOT_SIZE)
	binary.BigEndian.PutUint32(buffer[0:], s.RowID)
	binary.BigEndian.PutUint16(buffer[4:], s.Offset)
	return buffer
}
