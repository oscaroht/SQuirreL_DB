package main

import (
	"fmt"
	"time"
)

const BUFFER_POOL_SIZE = 10 // times 4kB
type PageID uint16
type FrameIndex uint16

type BufferManager struct {
	PageTable  map[PageID]FrameIndex // map pageID to index
	FreeList   []FrameIndex
	BufferPool [BUFFER_POOL_SIZE]*Page
}

func NewBufferManager() *BufferManager {
	// for bufferpool the default values are used array of nils
	pt := map[PageID]FrameIndex{} // init with empty map
	fl := []FrameIndex{}
	for i := range BUFFER_POOL_SIZE {
		fl = append(fl, FrameIndex(i))
	}
	return &BufferManager{PageTable: pt, FreeList: fl}
}

func (b BufferManager) getPage(pageID PageID) *Page {
	// Given the page ID return a pointer to the page.
	idx, ok := b.PageTable[pageID]
	if !ok {
		fmt.Printf("Implementation error. Page %v is not is buffer. Should get it from disk. Not implemented yet.", pageID)
	}
	pagePointer := b.BufferPool[idx]
	(*pagePointer).LatestUse = uint64(time.Now().UnixMilli())
	return pagePointer
}
func (b *BufferManager) addPage(page *Page) {
	if len(b.FreeList) == 0 {
		fmt.Println("BufferPool exceeded")
		b.evictLRUPage()
		return
	}
	idx := b.FreeList[0]
	b.FreeList = b.FreeList[1:] // remove 0 idx
	b.BufferPool[idx] = page
	b.PageTable[page.PageID] = idx
	fmt.Printf("Added page id %v to frame %v\n", page.PageID, idx)
}
func (b BufferManager) evictLRUPage() {
	// find the page to replace
	// get the pageid and frameid
	// remove the page from the page table and add the frame idx to the free list
	p := b.getLeastRecentlyUsed()
	pid := (*p).PageID
	fmt.Printf("Page %v was least used so it's frame can now be taken by another page\n", pid)
	frame_idx := b.PageTable[pid]
	b.FreeList = append(b.FreeList, frame_idx)
	delete(b.PageTable, pid) // delete from the page table
}
func (b BufferManager) getLeastRecentlyUsed() *Page {
	// get the FrameID of the least recentlyt used page
	// better would be to do some kind of LeastFrequentlyUsed or LRUk (need to look this up)
	var LRU *Page
	LRUval := uint64(time.Now().UnixMilli())
	// the POOL size is very small so linear search will be fast
	for _, page := range b.BufferPool {
		if (*page).LatestUse < LRUval {
			LRUval = (*page).LatestUse
			LRU = page
		}
	}
	return LRU
}

func (bm *BufferManager) bufferNewTable(tab *TableDescription) {

	for i, c := range tab.Columns {
		h := Header{HeaderLength: HEADER_SIZE, PageID: uint16(i), TableID: uint16(1)}
		var typeSize int8
		switch c.ColumnType { // maybe better to store this somewhere instead of a random switch here
		case "tinyint":
			typeSize = int8(1)
		case "smallint":
			typeSize = int8(2)
		case "int":
			typeSize = int8(4)
		case "text":
			typeSize = int8(64) // should have no max capacity not lets be easy for now
		}

		p := Page{Header: h, PageID: PageID(i), LatestUse: uint64(time.Now().UnixMilli()), TypeSize: typeSize}
		(*tab).Columns[i].PageIDs = append((*tab).Columns[i].PageIDs, p.PageID)
		bm.addPage(&p)
	}

}
