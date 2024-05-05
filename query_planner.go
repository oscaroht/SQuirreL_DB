package main

import (
	"fmt"
	"log/slog"
)

type Nextable interface {
	// Has a next function that returns a Tuple and a bool that describes if there are more tuples in the scan.
	next() (Tuple, bool)
}

type Distinct struct {
	// Check if the value of an incomming tuples is in a set. If so get the next tuple. If not yet in the set, put it in the set, and return the tuple.
	child   Nextable
	columns []Column
	set     map[string]uint8 // byte slice is not hashable. However, string is just byte slice so let's use that!
}

func NewDistinct(child Nextable, columns []Column) Distinct {
	return Distinct{child, columns, make(map[string]uint8)}
}

func (d *Distinct) next() (Tuple, bool) {
	slog.Debug("Call next on disitnct child")
	tup, eot := d.child.next()

	bytes := []byte{}
	for _, c := range d.columns {
		val, err := c.getTupleByRowID(tup.RowID)
		if err != nil {
			slog.Error(err.Error())
		}
		bytes = append(bytes, val.Value...)
	}

	for !eot {
		_, ok := d.set[string(bytes)]
		if !ok {
			d.set[string(bytes)] = 1 // I am not interested in the value so I just put uint8(1). I cannot find which type consumes the least amount of mem. Maybe it is bool?? Dunno.
			return tup, eot
		}
		tup, eot = d.child.next()
	}
	return tup, eot
}

type OrderBy struct {
	child     Nextable
	fetched   bool
	all       []Tuple
	it        Iterator
	direction string
	column    Column
}

func insertAt(a *[]Tuple, index int, input Tuple) {
	(*a) = append((*a)[:index+1], (*a)[index:]...)
	(*a)[index] = input
}

func insertionSort(a *[]Tuple, input Tuple, direction string) {
	if len((*a)) == 0 {
		slog.Debug("The array is empty so insert it in the first slot.")
		(*a) = []Tuple{input}
		return
	}
	for idx, _ := range *a {
		slog.Debug("check how compares", "insertValue", input.Value, "value", (*a)[idx].Value)
		if lt(input.Value, (*a)[idx].Value) {
			slog.Debug("insert here before", "stop", (*a)[idx].Value)
			insertAt(a, idx, input)
			return
		}
	}
	slog.Debug("insert at the end")
	*a = append(*a, input)
}

func (ob *OrderBy) next() (Tuple, bool) {
	// OrderBy: collect all tuples and insert them in order. Because next() returns 1 tuple at the time
	// an iterator is needed to iterate through the sorted tuples. This iterator will push the tuples
	// up to the next level.
	// var iterator Nextable
	if !ob.fetched {
		tuple, eot := ob.child.next()
		if eot {
			return Tuple{}, true
		}
		for !eot { // iterate for as long as there was no return and the table did not end
			slog.Debug("Order by: Insert value into array", "value", tuple.Value, "array", ob.all)
			insertionSort(&ob.all, tuple, "asc")
			tuple, eot = ob.child.next()
		}
		ob.fetched = true
		switch ob.direction {
		case "asc":
			ob.it = NewIterator(ob.column, &ob.all)
		case "desc":
			ob.it = NewReversedIterator(ob.column, &ob.all)
		default:
			slog.Error("Unknows order bby direction", "direction", ob.it.direction)
		}

	}
	slog.Debug("Fetched and ordered all tuples. Now iterate the tuples to the presenation layer.")
	slog.Debug("Check iterator", "ob.it", ob.it, "ob.itval", ob.it.arr)
	tup, eot2 := ob.it.next()
	// slog.Debug("End of table", "tuple", tup, "eot", eot2, "pointer", ob.it.pointer)
	return tup, eot2
}

func NewOrderBy(column Column, child Nextable, dir string) OrderBy {
	return OrderBy{column: column, child: child, fetched: false, all: []Tuple{}, it: Iterator{}, direction: dir}
}

type Limit struct {
	child   Nextable
	amount  int
	pointer int
}

func (lim *Limit) next() (Tuple, bool) {
	tuple := Tuple{}
	eot := false
	if lim.amount <= 0 {
		return tuple, true
	}
	if !eot && lim.pointer < lim.amount { // iterate for as long as there was no return and the table did not end
		tuple, eot = lim.child.next()
		lim.pointer++
		return tuple, false
	}
	return tuple, true
}

func NewLimit(child Nextable, amount int) Limit {
	return Limit{child: child, amount: amount, pointer: 0}
}

type Filter struct {
	child      Nextable
	page       *Page
	filterFunc binCompare
	operand    []byte
}

func operatorSelection(operator string) binCompare {
	// from string operator to operator function
	switch operator {
	case "=":
		return eq
	case "!=":
		return ne
	case ">":
		return gt
	case ">=":
		return ge
	case "<":
		return lt
	case "<=":
		return le
	default:
		fmt.Printf("No operator found")
		return eq
	}
}

func NewFilter(operator string, operand []byte, page *Page, child Nextable) Filter {
	fnc := operatorSelection(operator)
	return Filter{operand: operand, filterFunc: fnc, page: page, child: child}
}

func (f *Filter) next() (Tuple, bool) {
	tuple, eot := f.child.next()
	slog.Debug("Check filter", "tuple", tuple, "operand", f.operand, "eot", eot)
	for !eot { // iterate for as long as there was no return and the table did not end
		// rowVal := getField(tuple, eqfilter.filter.con.Operand1)

		newTup := f.page.getTuple(tuple.RowID)

		check := f.filterFunc(newTup.Value, f.operand)
		if check {
			slog.Debug("Condition satisfied. Return tuple")
			return tuple, eot
		}
		tuple, eot = f.child.next()
	}
	return tuple, eot // in this case EOT so return default row and eot true}
}

type Iterator struct {
	column    Column
	pointer   int
	arr       *[]Tuple
	direction int // 0 for 0->len, 1 for len->0
}

func NewIterator(column Column, arr *[]Tuple) Iterator {
	return Iterator{column: column, pointer: 0, arr: arr, direction: 0}
}

func NewReversedIterator(column Column, arr *[]Tuple) Iterator {
	return Iterator{column: column, pointer: 0, arr: arr, direction: 1}
}

func (it *Iterator) next() (Tuple, bool) {
	if it.pointer == len(*it.arr) {
		// fmt.Println("EOT")
		return (*it.arr)[0], true // not allowed to return nil
	}
	it.pointer++
	// fmt.Printf("Incremented iterator to %v\n", it.pointer)
	return (*it.arr)[it.direction*(len(*it.arr)-1)+(1-2*it.direction)*(it.pointer-1)], false
}
