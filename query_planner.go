package main

import (
	"fmt"
	"log/slog"
)

type Nextable interface {
	// Has a next function that returns a Tuple and a bool that describes if there are more tuples in the scan.
	next() (Tuple, bool)
}

type OrderBy struct {
	child   Nextable
	fetched bool
	all     []Tuple
	it      Iterator
}

func insertAt(a *[]Tuple, index int, input Tuple) {
	(*a) = append((*a)[:index+1], (*a)[index:]...)
	(*a)[index] = input
}

func insertionSort(a *[]Tuple, input Tuple, direction string) {
	if len((*a)) == 0 {
		(*a) = []Tuple{input}
		return
	}

	com := [2]dbtype{}
	for idx, _ := range *a {
		com[0] = input.Value
		com[1] = (*a)[idx].Value
		if lt(com) {
			insertAt(a, idx, input)
			return
		}
	}
}

func (ob OrderBy) next() (Tuple, bool) {
	if !ob.fetched {
		tuple, eot := ob.child.next()
		for !eot { // iterate for as long as there was no return and the table did not end
			slog.Debug("Order by: Insert value into array", "value", tuple.Value, "array", ob.all)
			insertionSort(&ob.all, tuple, "asc")
			tuple, eot = ob.child.next()
			ob.fetched = true
			ob.it = NewIterator(&ob.all)
		}
	}
	return ob.it.next()
}

func NewOrderBy(child Nextable) OrderBy {
	return OrderBy{child: child, fetched: false, all: []Tuple{}, it: Iterator{}}
}

type Limit struct {
	child  Nextable
	amount int
}

func (lim Limit) next() []Tuple {
	res := []Tuple{}
	if lim.amount <= 0 {
		return res
	}
	tuple, eot := lim.child.next()
	res = append(res, tuple)
	for !eot && len(res) < lim.amount { // iterate for as long as there was no return and the table did not end
		tuple, eot = lim.child.next()
		res = append(res, tuple)
	}
	return res
}

type Filter struct {
	child      Nextable
	filterFunc filfunc
	operand    dbtype
}

func eq[Q comparable](x [2]Q) bool {
	// fmt.Printf("Check if %v matches %v\n", x[0], x[1])
	// switch x := any(x).(type) {
	// case []smallint:
	// 	return x[0] == x[1]
	// case []tinyint:
	// 	return x[0] == x[1]
	// case []integer:
	// 	return x[0] == x[1]
	// case []text:
	// 	return x[0] == x[1]
	// default:
	// 	fmt.Printf("Unable to order type %v\n", x)
	// 	return false
	// }
	// fmt.Printf("Check if %v matches %v\n", x[0], x[1])
	slog.Debug("Evaluate == for.", "operand", x[0], "operand", x[1])
	return x[0] == x[1]
}
func ne[Q comparable](x [2]Q) bool { // this means the function takes any 2 things as long as they are comaprable
	return x[0] != x[1]
}
func gt[T dbtype](x [2]T) bool {
	// for == and != can be bitwise compared. >, >=, <, <= need a type that can be ordered. Unfortunately we
	// cannot specify this because for the filfunc definition we need a type. This cannot be a collection of
	// types or interface. So now, I have put the values to compare in an array to force the compiler to
	// understand that both are the same type. Then I create a switch on the type of the array which does the
	// same thing for every option.
	// yes, it is quite stupid.
	// I also do not like that this switch should be extened when new types are added. I would be better if it
	// were part of the dbtype interface. However, this would mean we need to make x.gt(y dbtype) -> dbtype
	// So in this case we need to switch and cast every dbtype to evert dbtype and repeat that code for every
	// dbtype. Very painfull.

	switch x := any(x).(type) {
	case []smallint:
		return x[0] > x[1]
	case []tinyint:
		return x[0] > x[1]
	case []integer:
		return x[0] > x[1]
	case []text:
		return x[0] > x[1]
	default:
		// fmt.Printf("Unable to compare type %v\n", x)
		return false
	}
}
func ge[T dbtype](x [2]T) bool {
	// having to do this switch is stupid. Probably skill issue
	switch x := any(x).(type) {
	case []smallint:
		return x[0] >= x[1]
	case []tinyint:
		return x[0] >= x[1]
	case []integer:
		return x[0] >= x[1]
	case []text:
		return x[0] >= x[1]
	default:
		fmt.Printf("Unable to order type %T of value %v\n", x)
		return false
	}
}
func lt[T dbtype](x [2]T) bool {
	// having to do this switch is stupid. Probably skill issue
	switch x := any(x).(type) {
	case []smallint:
		return x[0] < x[1]
	case []tinyint:
		return x[0] < x[1]
	case []integer:
		return x[0] < x[1]
	case []text:
		return x[0] < x[1]
	default:
		fmt.Printf("Unable to order type %T of value %v\n", x, x)
		return false
	}
}

func le[T dbtype](x [2]T) bool {
	// having to do this switch is stupid. Probably skill issue
	switch x := any(x).(type) {
	case []smallint:
		return x[0] <= x[1]
	case []tinyint:
		return x[0] <= x[1]
	case []integer:
		return x[0] <= x[1]
	case []text:
		return x[0] <= x[1]
	default:
		fmt.Printf("Unable to order type %v\n", x)
		return false
	}
}

type filfunc func([2]dbtype) bool

func operatorSelection(operator string) filfunc {
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

func NewFilter(operator string, operand dbtype, child Nextable) Filter {
	fnc := operatorSelection(operator)
	return Filter{operand: operand, filterFunc: fnc, child: child}
}

func (f *Filter) next() (Tuple, bool) {
	tuple, eot := f.child.next()
	slog.Debug("Check filter", "tuple", tuple, "operand", f.operand, "eot", eot)
	for !eot { // iterate for as long as there was no return and the table did not end
		// rowVal := getField(tuple, eqfilter.filter.con.Operand1)
		arr := [2]dbtype{tuple.Value, f.operand}
		check := f.filterFunc(arr)
		if check {
			slog.Debug("Condition satisfied. Return tuple")
			return tuple, eot
		}
		tuple, eot = f.child.next()
	}
	return tuple, eot // in this case EOT so return default row and eot true}
}

type Iterator struct {
	pointer int
	arr     *[]Tuple
}

func NewIterator(arr *[]Tuple) Iterator {
	return Iterator{pointer: 0, arr: arr}
}

// func (it *Iterator) AddQueryExecutionStep(qes *QueryExecutionStep){
// 	it.step = *qes
// }

func (it *Iterator) next() (Tuple, bool) {
	if it.pointer == len(*it.arr) {
		// fmt.Println("EOT")
		return (*it.arr)[0], true
	}
	it.pointer++
	// fmt.Printf("Incremented iterator to %v\n", it.pointer)
	return (*it.arr)[it.pointer-1], false
}
