package main

import (
	"fmt"
	"reflect"
)

type Nextable interface {
	next() (Tuple, bool)
}

type OrderBy struct {
	child Nextable
}

func (ob OrderBy) result() []Tuple {
	res := []Tuple{}
	tuple, eot := ob.child.next()
	res = append(res, tuple)
	for !eot { // iterate for as long as there was no return and the table did not end
		tuple, eot = ob.child.next()
		res = append(res, tuple)
	}
	// look at: https://stackoverflow.com/questions/28999735/what-is-the-shortest-way-to-simply-sort-an-array-of-structs-by-arbitrary-field
	// a:=res[0].Value
	// sort.Slice(res, func(i, j dbtype) bool {
	// 	return res[i].Value < res[j].Value
	// })

	return res
}

type Limit struct {
	child  Nextable
	amount int
}

func (lim Limit) result() []Tuple {
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
		fmt.Printf("Unable to compare type %v\n", x)
		return false
	}
}
func ge[T dbtype](x [2]T) bool {
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
		fmt.Printf("Unable to order type %v\n", x)
		return false
	}
}
func lt[T dbtype](x [2]T) bool {
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
		fmt.Printf("Unable to order type %v\n", x)
		return false
	}
}

func le[T dbtype](x [2]T) bool {
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

func (f Filter) next() (Tuple, bool) {
	tuple, eot := f.child.next()
	fmt.Printf("Go tuple: %v, check if it macthes %v. EOT %v\n", tuple, f.operand, eot)
	for !eot { // iterate for as long as there was no return and the table did not end
		// rowVal := getField(tuple, eqfilter.filter.con.Operand1)
		arr := [2]dbtype{tuple.Value, f.operand}

		check := f.filterFunc(arr)
		fmt.Printf("%v\n", check)
		if check {
			fmt.Printf("Filter condition satisfied so let's return")
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
		fmt.Println("EOT")
		return (*it.arr)[0], true
	}
	it.pointer++
	fmt.Printf("Incremented iterator to %v\n", it.pointer)
	return (*it.arr)[it.pointer-1], false
}

// func (qp *EqFilter) next() (*Row, bool) {
// 	row, eot := qp.it.next()
// 	for !eot { // iterate for as long as there was no return and the table did not end
// 		rowVal := getField(row, qp.f1.con.Operand1)
// 		if rowVal == qp.f1.con.Operand2 {
// 			return row, eot
// 		}
// 		row, eot = qp.it.next()
// 	}
// 	return row, eot

// }

func getField(v *Row, field string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return string(f.String())
}
