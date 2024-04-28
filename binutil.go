package main

import "log/slog"

type binCompare func([]byte, []byte) bool

func eq(x []byte, y []byte) bool {
	slog.Debug("Evaluate x==y for", "x", x, "y", y)
	if len(x) != len(y) {
		return false
	}
	for i, _ := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func ne(x []byte, y []byte) bool {
	return !eq(x, y)
}

func gt(x []byte, y []byte) bool {
	for i, _ := range x {
		slog.Debug("Check if x>y", "x", x[i], "y", y[i])
		if x[i] > y[i] {
			return true
		}
	}
	return false
}

func ge(x []byte, y []byte) bool {
	for i, _ := range x {
		slog.Debug("Check if x<y", "x", x[i], "y", y[i])
		if x[i] > y[i] {
			return true
		}
		if x[i] < y[i] {
			return false
		}
	}
	return len(x) == len(y) // at this point all of x bytes are also in y. If the length is the same both arrays are the same
}

func lt(x []byte, y []byte) bool {
	return !ge(x, y)
}
func le(x []byte, y []byte) bool {
	return !gt(x, y)
}
