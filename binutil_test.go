package main

import (
	"encoding/binary"
	"log/slog"
	"testing"
)

func TestBinUtil(t *testing.T) {

	slog.SetLogLoggerLevel(slog.LevelDebug)

	tests := []struct {
		x        uint16
		function binCompare
		y        uint16
		want     bool
	}{
		{x: 258, function: gt, y: 64, want: true},
		{x: 1100, function: gt, y: 3, want: true},
		{x: 851, function: gt, y: 644, want: true},
		{x: 258, function: gt, y: 690, want: false},
		{x: 0, function: gt, y: 0, want: false},
		{x: 258, function: gt, y: 258, want: false},
		{x: 259, function: gt, y: 690, want: false},

		{x: 258, function: ge, y: 64, want: true},
		{x: 1100, function: ge, y: 3, want: true},
		{x: 851, function: ge, y: 644, want: true},
		{x: 258, function: ge, y: 690, want: false},
		{x: 0, function: ge, y: 0, want: true},
		{x: 258, function: ge, y: 258, want: true},
		{x: 259, function: ge, y: 690, want: false},

		{x: 258, function: lt, y: 64, want: false},
		{x: 1100, function: lt, y: 3, want: false},
		{x: 851, function: lt, y: 644, want: false},
		{x: 258, function: lt, y: 690, want: true},
		{x: 0, function: lt, y: 0, want: false},
		{x: 258, function: lt, y: 258, want: false},
		{x: 259, function: lt, y: 690, want: true},

		{x: 258, function: le, y: 64, want: false},
		{x: 1100, function: le, y: 3, want: false},
		{x: 851, function: le, y: 644, want: false},
		{x: 258, function: le, y: 690, want: true},
		{x: 0, function: le, y: 0, want: true},
		{x: 258, function: le, y: 258, want: true},
		{x: 259, function: le, y: 690, want: true},

		{x: 258, function: eq, y: 64, want: false},
		{x: 1100, function: eq, y: 3, want: false},
		{x: 851, function: eq, y: 644, want: false},
		{x: 258, function: eq, y: 690, want: false},
		{x: 0, function: eq, y: 0, want: true},
		{x: 258, function: eq, y: 258, want: true},
		{x: 259, function: eq, y: 690, want: false},

		{x: 258, function: ne, y: 64, want: true},
		{x: 1100, function: ne, y: 3, want: true},
		{x: 851, function: ne, y: 644, want: true},
		{x: 258, function: ne, y: 690, want: true},
		{x: 0, function: ne, y: 0, want: false},
		{x: 258, function: ne, y: 258, want: false},
		{x: 259, function: ne, y: 690, want: true},
	}
	a := make([]byte, 16)
	b := make([]byte, 16)
	for _, test := range tests {
		binary.BigEndian.PutUint16(a, uint16(test.x))
		binary.BigEndian.PutUint16(b, uint16(test.y))
		if test.function(a, b) != test.want {
			t.Fatalf("%v %v %v is not %v", test.x, test.function, test.y, test.want)
		}
	}
}
