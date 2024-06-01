package main

import (
	"log/slog"
	"testing"
)

func TestSemanticVersioning(t *testing.T) {

	slog.SetLogLoggerLevel(slog.LevelDebug)

	tests := []struct {
		version semanticVersion
		want    []byte
	}{
		{version: [3]uint8{1, 1, 1}, want: []byte{1, 1, 1}},
		{version: [3]uint8{1, 2, 3}, want: []byte{1, 2, 3}},
		{version: [3]uint8{1, 4, 4}, want: []byte{1, 4, 4}},
		{version: [3]uint8{0, 8, 8}, want: []byte{0, 8, 8}},
	}

	for _, test := range tests {
		// test serialization
		result := test.version.serialize()
		for i := range result {
			if result[i] != test.want[i] {
				t.Fatalf("result %v not equal to want %v", result, test.want)
			}
		}

		// test deserialization
		vers := deserializeSemanticVersion(test.want)
		for i := range vers {
			if vers[i] != test.version[i] {
				t.Fatalf("result %v not equal to want %v", result, test.want)
			}
		}

	}

}

func TestPage(t *testing.T) {

	slog.SetLogLoggerLevel(slog.LevelDebug)

	bm = NewBufferManager()
	tm = NewTableManager()

	tests := []struct {
		version semanticVersion
		want    []byte
	}{
		{version: [3]uint8{1, 1, 1}, want: []byte{1, 1, 1}},
		{version: [3]uint8{1, 2, 3}, want: []byte{1, 2, 3}},
		{version: [3]uint8{1, 4, 4}, want: []byte{1, 4, 4}},
		{version: [3]uint8{0, 8, 8}, want: []byte{0, 8, 8}},
	}

	for _, test := range tests {
		// test serialization
		result := test.version.serialize()
		for i := range result {
			if result[i] != test.want[i] {
				t.Fatalf("result %v not equal to want %v", result, test.want)
			}
		}

		// test deserialization
		vers := deserializeSemanticVersion(test.want)
		for i := range vers {
			if vers[i] != test.version[i] {
				t.Fatalf("result %v not equal to want %v", result, test.want)
			}
		}

	}

}
