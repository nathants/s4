package main

import "testing"

func TestParseGlob(t *testing.T) {
	type test struct {
		input  string
		output string
		glob   string
	}
	tests := []test{
		{"s4://dir1/dir2/", "s4://dir1/dir2/", ""},
		{"s4://dir1/dir2/*", "s4://dir1/dir2/", "*"},
		{"s4://dir1/dir2/*/*", "s4://dir1/dir2/", "*/*"},
		{"s4://dir1/dir2/*/*_1", "s4://dir1/dir2/", "*/*_1"},
	}
	for _, test := range tests {
		result := parseGlob(test.input)
		output := result[0]
		glob := result[1]
		if output != test.output {
			t.Errorf("got: %s, want: %s", output, test.output)
		}
		if glob != test.glob {
			t.Errorf("got: %s, want: %s", glob, test.glob)
		}
	}
}
