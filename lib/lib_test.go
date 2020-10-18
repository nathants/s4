package lib

import (
	"fmt"
	"testing"
)

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
		output, glob := ParseGlob(test.input)
		if output != test.output {
			t.Errorf("got: %s, want: %s", output, test.output)
		}
		if glob != test.glob {
			t.Errorf("got: %s, want: %s", glob, test.glob)
		}
	}
}

func TestHash(t *testing.T) {
	type test struct {
		input  string
		output uint64
	}
	tests := []test{
		{"asdf", 8348297608100219689},
		{"123", 10211175311721367273},
		{"hello", 7921424674728911129},
	}
	for _, test := range tests {
		want := test.output
		got := hash(test.input)
		if want != got {
			t.Errorf("got: %d, want: %d", got, want)
		}
	}
}

func TestPickServer(t *testing.T) {
	servers := []Server{
		{"a", "123"},
		{"b", "123"},
		{"c", "123"},
	}
	type test struct {
		key    string
		output string
	}
	tests := []test{
		{"s4://bucket/a.txt", "a:123"},
		{"s4://bucket/d.txt", "c:123"},
		{"s4://bucket/f.txt", "b:123"},
	}
	for _, test := range tests {
		server, _ := PickServer(test.key, servers)
		if fmt.Sprintf("%s:%s", server.Address, server.Port) != test.output {
			t.Errorf("got: %s, want: %s", server, test.output)
		}
	}
}
