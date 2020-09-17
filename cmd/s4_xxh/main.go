package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/cespare/xxhash"
)

func main() {

	stream := flag.Bool("stream", false, "stream stdin to stdout with checksum on stderr")
	flag.Parse()

	d := xxhash.New()

	var r io.Reader
	if *stream {
		r = io.TeeReader(os.Stdin, os.Stdout)
	} else {
		r = os.Stdin
	}

	_, err := io.Copy(d, r)
	if err != nil {
		panic(err)
	}

	sum := d.Sum64()

	if *stream {
		_, err = fmt.Fprintf(os.Stderr, "%x\n", sum)
	} else {
		_, err = fmt.Fprintf(os.Stdout, "%x\n", sum)
	}
	if err != nil {
		panic(err)
	}

}
