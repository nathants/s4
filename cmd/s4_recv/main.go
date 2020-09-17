package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"s4/lib"
)

const timeout = 5 * time.Second

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: s4-recv PORT > output\n")
		os.Exit(1)
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}

	src := fmt.Sprintf(":%d", port)
	li, err := net.Listen("tcp", src)
	if err != nil {
		panic(err)
	}

	start := time.Now()

	go func() {
		for {
			if time.Since(start) > timeout {
				panic("timeout reading")
			}
			time.Sleep(time.Microsecond * 10000)
		}
	}()

	conn, err := li.Accept()
	if err != nil {
		panic(err)
	}
	start = time.Now()

	rwc := lib.RWCallback{conn, func() { start = time.Now() }}

	_, err = io.Copy(os.Stdout, rwc)
	if err != nil {
		panic(err)
	}

	err = rwc.Close()
	if err != nil {
		panic(err)
	}

	err = li.Close()
	if err != nil {
		panic(err)
	}

}
