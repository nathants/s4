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

	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: cat input | s4-send ADDR PORT\n")
		os.Exit(1)
	}

	addr := os.Args[1]
	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	dst := fmt.Sprintf("%s:%d", addr, port)

	var conn net.Conn
	start := time.Now()

	for {
		conn, err = net.DialTimeout("tcp", dst, timeout)
		if err == nil {
			break
		}
		if time.Since(start) > timeout {
			panic("timeout dialing")
		}
		time.Sleep(time.Microsecond * 10000)
	}

	start = time.Now()

	go func() {
		for {
			if time.Since(start) > timeout {
				panic("timeout writing")
			}
			time.Sleep(time.Microsecond * 10000)
		}
	}()

	rwc := lib.RWCallback{Rw: conn, Cb: func() { start = time.Now() }}

	_, err = io.Copy(rwc, os.Stdin)
	if err != nil {
		panic(err)
	}

	err = rwc.Close()
	if err != nil {
		panic(err)
	}

}
