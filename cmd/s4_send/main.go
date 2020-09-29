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

func Assert(cond bool, format string, a ...interface{}) {
	if !cond {
		panic(fmt.Sprintf(format, a...))
	}
}

func Panic1(e error) {
	if e != nil {
		panic(e)
	}
}

func Panic2(x interface{}, e error) interface{} {
	if e != nil {
		panic(e)
	}
	return x
}

func main() {

	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: cat input | s4-send ADDR PORT\n")
		os.Exit(1)
	}

	addr := os.Args[1]
	port, err := strconv.Atoi(os.Args[2])
	Assert(err == nil, fmt.Sprint(err))

	dst := fmt.Sprintf("%s:%d", addr, port)

	var conn net.Conn
	start := time.Now()

	for {
		conn, err = net.DialTimeout("tcp", dst, timeout)
		if err == nil {
			break
		}
		Assert(time.Since(start) < timeout, "timeout dialing")
		time.Sleep(time.Microsecond * 10000)
	}

	start = time.Now()

	go func() {
		for {
			Assert(time.Since(start) < timeout, "timeout writing")
			time.Sleep(time.Microsecond * 10000)
		}
	}()

	rwc := lib.RWCallback{Rw: conn, Cb: func() { start = time.Now() }}

	Panic2(io.Copy(rwc, os.Stdin))

	Panic1(rwc.Close())

}
