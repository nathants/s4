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

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: s4-recv PORT > output\n")
		os.Exit(1)
	}

	port := Panic2(strconv.Atoi(os.Args[1])).(int)

	src := fmt.Sprintf(":%d", port)

	li := Panic2(net.Listen("tcp", src)).(net.Listener)

	start := time.Now()

	go func() {
		for {
			Assert(time.Since(start) < timeout, "timeout reading")
			time.Sleep(time.Microsecond * 10000)
		}
	}()

	conn := Panic2(li.Accept()).(net.Conn)

	start = time.Now()

	rwc := lib.RWCallback{Rw: conn, Cb: func() { start = time.Now() }}

	Panic2(io.Copy(os.Stdout, rwc))

	Panic1(rwc.Close())

	Panic1(li.Close())

}
