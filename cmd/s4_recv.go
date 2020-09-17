package main

import (
	"fmt"
	"net"
	"strconv"
	"io"
	"os"
	"time"
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
	conn, err := li.Accept()
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(os.Stdout, conn)
	if err != nil {
		panic(err)
	}
	err = conn.Close()
	if err != nil {
		panic(err)
	}
	err = li.Close()
	if err != nil {
		panic(err)
	}
}
