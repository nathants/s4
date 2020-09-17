package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"
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
	conn, err := net.DialTimeout("tcp", dst, timeout)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(conn, os.Stdin)
	if err != nil {
		panic(err)
	}
	err = conn.Close()
	if err != nil {
		panic(err)
	}
}
