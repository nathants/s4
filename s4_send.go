package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"s4/lib"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: s4-send ADDR PORT\n")
		os.Exit(1)
	}
	addr := os.Args[1]
	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	dst := fmt.Sprintf("%s:%d", addr, port)
	fmt.Println(dst)
	fmt.Println(lib.Test())
	conn, err := net.Dial("tcp", dst)
	if err != nil {
		panic(err)
	}
	_ = conn
}
