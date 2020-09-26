package main

import (
	"fmt"
	"os"
)

func Eval() {
	fmt.Println("eval")
}

func Ls() {
}

func Cp() {
}

func Config() {
}

func Health() {
}

func usage() {
	fmt.Println("usage: ")
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "eval":
		Eval()
	case "ls":
		Ls()
	case "cp":
		Cp()
	case "config":
		Config()
	case "health":
		Health()
	default:
		usage()
	}
}
