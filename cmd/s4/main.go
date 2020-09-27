package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"s4/lib"
	"sort"
	"strings"
)

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

type Result struct {
	resp *http.Response
	err  error
}

func Rm() {
	cmd := flag.NewFlagSet("rm", flag.ExitOnError)
	recursive := *cmd.Bool("recursive", false, "")
	Panic1(cmd.Parse(os.Args[2:]))
	if cmd.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: s4 rm PREFIX [-recursive]")
		cmd.Usage()
		os.Exit(1)
	}
	prefix := cmd.Arg(0)
	if !strings.HasSuffix(prefix, "s4://") {
		panic(prefix)
	}
	if recursive {
		results := make(chan Result)
		for _, server := range lib.Servers() {
			go func() {
				url := fmt.Sprintf("http://%s:%s/delete?prefix=%s&recursive=true", server.Address, server.Port, prefix)
				resp, err := http.Post(url, "application/text", bytes.NewBuffer([]byte{}))
				results <- Result{resp, err}
			}()
		}
		for range lib.Servers() {
			result := <-results
			if result.err != nil {
				panic(result.err)
			}
			if result.resp.StatusCode != 200 {
				panic(result.resp)
			}
		}
	} else {
		server := lib.PickServer(prefix)
		url := fmt.Sprintf("http://%s:%s/delete?prefix=%s", server.Address, server.Port, prefix)
		resp, err := http.Post(url, "application/text", bytes.NewBuffer([]byte{}))
		if err != nil {
			panic(err)
		}
		if resp.StatusCode != 200 {
			panic(resp)
		}
	}
}

func Eval() {
	if len(os.Args) != 4 {
		Panic2(fmt.Fprintln(os.Stderr, "usage: s4 eval KEY CMD"))
		Panic2(fmt.Println(len(os.Args)))
		os.Exit(1)
	}
	key := os.Args[2]
	cmd := os.Args[3]
	server := lib.PickServer(key)
	url := fmt.Sprintf("http://%s:%s/eval?key=%s", server.Address, server.Port, key)
	resp, err := http.Post(url, "application/text", bytes.NewBuffer([]byte(cmd)))
	if err != nil {
		panic(err)
	}
	switch resp.StatusCode {
	case 404:
		fmt.Fprintln(os.Stderr, "fatal: no such key")
		os.Exit(1)
	case 400:
		var result lib.Result
		bytes := Panic2(ioutil.ReadAll(resp.Body)).([]byte)
		Panic1(json.Unmarshal(bytes, &result))
		Panic2(fmt.Fprintln(os.Stderr, result.Stdout))
		Panic2(fmt.Fprintln(os.Stderr, result.Stderr))
		Panic2(fmt.Fprintf(os.Stderr, "%s\n", result.Err))
	case 200:
		bytes := Panic2(ioutil.ReadAll(resp.Body)).([]byte)
		Panic2(fmt.Print(bytes))
	default:
		panic(resp)
	}
}

func contains(parts []string, part string) bool {
	for _, p := range parts {
		if p == part {
			return true
		}
	}
	return false
}

func Ls() {
	cmd := flag.NewFlagSet("ls", flag.ExitOnError)
	recursive := *cmd.Bool("recursive", false, "")
	Panic1(cmd.Parse(os.Args[2:]))
	if false {
		fmt.Fprintln(os.Stderr, "usage: s4 ls [PREFIX] [-recursive]")
		cmd.Usage()
		os.Exit(1)
	}
	var lines [][]string
	switch cmd.NArg() {
	case 1:
		prefix := cmd.Arg(0)
		val := strings.SplitN(prefix, "://", 2)[1]
		if !recursive && strings.Count(val, "/") == 0 {
			for _, line := range list_buckets() {
				if contains(line, val) {
					lines = [][]string{line}
					break
				}
			}
		} else {
			lines = list(prefix, recursive)
		}
	case 0:
		lines = list_buckets()
	default:
		fmt.Fprintln(os.Stderr, "usage: s4 ls [PREFIX] [-recursive]")
		cmd.Usage()
		os.Exit(1)
	}
	for _, line := range lines {
		fmt.Println(line)
	}
}

func list_buckets() [][]string {
	results := make(chan Result)
	for _, server := range lib.Servers() {
		go func() {
			url := fmt.Sprintf("http://%s:%s/list_buckets", server.Address, server.Port)
			resp, err := http.Get(url)
			results <- Result{resp, err}
		}()
	}
	var buckets map[string][]string
	for range lib.Servers() {
		result := <-results
		if result.err != nil {
			panic(result.err)
		}
		if result.resp.StatusCode != 200 {
			panic(result.resp)
		}
		bytes := Panic2(ioutil.ReadAll(result.resp.Body)).([]byte)
		var res [][]string
		Panic1(json.Unmarshal(bytes, res))
		for _, line := range res {
			path := line[3]
			buckets[path] = line
		}
	}
	var lines [][]string
	var keys []string
	for k, _ := range buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] > keys[j]
	})
	for _, key := range keys {
		lines = append(lines, buckets[key])
	}
	return lines
}

func list(prefix string, recursive bool) [][]string {
	recursive_param := ""
	if recursive {
		recursive_param = "&recursive=true"
	}
	results := make(chan Result)
	for _, server := range lib.Servers() {
		go func() {
			url := fmt.Sprintf("http://%s:%s/list?prefix=%s%s", server.Address, server.Port, prefix, recursive_param)
			resp, err := http.Get(url)
			results <- Result{resp, err}
		}()
	}
	var lines [][]string
	var line []string
	for range lib.Servers() {
		result := <-results
		if result.err != nil {
			panic(result.err)
		}
		if result.resp.StatusCode != 200 {
			panic(result.resp)
		}
		bytes := Panic2(ioutil.ReadAll(result.resp.Body)).([]byte)
		Panic1(json.Unmarshal(bytes, &line))
		lines = append(lines, line)
	}
	return lines
}

func Cp() {
}

func Config() {
}

func Health() {
}

func usage() {
	fmt.Println("usage:")
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "rm":
		Rm()
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
