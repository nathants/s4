package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/nathants/s4"
	"github.com/nathants/s4/lib"
)

func Rm() {
	cmd := flag.NewFlagSet("rm", flag.ExitOnError)
	recursive := cmd.Bool("r", false, "recursive")
	if len(os.Args) < 2 || lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 rm PREFIX [-r]"))
		cmd.PrintDefaults()
		os.Exit(1)
	}
	panic1(cmd.Parse(os.Args[2:]))
	prefix := cmd.Arg(0)
	panic1(s4.Rm(prefix, *recursive))
}

func Map() {
	if len(os.Args) != 5 || lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 map INDIR OUTDIR CMD"))
		os.Exit(1)
	}
	indir := os.Args[2]
	outdir := os.Args[3]
	cmd := os.Args[4]
	panic1(s4.Map(indir, outdir, cmd, func() { fmt.Printf("ok ") }))
}

func MapToN() {
	if len(os.Args) != 5 || lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 map-to-n INDIR OUTDIR CMD"))
		os.Exit(1)
	}
	indir := os.Args[2]
	outdir := os.Args[3]
	cmd := os.Args[4]
	panic1(s4.MapToN(indir, outdir, cmd, func() { fmt.Printf("ok ") }))
}

func MapFromN() {
	if len(os.Args) != 5 || lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 map-from-n INDIR OUTDIR CMD"))
		os.Exit(1)
	}
	indir := os.Args[2]
	outdir := os.Args[3]
	cmd := os.Args[4]
	panic1(s4.MapFromN(indir, outdir, cmd, func() { fmt.Printf("ok ") }))
}

func Eval() {
	if len(os.Args) != 4 || lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 eval KEY CMD"))
		os.Exit(1)
	}
	key := os.Args[2]
	cmd := os.Args[3]
	result, err := s4.Eval(key, cmd)
	panic1(err)
	fmt.Println(result)
}

func Ls() {
	cmd := flag.NewFlagSet("ls", flag.ExitOnError)
	recursive := cmd.Bool("r", false, "recursive")
	if lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 ls [PREFIX] [-r]"))
		os.Exit(1)
	}
	panic1(cmd.Parse(os.Args[2:]))
	var lines [][]string
	var err error
	switch cmd.NArg() {
	case 1:
		prefix := cmd.Arg(0)
		val := strings.SplitN(prefix, "://", 2)[1]
		if !*recursive && strings.Count(val, "/") == 0 {
			for _, line := range panic2(s4.ListBuckets()).([][]string) {
				if lib.Contains(line, val) {
					lines = [][]string{line}
					break
				}
			}
		} else {
			lines, err = s4.List(prefix, *recursive)
			panic1(err)
		}
	case 0:
		lines, err = s4.ListBuckets()
		panic1(err)
	default:
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 ls [PREFIX] [-r]"))
		cmd.Usage()
		os.Exit(1)
	}
	if len(lines) == 0 {
		os.Exit(1)
	}
	for _, line := range lines {
		panic2(fmt.Println(strings.Join(line, " ")))
	}
}

func Cp() {
	cmd := flag.NewFlagSet("cp", flag.ExitOnError)
	recursive := cmd.Bool("r", false, "recursive")
	if lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 cp SRC DST [-r]"))
		cmd.PrintDefaults()
		os.Exit(1)
	}
	panic1(cmd.Parse(os.Args[2:]))
	src := cmd.Arg(0)
	dst := cmd.Arg(1)
	panic1(s4.Cp(src, dst, *recursive))
}

func Config() {
	for _, server := range panic2(lib.Servers()).([]lib.Server) {
		fmt.Printf("%s:%s\n", server.Address, server.Port)
	}
}

func Health() {
	servers := panic2(lib.Servers()).([]lib.Server)
	results := make(chan string, len(servers))
	client := http.Client{Timeout: 1 * time.Second}
	for _, server := range servers {
		go func(server lib.Server) {
			url := fmt.Sprintf("http://%s:%s/health", server.Address, server.Port)
			resp, err := client.Get(url)
			if err != nil || resp.StatusCode != 200 {
				results <- fmt.Sprintf("unhealthy: %s:%s", server.Address, server.Port)
			} else {
				results <- fmt.Sprintf("healthy:   %s:%s", server.Address, server.Port)
			}
		}(server)
	}
	fail := false
	for range servers {
		result := <-results
		if !strings.HasPrefix(result, "healthy: ") {
			fail = true
		}
		fmt.Println(result)
	}
	if fail {
		os.Exit(1)
	}
}

func Usage() {
	panic2(fmt.Println(`usage: s4 {rm,eval,ls,cp,map,map-to-n,map-from-n,config,health}

    rm                  delete data from s4
    eval                eval a bash cmd with key data as stdin
    ls                  list keys
    cp                  copy data to or from s4
    map                 process data
    map-to-n            shuffle data
    map-from-n          merge shuffled data
    config              list the server addresses
    health              health check every server`))
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		Usage()
	}
	switch os.Args[1] {
	case "rm":
		Rm()
	case "map":
		Map()
	case "map-to-n":
		MapToN()
	case "map-from-n":
		MapFromN()
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
		Usage()
	}
}

func panic1(e error) {
	if e != nil {
		fmt.Fprintf(os.Stderr, "fatal: %s\n", e)
		os.Exit(1)
	}
}

func panic2(x interface{}, e error) interface{} {
	if e != nil {
		fmt.Fprintf(os.Stderr, "fatal: %s\n", e)
		os.Exit(1)
	}
	return x
}
