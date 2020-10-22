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
	flg := flag.NewFlagSet("rm", flag.ExitOnError)
	usage := func() {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 rm PREFIX [-r] [-c]"))
		flg.PrintDefaults()
		os.Exit(1)
	}
	recursive := flg.Bool("r", false, "recursive")
	conf_path := flg.String("c", lib.DefaultConfPath(), "conf-path")
	if lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		usage()
	}
	panic1(flg.Parse(os.Args[2:]))
	if flg.NArg() != 1 {
		usage()
	}
	prefix := flg.Arg(0)
	servers := panic2(lib.GetServers(*conf_path)).([]lib.Server)
	panic1(s4.Rm(prefix, *recursive, servers))
}

func Map() {
	flg := flag.NewFlagSet("map", flag.ExitOnError)
	usage := func() {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 map INDIR OUTDIR CMD [-c]"))
		flg.PrintDefaults()
		os.Exit(1)
	}
	conf_path := flg.String("c", lib.DefaultConfPath(), "conf-path")
	if lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		usage()
	}
	panic1(flg.Parse(os.Args[2:]))
	if flg.NArg() != 3 {
		usage()
	}
	indir := flg.Arg(0)
	outdir := flg.Arg(1)
	cmd := flg.Arg(2)
	servers := panic2(lib.GetServers(*conf_path)).([]lib.Server)
	panic1(s4.Map(indir, outdir, cmd, servers, func() { fmt.Printf("ok ") }))
}

func MapToN() {
	flg := flag.NewFlagSet("map-to-n", flag.ExitOnError)
	usage := func() {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 map-to-n INDIR OUTDIR CMD [-c]"))
		flg.PrintDefaults()
		os.Exit(1)
	}
	conf_path := flg.String("c", lib.DefaultConfPath(), "conf-path")
	if lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		usage()
	}
	panic1(flg.Parse(os.Args[2:]))
	if flg.NArg() != 3 {
		usage()
	}
	indir := flg.Arg(0)
	outdir := flg.Arg(1)
	cmd := flg.Arg(2)
	servers := panic2(lib.GetServers(*conf_path)).([]lib.Server)
	panic1(s4.MapToN(indir, outdir, cmd, servers, func() { fmt.Printf("ok ") }))
}

func MapFromN() {
	flg := flag.NewFlagSet("map-from-n", flag.ExitOnError)
	usage := func() {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 map-from-n INDIR OUTDIR CMD [-c]"))
		flg.PrintDefaults()
		os.Exit(1)
	}
	conf_path := flg.String("c", lib.DefaultConfPath(), "conf-path")
	if lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		usage()
	}
	panic1(flg.Parse(os.Args[2:]))
	indir := flg.Arg(0)
	outdir := flg.Arg(1)
	cmd := flg.Arg(2)
	if flg.NArg() != 3 {
		usage()
	}
	servers := panic2(lib.GetServers(*conf_path)).([]lib.Server)
	panic1(s4.MapFromN(indir, outdir, cmd, servers, func() { fmt.Printf("ok ") }))
}

func Eval() {
	flg := flag.NewFlagSet("eval", flag.ExitOnError)
	usage := func() {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 eval KEY CMD [-c]"))
		flg.PrintDefaults()
		os.Exit(1)
	}
	conf_path := flg.String("c", lib.DefaultConfPath(), "conf-path")
	if lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		usage()
	}
	panic1(flg.Parse(os.Args[2:]))
	key := flg.Arg(0)
	cmd := flg.Arg(1)
	if flg.NArg() != 2 {
		usage()
	}
	servers := panic2(lib.GetServers(*conf_path)).([]lib.Server)
	result, err := s4.Eval(key, cmd, servers)
	panic1(err)
	fmt.Println(result)
}

func Ls() {
	flg := flag.NewFlagSet("ls", flag.ExitOnError)
	usage := func() {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 ls [PREFIX] [-r] [-c]"))
		flg.PrintDefaults()
		os.Exit(1)
	}
	recursive := flg.Bool("r", false, "recursive")
	conf_path := flg.String("c", lib.DefaultConfPath(), "conf-path")
	if lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		usage()
	}
	panic1(flg.Parse(os.Args[2:]))
	servers := panic2(lib.GetServers(*conf_path)).([]lib.Server)
	var lines [][]string
	var err error
	switch flg.NArg() {
	case 1:
		prefix := flg.Arg(0)
		val := strings.SplitN(prefix, "://", 2)[1]
		if !*recursive && strings.Count(val, "/") == 0 {
			for _, line := range panic2(s4.ListBuckets(servers)).([][]string) {
				if lib.Contains(line, val) {
					lines = [][]string{line}
					break
				}
			}
		} else {
			lines, err = s4.List(prefix, *recursive, servers)
			panic1(err)
		}
	case 0:
		lines, err = s4.ListBuckets(servers)
		panic1(err)
	default:
		usage()
	}
	if len(lines) == 0 {
		os.Exit(1)
	}
	for _, line := range lines {
		panic2(fmt.Println(strings.Join(line, " ")))
	}
}

func Cp() {
	flg := flag.NewFlagSet("cp", flag.ExitOnError)
	usage := func() {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 cp SRC DST [-r] [-c]"))
		flg.PrintDefaults()
		os.Exit(1)
	}
	recursive := flg.Bool("r", false, "recursive")
	conf_path := flg.String("c", lib.DefaultConfPath(), "conf-path")
	if lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		usage()
	}
	panic1(flg.Parse(os.Args[2:]))
	if flg.NArg() != 2 {
		usage()
	}
	src := flg.Arg(0)
	dst := flg.Arg(1)
	servers := panic2(lib.GetServers(*conf_path)).([]lib.Server)
	panic1(s4.Cp(src, dst, *recursive, servers))
}

func Health() {
	flg := flag.NewFlagSet("health", flag.ExitOnError)
	conf_path := flg.String("c", lib.DefaultConfPath(), "conf-path")
	if lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 health [-r] [-c]"))
		flg.PrintDefaults()
		os.Exit(1)
	}
	servers := panic2(lib.GetServers(*conf_path)).([]lib.Server)
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
	panic2(fmt.Println(`usage: s4 {rm,eval,ls,cp,map,map-to-n,map-from-n,health}

    rm                  delete data from s4
    eval                eval a bash cmd with key data as stdin
    ls                  list keys
    cp                  copy data to or from s4
    map                 process data
    map-to-n            shuffle data
    map-from-n          merge shuffled data
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
