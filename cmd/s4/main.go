package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"

	"os"
	"path"
	"path/filepath"
	"s4/lib"
	"sort"
	"strings"
	"time"
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
	assert(strings.HasPrefix(prefix, "s4://"), prefix)
	if *recursive {
		results := make(chan *lib.HttpResult)
		for _, server := range lib.Servers() {
			go func(server lib.Server) {
				results <- lib.Post(fmt.Sprintf("http://%s:%s/delete?prefix=%s&recursive=true", server.Address, server.Port, prefix), "application/text", bytes.NewBuffer([]byte{}))
			}(server)
		}
		for range lib.Servers() {
			result := <-results
			assert(result.Err == nil, fmt.Sprint(result.Err))
			assert(result.StatusCode == 200, "%d %s", result.StatusCode, result.Body)
		}
	} else {
		server := panic2(lib.PickServer(prefix)).(*lib.Server)
		result := lib.Post(fmt.Sprintf("http://%s:%s/delete?prefix=%s", server.Address, server.Port, prefix), "application/text", bytes.NewBuffer([]byte{}))
		assert(result.Err == nil, fmt.Sprint(result.Err))
		assert(result.StatusCode == 200, "%s", result.Body)
	}
}

func Map() {
	if len(os.Args) != 5 || lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 map INDIR OUTDIR CMD"))
		os.Exit(1)
	}
	indir := os.Args[2]
	outdir := os.Args[3]
	cmd := os.Args[4]
	indir, glob := lib.ParseGlob(indir)
	assert(strings.HasSuffix(indir, "/"), indir)
	assert(strings.HasSuffix(outdir, "/"), outdir)
	lines := list(indir, true)
	pth := strings.SplitN(indir, "://", 2)[1]
	pth = strings.SplitN(pth, "/", 2)[1]
	datas := make(map[string][][]string)
	for _, line := range lines {
		size := line[2]
		key := line[3]
		if pth != "" {
			key = strings.SplitN(key, pth, 2)[1]
		}
		if size == "PRE" {
			continue
		}
		if glob != "" && !panic2(path.Match(glob, key)).(bool) {
			continue
		}
		inkey := lib.Join(indir, key)
		outkey := lib.Join(outdir, key)
		server, err := lib.PickServer(inkey)
		panic1(err)
		url := fmt.Sprintf("http://%s:%s/map", server.Address, server.Port)
		datas[url] = append(datas[url], []string{inkey, outkey})
	}
	var urls []Url
	for url, data := range datas {
		d := lib.MapArgs{Cmd: cmd, Args: data}
		bytes := panic2(json.Marshal(d)).([]byte)
		urls = append(urls, Url{url, bytes})
	}
	postAll(urls)
}

func MapToN() {
	if len(os.Args) != 5 || lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 map-to-n INDIR OUTDIR CMD"))
		os.Exit(1)
	}
	indir := os.Args[2]
	outdir := os.Args[3]
	cmd := os.Args[4]
	indir, glob := lib.ParseGlob(indir)
	assert(strings.HasSuffix(indir, "/"), indir)
	assert(strings.HasSuffix(outdir, "/"), outdir)
	lines := list(indir, true)
	pth := strings.SplitN(indir, "://", 2)[1]
	pth = strings.SplitN(pth, "/", 2)[1]
	datas := make(map[string][][]string)
	for _, line := range lines {
		size := line[2]
		key := line[3]
		if pth != "" {
			key = strings.SplitN(key, pth, 2)[1]
		}
		assert(size != "PRE", key)
		if glob != "" && !panic2(path.Match(glob, key)).(bool) {
			continue
		}
		inkey := lib.Join(indir, key)
		server, err := lib.PickServer(inkey)
		panic1(err)
		url := fmt.Sprintf("http://%s:%s/map_to_n", server.Address, server.Port)
		datas[url] = append(datas[url], []string{inkey, outdir})
	}
	var urls []Url
	for url, data := range datas {
		d := lib.MapArgs{Cmd: cmd, Args: data}
		bytes := panic2(json.Marshal(d)).([]byte)
		urls = append(urls, Url{url, bytes})
	}
	postAll(urls)
}

func MapFromN() {
	if len(os.Args) != 5 || lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 map-from-n INDIR OUTDIR CMD"))
		os.Exit(1)
	}
	indir := os.Args[2]
	outdir := os.Args[3]
	cmd := os.Args[4]
	indir, glob := lib.ParseGlob(indir)
	assert(strings.HasSuffix(indir, "/"), indir)
	assert(strings.HasSuffix(outdir, "/"), outdir)
	lines := list(indir, true)
	pth := strings.Split(indir, "://")[1]
	parts := strings.SplitN(pth, "/", 2)
	bucket := parts[0]
	indir = parts[1]
	buckets := make(map[string][]string)
	for _, line := range lines {
		key := line[3]
		if indir != "" {
			key = strings.SplitN(key, indir, 2)[1]
		}
		if glob != "" && !panic2(path.Match(glob, key)).(bool) {
			continue
		}
		prefix := lib.KeyPrefix(key)
		buckets[prefix] = append(buckets[prefix], lib.Join(fmt.Sprintf("s4://%s", bucket), indir, key))
	}
	datas := make(map[string][][]string)
	for _, inkeys := range buckets {
		var servers []*lib.Server
		for i, inkey := range inkeys {
			server, err := lib.PickServer(inkey)
			panic1(err)
			servers = append(servers, server)
			if i != 0 {
				assert(servers[0].Address == server.Address, "fail")
				assert(servers[0].Port == server.Port, "fail")
			}
		}
		url := fmt.Sprintf("http://%s:%s/map_from_n?outdir=%s", servers[0].Address, servers[0].Port, outdir)
		datas[url] = append(datas[url], inkeys)
	}
	var urls []Url
	for url, data := range datas {
		d := lib.MapArgs{Cmd: cmd, Args: data}
		bytes := panic2(json.Marshal(d)).([]byte)
		urls = append(urls, Url{url, bytes})
	}
	postAll(urls)
}

type Url struct {
	Url        string
	MapArgs []byte
}

type HttpResultUrl struct {
	StatusCode int
	Body       []byte
	Err        error
	Url        Url
}

func postAll(urls []Url) {
	results := make(chan *HttpResultUrl, len(urls))
	for _, url := range urls {
		go func(url Url) {
			result := lib.Post(url.Url, "application/json", bytes.NewBuffer(url.MapArgs))
			results <- &HttpResultUrl{result.StatusCode, result.Body, result.Err, url}
		}(url)
	}
	for range urls {
		result := <-results
		assert(result.Err == nil, "%s", result.Err)
		if result.StatusCode != 200 {
			fmt.Printf("fatal: %d %s\n", result.StatusCode, result.Url.Url)
			fmt.Println(result.Body)
			os.Exit(1)
		}
		fmt.Printf("ok ")
	}
	fmt.Println("")
}

func Eval() {
	if len(os.Args) != 4 || lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 eval KEY CMD"))
		os.Exit(1)
	}
	key := os.Args[2]
	cmd := os.Args[3]
	server, err := lib.PickServer(key)
	panic1(err)
	url := fmt.Sprintf("http://%s:%s/eval?key=%s", server.Address, server.Port, key)
	result := lib.Post(url, "application/text", bytes.NewBuffer([]byte(cmd)))
	assert(result.Err == nil, "%s", result.Err)
	switch result.StatusCode {
	case 404:
		panic2(fmt.Fprintln(os.Stderr, "fatal: no such key"))
		os.Exit(1)
	case 200:
		panic2(os.Stdout.Write(result.Body))
	default:
		panic(fmt.Sprintf("%d %s", result.StatusCode, result.Body))
	}
}

func Ls() {
	cmd := flag.NewFlagSet("ls", flag.ExitOnError)
	recursive := cmd.Bool("r", false, "recursive")
	if lib.Contains(os.Args, "-h") || lib.Contains(os.Args, "--help") {
		panic2(fmt.Fprintln(os.Stderr, "usage: s4 ls [PREFIX] [-r]"))
		cmd.PrintDefaults()
		os.Exit(1)
	}
	panic1(cmd.Parse(os.Args[2:]))
	var lines [][]string
	switch cmd.NArg() {
	case 1:
		prefix := cmd.Arg(0)
		val := strings.SplitN(prefix, "://", 2)[1]
		if !*recursive && strings.Count(val, "/") == 0 {
			for _, line := range listBuckets() {
				if lib.Contains(line, val) {
					lines = [][]string{line}
					break
				}
			}
		} else {
			lines = list(prefix, *recursive)
		}
	case 0:
		lines = listBuckets()
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

func listBuckets() [][]string {
	results := make(chan *lib.HttpResult)
	for _, server := range lib.Servers() {
		go func(server lib.Server) {
			results <- lib.Get(fmt.Sprintf("http://%s:%s/list_buckets", server.Address, server.Port))
		}(server)
	}
	buckets := make(map[string][]string)
	for range lib.Servers() {
		result := <-results
		assert(result.Err == nil, fmt.Sprint(result.Err))
		assert(result.StatusCode == 200, "%s", result.Body)
		var res [][]string
		panic1(json.Unmarshal(result.Body, &res))
		for _, line := range res {
			path := line[3]
			buckets[path] = line
		}
	}
	var lines [][]string
	var keys []string
	for k := range buckets {
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
	results := make(chan *lib.HttpResult)
	for _, server := range lib.Servers() {
		go func(server lib.Server) {
			results <- lib.Get(fmt.Sprintf("http://%s:%s/list?prefix=%s%s", server.Address, server.Port, prefix, recursive_param))
		}(server)
	}
	var lines [][]string
	for range lib.Servers() {
		result := <-results
		assert(result.Err == nil, fmt.Sprint(result.Err))
		assert(result.StatusCode == 200, "%s", result.Body)
		var tmp [][]string
		panic1(json.Unmarshal(result.Body, &tmp))
		lines = append(lines, tmp...)
	}
	sort.Slice(lines, func(i, j int) bool { return lines[i][3] < lines[j][3] })
	var deduped [][]string
	for _, val := range lines {
		if len(deduped) == 0 || deduped[len(deduped)-1][3] != val[3] {
			deduped = append(deduped, val)
		}
	}
	return deduped
}

func cp(src string, dst string, recursive bool) {
	assert(!(strings.HasPrefix(src, "s4://") && strings.HasPrefix(dst, "s4://")), "fatal: there is no move, there is only cp and rm.")
	assert(!strings.Contains(src, " ") && !strings.Contains(dst, " "), "fatal: spaces in keys are not allowed")
	assert(!strings.HasPrefix(src, "s4://") || !strings.HasPrefix(strings.SplitN(src, "s4://", 2)[1], "_"), "fatal: buckets cannot start with underscore")
	assert(!strings.HasPrefix(dst, "s4://") || !strings.HasPrefix(strings.SplitN(dst, "s4://", 2)[1], "_"), "fatal: buckets cannot start with underscore")
	if recursive {
		if strings.HasPrefix(src, "s4://") {
			getRecursive(src, dst)
		} else if strings.HasPrefix(dst, "s4://") {
			putRecursive(src, dst)
		} else {
			panic("fatal: src or dst needs s4://")
		}
	} else if strings.HasPrefix(src, "s4://") {
		get(src, dst)
	} else if strings.HasPrefix(dst, "s4://") {
		panic1(lib.Put(src, dst))
	} else {
		panic("fatal: src or dst needs s4://")
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
	cp(src, dst, *recursive)
}

func getRecursive(src string, dst string) {
	part := strings.SplitN(src, "s4://", 2)[1]
	part = strings.TrimRight(part, "/")
	parts := strings.Split(part, "/")
	bucket := parts[0]
	parts = parts[1:]
	prefix := fmt.Sprintf("%s/", bucket)
	if len(parts) != 0 {
		prefix = strings.Join(parts, "/")
	}
	for _, line := range list(src, true) {
		key := line[3]
		token := prefix
		if dst == "." {
			token = lib.Dir(prefix)
		}
		if token == "" {
			token = " "
		}
		pth := lib.Last(strings.SplitN(key, token, 2))
		pth = strings.TrimLeft(pth, " /")
		pth = lib.Join(dst, pth)
		if lib.Dir(pth) != "" {
			panic1(os.MkdirAll(lib.Dir(pth), os.ModePerm))
		}
		cp(fmt.Sprintf("s4://%s", lib.Join(bucket, key)), pth, false)
	}
}

func putRecursive(src string, dst string) {
	panic1(filepath.Walk(src, func(fullpath string, info os.FileInfo, err error) error {
		panic1(err)
		if !info.IsDir() {
			src = strings.TrimRight(src, "/")
			file := path.Base(fullpath)
			dirpath := lib.Dir(fullpath)
			pth := strings.TrimLeft(lib.Last(strings.SplitN(dirpath, src, 2)), "/")
			cp(lib.Join(dirpath, file), lib.Join(dst, pth, file), false)
		}
		return nil
	}))
}

func get(src string, dst string) {
	server, err := lib.PickServer(src)
	panic1(err)
	port := make(chan string, 1)
	done := make(chan error)
	temp_path := fmt.Sprintf("%s.temp", dst)
	defer func() { _ = os.Remove(temp_path) }()
	var client_checksum string
	go func() {
		if dst == "-" {
			client_checksum = panic2(lib.Recv(os.Stdout, port)).(string)
		} else {
			client_checksum = panic2(lib.RecvFile(temp_path, port)).(string)
		}
		done <- nil
	}()
	url := fmt.Sprintf("http://%s:%s/prepare_get?key=%s&port=%s", server.Address, server.Port, src, <-port)
	result := lib.Post(url, "application/text", bytes.NewBuffer([]byte{}))
	assert(result.StatusCode != 404, fmt.Sprintf("fatal: no such key: %s", src))
	assert(result.StatusCode == 200, "%s", result.Body)
	uid := result.Body
	<-done
	url = fmt.Sprintf("http://%s:%s/confirm_get?uuid=%s&checksum=%s", server.Address, server.Port, uid, client_checksum)
	result = lib.Post(url, "application/text", bytes.NewBuffer([]byte{}))
	assert(result.StatusCode == 200, "%s", result.Body)
	if strings.HasSuffix(dst, "/") {
		panic1(os.MkdirAll(dst, os.ModePerm))
		dst = lib.Join(dst, path.Base(src))
	} else if dst == "." {
		dst = path.Base(src)
	}
	if dst != "-" {
		panic1(os.Rename(temp_path, dst))
	}
}

func Config() {
	for _, server := range lib.Servers() {
		fmt.Printf("%s:%s\n", server.Address, server.Port)
	}
}

func Health() {
	servers := lib.Servers()
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

func assert(cond bool, format string, a ...interface{}) {
	if !cond {
		fmt.Fprintf(os.Stderr, format, a...)
		os.Exit(1)
	}
}

func panic1(e error) {
	if e != nil {
		panic(e)
	}
}

func panic2(x interface{}, e error) interface{} {
	if e != nil {
		panic(e)
	}
	return x
}
