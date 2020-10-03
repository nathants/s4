package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"s4/lib"
	"sort"
	"strings"

	"github.com/phayes/freeport"
)

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

type Result struct {
	resp *http.Response
	err  error
}

func Rm() {
	cmd := flag.NewFlagSet("rm", flag.ExitOnError)
	recursive := cmd.Bool("r", false, "recursive")
	Panic1(cmd.Parse(os.Args[2:]))
	if cmd.NArg() != 1 {
		Panic2(fmt.Fprintln(os.Stderr, "usage: s4 rm PREFIX [-r]"))
		cmd.Usage()
		os.Exit(1)
	}
	prefix := cmd.Arg(0)
	Assert(strings.HasPrefix(prefix, "s4://"), prefix)
	if *recursive {
		results := make(chan Result)
		for _, server := range lib.Servers() {
			go func(server lib.Server) {
				url := fmt.Sprintf("http://%s:%s/delete?prefix=%s&recursive=true", server.Address, server.Port, prefix)
				resp, err := http.Post(url, "application/text", bytes.NewBuffer([]byte{}))
				results <- Result{resp, err}
			}(server)
		}
		for range lib.Servers() {
			result := <-results
			Assert(result.err == nil, fmt.Sprint(result.err))
			Assert(result.resp.StatusCode == 200, string(Panic2(ioutil.ReadAll(result.resp.Body)).([]byte)))
		}
	} else {
		server := lib.PickServer(prefix)
		url := fmt.Sprintf("http://%s:%s/delete?prefix=%s", server.Address, server.Port, prefix)
		resp, err := http.Post(url, "application/text", bytes.NewBuffer([]byte{}))
		Assert(err == nil, fmt.Sprint(err))
		Assert(resp.StatusCode == 200, string(Panic2(ioutil.ReadAll(resp.Body)).([]byte)))
	}
}

func parseGlob(indir string) []string {
	glob := ""
	if strings.Contains(indir, "*") {
		var base []string
		var pattern []string
		switched := false
		for _, part := range strings.Split(indir, "/") {
			if strings.Contains(part, "*") {
				switched = true
			}
			if switched {
				pattern = append(pattern, part)
			} else {
				base = append(base, part)
			}
		}
		indir = strings.Join(base, "/") + "/"
		glob = strings.Join(pattern, "/")
	}
	return []string{indir, glob}
}

func Map() {
	if len(os.Args) != 5 {
		Panic2(fmt.Fprintln(os.Stderr, "usage: s4 map INDIR OUTDIR CMD"))
		os.Exit(1)
	}
	indir := os.Args[2]
	outdir := os.Args[3]
	cmd := os.Args[4]

	parts := parseGlob(indir)
	indir = parts[0]
	glob := parts[1]

	Assert(strings.HasSuffix(indir, "/"), indir)
	Assert(strings.HasSuffix(outdir, "/"), outdir)

	lines := list(indir, true)

	parts = strings.Split(indir, "://")
	// proto := parts[0]
	pth := parts[1]

	parts = strings.SplitN(pth, "/", 2)
	// bucket := parts[0]
	pth = parts[1]

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
		if glob != "" && !Panic2(path.Match(glob, key)).(bool) {
			continue
		}
		inkey := lib.Join(indir, key)
		outkey := lib.Join(outdir, key)
		server := lib.PickServer(inkey)
		url := fmt.Sprintf("http://%s:%s/map", server.Address, server.Port)
		datas[url] = append(datas[url], []string{inkey, outkey})
	}
	var urls []Url
	for url, data := range datas {
		d := Data{cmd, data}
		bytes := Panic2(json.Marshal(d)).([]byte)
		urls = append(urls, Url{url, bytes})
	}
	postAll(urls)
}

type UrlResult struct {
	resp *http.Response
	err  error
	url  Url
}

func postAll(urls []Url) {
	results := make(chan UrlResult, len(urls))
	for _, url := range urls {
		go func(url Url) {
			resp, err := http.Post(url.Url, "application/json", bytes.NewBuffer(url.Data))
			results <- UrlResult{resp, err, url}
		}(url)
	}
	for range urls {
		result := <-results
		body := Panic2(ioutil.ReadAll(result.resp.Body)).([]byte)
		if result.err != nil {
			panic(result.err)
		}
		switch result.resp.StatusCode {
		case 400, 409:
			fmt.Printf("fatal: cmd failure %s\n", result.url.Url)
			fmt.Println(string(body))
			os.Exit(1)
		default:
			Assert(result.resp.StatusCode == 200, result.url.Url)
			fmt.Printf("ok ")
		}
	}
	fmt.Println("")
}

type Data struct {
	Cmd string `json:"cmd"`

	Args [][]string `json:"args"`
}

type Url struct {
	Url  string
	Data []byte
}

func Eval() {
	if len(os.Args) != 4 {
		Panic2(fmt.Fprintln(os.Stderr, "usage: s4 eval KEY CMD"))
		os.Exit(1)
	}
	key := os.Args[2]
	cmd := os.Args[3]
	server := lib.PickServer(key)
	url := fmt.Sprintf("http://%s:%s/eval?key=%s", server.Address, server.Port, key)
	resp, err := http.Post(url, "application/text", bytes.NewBuffer([]byte(cmd)))
	Assert(err == nil, fmt.Sprint(err))
	switch resp.StatusCode {
	case 404:
		Panic2(fmt.Fprintln(os.Stderr, "fatal: no such key"))
		os.Exit(1)
	case 400:
		var result lib.Result
		bytes := Panic2(ioutil.ReadAll(resp.Body)).([]byte)
		if len(bytes) == 0 {
			return
		}
		Panic1(json.Unmarshal(bytes, &result))
		Panic2(fmt.Fprintln(os.Stderr, result.Stdout))
		Panic2(fmt.Fprintln(os.Stderr, result.Stderr))
		Panic2(fmt.Fprintf(os.Stderr, "%s\n", result.Err))
	case 200:
		bytes := Panic2(ioutil.ReadAll(resp.Body)).([]byte)
		Panic2(os.Stdout.Write(bytes))
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
	recursive := cmd.Bool("r", false, "recursive")
	Panic1(cmd.Parse(os.Args[2:]))
	if contains(os.Args, "-h") || contains(os.Args, "--help") {
		Panic2(fmt.Fprintln(os.Stderr, "usage: s4 ls [PREFIX] [-r]"))
		cmd.Usage()
		os.Exit(1)
	}
	var lines [][]string
	switch cmd.NArg() {
	case 1:
		prefix := cmd.Arg(0)
		val := strings.SplitN(prefix, "://", 2)[1]
		if !*recursive && strings.Count(val, "/") == 0 {
			for _, line := range list_buckets() {
				if contains(line, val) {
					lines = [][]string{line}
					break
				}
			}
		} else {
			lines = list(prefix, *recursive)
		}
	case 0:
		lines = list_buckets()
	default:
		Panic2(fmt.Fprintln(os.Stderr, "usage: s4 ls [PREFIX] [-r]"))
		cmd.Usage()
		os.Exit(1)
	}
	if len(lines) == 0 {
		os.Exit(1)
	}
	for _, line := range lines {
		Panic2(fmt.Println(strings.Join(line, " ")))
	}
}

func list_buckets() [][]string {
	results := make(chan Result)
	for _, server := range lib.Servers() {
		go func(server lib.Server) {
			url := fmt.Sprintf("http://%s:%s/list_buckets", server.Address, server.Port)
			resp, err := http.Get(url)
			results <- Result{resp, err}
		}(server)
	}
	buckets := make(map[string][]string)
	for range lib.Servers() {
		result := <-results
		Assert(result.err == nil, fmt.Sprint(result.err))
		bytes := Panic2(ioutil.ReadAll(result.resp.Body)).([]byte)
		Assert(result.resp.StatusCode == 200, string(bytes))
		var res [][]string
		Panic1(json.Unmarshal(bytes, &res))
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
	results := make(chan Result)
	for _, server := range lib.Servers() {
		go func(server lib.Server) {
			url := fmt.Sprintf("http://%s:%s/list?prefix=%s%s", server.Address, server.Port, prefix, recursive_param)
			resp, err := http.Get(url)
			results <- Result{resp, err}
		}(server)
	}
	var lines [][]string
	for range lib.Servers() {
		result := <-results
		Assert(result.err == nil, fmt.Sprint(result.err))
		bytes := Panic2(ioutil.ReadAll(result.resp.Body)).([]byte)
		Assert(result.resp.StatusCode == 200, string(bytes))
		var tmp [][]string
		Panic1(json.Unmarshal(bytes, &tmp))
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
	Assert(!(strings.HasPrefix(src, "s4://") && strings.HasPrefix(dst, "s4://")), "fatal: there is no move, there is only cp and rm.")
	Assert(!strings.Contains(src, " ") && !strings.Contains(dst, " "), "fatal: spaces in keys are not allowed")
	Assert(!strings.HasPrefix(src, "s4://") || !strings.HasPrefix(strings.SplitN(src, "s4://", 2)[1], "_"), "fatal: buckets cannot start with underscore")
	Assert(!strings.HasPrefix(dst, "s4://") || !strings.HasPrefix(strings.SplitN(dst, "s4://", 2)[1], "_"), "fatal: buckets cannot start with underscore")
	if recursive {
		if strings.HasPrefix(src, "s4://") {
			get_recursive(src, dst)
		} else if strings.HasPrefix(dst, "s4://") {
			put_recursive(src, dst)
		} else {
			panic("fatal: src or dst needs s4://")
		}
	} else if strings.HasPrefix(src, "s4://") {
		get(src, dst)
	} else if strings.HasPrefix(dst, "s4://") {
		put(src, dst)
	} else {
		panic("fatal: src or dst needs s4://")
	}
}

func Cp() {
	cmd := flag.NewFlagSet("cp", flag.ExitOnError)
	recursive := cmd.Bool("r", false, "recursive")
	Panic1(cmd.Parse(os.Args[2:]))
	if cmd.NArg() != 2 || contains(os.Args, "-h") || contains(os.Args, "--help") {
		Panic2(fmt.Fprintln(os.Stderr, "usage: s4 cp SRC DST [-r]"))
		cmd.Usage()
		os.Exit(1)
	}
	src := cmd.Arg(0)
	dst := cmd.Arg(1)
	cp(src, dst, *recursive)
}

func get_recursive(src string, dst string) {
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
		pths := strings.SplitN(key, token, 2)
		pth := pths[len(pths)-1]
		pth = strings.TrimLeft(pth, " /")
		pth = lib.Join(dst, pth)
		if lib.Dir(pth) != "" {
			Panic1(os.MkdirAll(lib.Dir(pth), os.ModePerm))
		}
		cp(fmt.Sprintf("s4://%s", lib.Join(bucket, key)), pth, false)
	}
}

func put_recursive(src string, dst string) {
	Panic1(filepath.Walk(src, func(fullpath string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			src = strings.TrimRight(src, "/")
			file := path.Base(fullpath)
			dirpath := lib.Dir(fullpath)
			parts := strings.SplitN(dirpath, src, 2)
			pth := strings.TrimLeft(parts[len(parts)-1], "/")
			cp(lib.Join(dirpath, file), lib.Join(dst, pth, file), false)
		}
		return nil
	}))
}

func get(src string, dst string) {
	server := lib.PickServer(src)
	port := Panic2(freeport.GetFreePort()).(int)
	temp_path := fmt.Sprintf("%s.temp", dst)
	url := fmt.Sprintf("http://%s:%s/prepare_get?key=%s&port=%d", server.Address, server.Port, src, port)
	resp := Panic2(http.Post(url, "application/text", bytes.NewBuffer([]byte{}))).(*http.Response)
	Assert(resp.StatusCode != 404, fmt.Sprintf("fatal: no such key: %s", src))
	_bytes := Panic2(ioutil.ReadAll(resp.Body)).([]byte)
	Assert(resp.StatusCode == 200, string(_bytes))
	uid := _bytes
	var cmd string
	if dst == "-" {
		cmd = fmt.Sprintf("s4-recv %d | s4-xxh --stream", port)
	} else {
		Assert(!lib.Exists(temp_path), temp_path)
		cmd = fmt.Sprintf("s4-recv %d | s4-xxh --stream > %s", port, temp_path)
	}
	result := lib.WarnStreamOut(cmd)
	Assert(result.Err == nil, fmt.Sprint(result.Err))
	client_checksum := result.Stderr
	url = fmt.Sprintf("http://%s:%s/confirm_get?uuid=%s&checksum=%s", server.Address, server.Port, uid, client_checksum)
	resp = Panic2(http.Post(url, "application/text", bytes.NewBuffer([]byte{}))).(*http.Response)
	Assert(resp.StatusCode == 200, string(Panic2(ioutil.ReadAll(resp.Body)).([]byte)))
	if strings.HasSuffix(dst, "/") {
		Panic1(os.MkdirAll(dst, os.ModePerm))
		dst = lib.Join(dst, path.Base(src))
	} else if dst == "." {
		dst = path.Base(src)
	}
	if dst != "-" {
		Panic1(os.Rename(temp_path, dst))
	}
	_ = os.Remove(temp_path)
}

func put(src string, dst string) {
	if strings.HasSuffix(dst, "/") {
		dst = lib.Join(dst, path.Base(src))
	}
	server := lib.PickServer(dst)
	url := fmt.Sprintf("http://%s:%s/prepare_put?key=%s", server.Address, server.Port, dst)
	resp := Panic2(http.Post(url, "application/text", bytes.NewBuffer([]byte{}))).(*http.Response)
	Assert(resp.StatusCode != 409, fmt.Sprintf("fatal: key already exists: %s", dst))
	_bytes := Panic2(ioutil.ReadAll(resp.Body)).([]byte)
	Assert(resp.StatusCode == 200, string(_bytes))
	vals := strings.Split(string(_bytes), " ")
	Assert(len(vals) == 2, fmt.Sprint(vals))
	uid := vals[0]
	port := vals[1]
	var result *lib.Result
	if src == "-" {
		result = lib.WarnStreamIn("s4-xxh --stream | s4-send %s %s", server.Address, port)
	} else {
		result = lib.Warn("< %s s4-xxh --stream | s4-send %s %s", src, server.Address, port)
	}
	Assert(result.Err == nil, fmt.Sprint(result.Err))
	client_checksum := result.Stderr
	url = fmt.Sprintf("http://%s:%s/confirm_put?uuid=%s&checksum=%s", server.Address, server.Port, uid, client_checksum)
	resp = Panic2(http.Post(url, "application/text", bytes.NewBuffer([]byte{}))).(*http.Response)
	Assert(resp.StatusCode == 200, string(Panic2(ioutil.ReadAll(resp.Body)).([]byte)))
}

func Config() {
	for _, server := range lib.Servers() {
		fmt.Printf("%s:%s\n", server.Address, server.Port)
	}
}

func Health() {
	servers := lib.Servers()
	results := make(chan string, len(servers))
	for _, server := range servers {
		go func(server lib.Server) {
			url := fmt.Sprintf("http://%s:%s/health", server.Address, server.Port)
			resp, err := http.Get(url)
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

func usage() {
	Panic2(fmt.Println("usage:"))
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "rm":
		Rm()
	case "map":
		Map()
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
