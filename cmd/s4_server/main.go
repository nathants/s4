package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"s4/lib"
	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/julienschmidt/httprouter"
	"github.com/phayes/freeport"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/sync/semaphore"
)

var (
	num_cpus     = runtime.GOMAXPROCS(0)
	max_io_jobs  = num_cpus * 4
	max_cpu_jobs = num_cpus + 2
	io_send_pool = semaphore.NewWeighted(int64(max_io_jobs))
	io_recv_pool = semaphore.NewWeighted(int64(max_io_jobs))
	cpu_pool     = semaphore.NewWeighted(int64(max_cpu_jobs))
	misc_pool    = semaphore.NewWeighted(int64(max_cpu_jobs))
	solo_pool    = semaphore.NewWeighted(int64(1))
	io_jobs      = sync.Map{}
)

func with(pool *semaphore.Weighted, fn func()) {
	Panic1(pool.Acquire(context.Background(), 1))
	fn()
	pool.Release(1)
}

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

type GetJob struct {
	start         time.Time
	result        chan *lib.CmdResult
	disk_checksum string
}

type PutJob struct {
	start     time.Time
	result    chan *lib.CmdResult
	path      string
	temp_path string
}

func ConfirmGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	//
	uids := r.URL.Query()["uuid"]
	Assert(len(uids) == 1, "missing query parameter: uuid")
	uid := uids[0]
	//
	client_checksums := r.URL.Query()["checksum"]
	Assert(len(client_checksums) == 1, "missing query parameter: checksum")
	client_checksum := client_checksums[0]
	//
	v, ok := io_jobs.LoadAndDelete(uid)
	Assert(ok, uid)
	job := v.(*GetJob)
	//
	result := <-job.result
	Assert(result.Err == nil, result.Stdout+"\n"+result.Stderr)
	disk_checksum := job.disk_checksum
	server_checksum := result.Stderr
	Assert(client_checksum == server_checksum && server_checksum == disk_checksum, "checksum mismatch: %s %s %s\n", client_checksum, server_checksum, disk_checksum)
	w.WriteHeader(200)
}

func PrepareGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	//
	ports := r.URL.Query()["port"]
	Assert(len(ports) == 1, "missing query paramter: port")
	port := ports[0]
	//
	keys := r.URL.Query()["key"]
	Assert(len(keys) == 1, "missing query parameter: key")
	key := keys[0]
	//
	Assert(lib.OnThisServer(key), "wrong server for request\n")
	remote := strings.Split(r.RemoteAddr, ":")[0]
	if remote == "127.0.0.1" {
		remote = "0.0.0.0"
	}
	path := strings.Split(key, "s4://")[1]
	var exists bool
	with(solo_pool, func() { exists = lib.Exists(path) })
	if !exists {
		w.WriteHeader(404)
		return
	}
	uid := uuid.NewV4().String()
	started := make(chan bool, 1)
	result := make(chan *lib.CmdResult, 1)
	go with(io_send_pool, func() {
		started <- true
		result <- lib.Warn("< %s s4-xxh --stream | s4-send %s %s", path, remote, port)
	})
	// solo pool
	var disk_checksum string
	with(solo_pool, func() { disk_checksum = checksumRead(path) })
	//
	job := &GetJob{
		time.Now(),
		result,
		disk_checksum,
	}
	_, loaded := io_jobs.LoadOrStore(uid, job)
	Assert(!loaded, uid)
	select {
	case <-time.After(lib.Timeout):
		io_jobs.Delete(uid) // TODO kill process
		w.WriteHeader(429)
	case <-started:
		Panic2(w.Write([]byte(uid)))
	}
}

func PreparePut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	//
	keys := r.URL.Query()["key"]
	Assert(len(keys) == 1, "missing query parameter: key")
	key := keys[0]
	//
	Assert(!strings.Contains(key, " "), "key contains spaces: %s\n", key)
	Assert(lib.OnThisServer(key), "wronger server for request")
	path := strings.Split(key, "s4://")[1]
	Assert(!strings.HasPrefix(path, "_"), path)
	// solo pool
	var port int
	var exists bool
	var temp_path string
	with(solo_pool, func() {
		port = Panic2(freeport.GetFreePort()).(int)
		exists = lib.Exists(path)
		temp_path = lib.NewTempPath("_tempfiles")
	})
	//
	if exists {
		w.WriteHeader(409)
		return
	}
	uid := uuid.NewV4().String()
	started := make(chan bool, 1)
	result := make(chan *lib.CmdResult, 1)
	go with(io_recv_pool, func() {
		started <- true
		result <- lib.Warn("s4-recv %d | s4-xxh --stream > %s", port, temp_path)
	})
	job := &PutJob{
		time.Now(),
		result,
		path,
		temp_path,
	}
	_, loaded := io_jobs.LoadOrStore(uid, job)
	Assert(!loaded, uid)
	select {
	case <-time.After(lib.Timeout):
		io_jobs.Delete(uid) // TODO kill process
		_ = os.Remove(path)
		_ = os.Remove(checksumPath(path))
		w.WriteHeader(429)
	case <-started:
		w.Header().Set("Content-Type", "application/text")
		Panic2(fmt.Fprintf(w, "%s %d", uid, port))
	}
}

func ConfirmPut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	uids := r.URL.Query()["uuid"]
	Assert(len(uids) == 1, "missing query parameter: uuid")
	uid := uids[0]
	client_checksums := r.URL.Query()["checksum"]
	Assert(len(client_checksums) == 1, "missing query parameter: checksum")
	client_checksum := client_checksums[0]
	v, ok := io_jobs.LoadAndDelete(uid)
	Assert(ok, "no such job: %s", uid)
	job := v.(*PutJob)
	result := <-job.result
	Assert(result.Err == nil, result.Stdout+"\n"+result.Stderr)
	server_checksum := result.Stderr
	Assert(client_checksum == server_checksum, "checksum mismatch: %s %s\n", client_checksum, server_checksum)
	with(solo_pool, func() {
		Panic1(os.MkdirAll(lib.Dir(job.path), os.ModePerm))
		Assert(!lib.Exists(job.path), job.path)
		Panic1(ioutil.WriteFile(checksumPath(job.path), []byte(server_checksum), 0o444))
		Panic1(os.Chmod(job.temp_path, 0o444))
		Panic1(os.Rename(job.temp_path, job.path))
	})
	w.WriteHeader(200)
}

func Delete(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	prefixes := r.URL.Query()["prefix"]
	Assert(len(prefixes) == 1, "missing query parameter: prefix")
	prefix := prefixes[0]
	parts := strings.SplitN(prefix, "s4://", 2)
	prefix = parts[len(parts)-1]
	Assert(!strings.HasPrefix(prefix, "/"), prefix)
	recursive := false
	recursives := r.URL.Query()["recursive"]
	if len(recursives) == 1 && recursives[0] == "true" {
		recursive = true
	}
	with(solo_pool, func() {
		if recursive {
			lib.Run("rm -rf %s*", prefix)
		} else {
			lib.Run("rm -rf %s %s", prefix, checksumPath(prefix))
		}
	})
}

func checksumPath(prefix string) string {
	Assert(!strings.HasSuffix(prefix, "/"), prefix)
	return fmt.Sprintf("%s.xxh3", prefix)
}

type MapResult struct {
	CmdResult *lib.CmdResultTempdir
	Outkey    string
}

func Map(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	//
	var data lib.Data
	bytes := Panic2(ioutil.ReadAll(r.Body)).([]byte)
	Panic1(json.Unmarshal(bytes, &data))
	if strings.HasPrefix(data.Cmd, "while read") {
		data.Cmd = fmt.Sprintf("cat | %s", data.Cmd)
	}
	//
	results := make(chan MapResult, len(data.Args))
	for _, arg := range data.Args {
		Assert(len(arg) == 2, fmt.Sprint(arg))
		inkey := arg[0]
		outkey := arg[1]
		Assert(lib.OnThisServer(inkey), inkey)
		Assert(lib.OnThisServer(outkey), outkey)
		inpath := Panic2(filepath.Abs(strings.SplitN(inkey, "s4://", 2)[1])).(string)
		go func(inpath string) {
			with(cpu_pool, func() {
				results <- MapResult{
					lib.WarnTempdir(fmt.Sprintf("export filename=%s; < %s %s > output", path.Base(inpath), inpath, data.Cmd)),
					outkey,
				}
			})
		}(inpath)
	}
	//
	var tempdirs []string
	defer func() {
		for _, tempdir := range tempdirs {
			Panic1(os.RemoveAll(tempdir))
		}
	}()
	//
	max_timeout := time.After(lib.MaxTimeout)
	puts := make(chan error, len(data.Args))
	for range data.Args {
		select {
		case result := <-results:
			tempdirs = append(tempdirs, result.CmdResult.Tempdir)
			if result.CmdResult.Err != nil {
				w.WriteHeader(400)
				Panic2(fmt.Fprint(w, result.CmdResult.Stdout+"\n"+result.CmdResult.Stderr))
				return
			} else {
				go func(result MapResult) {
					temp_path := lib.Join(result.CmdResult.Tempdir, "output")
					localPut(temp_path, result.Outkey)
					puts <- nil
				}(result)
			}
		case <-max_timeout:
			w.WriteHeader(429)
			return
		}
	}
	//
	for range data.Args {
		select {
		case <-puts:
		case <-max_timeout:
			w.WriteHeader(429)
			return
		}
	}
	w.WriteHeader(200)
}

func localPut(temp_path string, key string) {
	Assert(!strings.Contains(key, " "), key)
	Assert(lib.OnThisServer(key), key)
	parts := strings.SplitN(key, "s4://", 2)
	path := parts[len(parts)-1]
	Assert(!strings.HasPrefix(path, "_"), path)
	var checksum string
	with(misc_pool, func() { checksum = xxhChecksum(temp_path) })
	with(solo_pool, func() { confirmLocalPut(temp_path, path, checksum) })
}

func confirmLocalPut(temp_path string, path string, checksum string) {
	//
	Panic1(os.MkdirAll(lib.Dir(path), os.ModePerm))
	Assert(!lib.Exists(path), fmt.Sprintf("fatal: key already exists s4://%s", path))
	Assert(!lib.Exists(checksumPath(path)), checksumPath(path))
	Panic1(os.Chmod(temp_path, 0o444))
	Panic1(os.Rename(temp_path, path))
	checksumWrite(path, checksum)
}

func checksumWrite(path string, checksum string) {
	Panic1(ioutil.WriteFile(checksumPath(path), []byte(checksum), 0o444))
}

func checksumRead(path string) string {
	return string(Panic2(ioutil.ReadFile(checksumPath(path))).([]byte))
}

func xxhChecksum(path string) string {
	return lib.Run("< %s s4-xxh", path)
}

type MapToNResult struct {
	CmdResult *lib.CmdResultTempdir
	Inpath    string
	Outdir    string
}

func MapToN(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	//
	var data lib.Data
	bytes := Panic2(ioutil.ReadAll(r.Body)).([]byte)
	Panic1(json.Unmarshal(bytes, &data))
	if strings.HasPrefix(data.Cmd, "while read") {
		data.Cmd = fmt.Sprintf("cat | %s", data.Cmd)
	}
	//
	results := make(chan MapToNResult, len(data.Args))
	for _, arg := range data.Args {
		Assert(len(arg) == 2, fmt.Sprint(arg))
		inkey := arg[0]
		outdir := arg[1]
		Assert(lib.OnThisServer(inkey), inkey)
		Assert(strings.HasPrefix(outdir, "s4://") && strings.HasSuffix(outdir, "/"), outdir)
		inpath := Panic2(filepath.Abs(strings.SplitN(inkey, "s4://", 2)[1])).(string)
		go func(inpath string) {
			with(cpu_pool, func() {
				results <- MapToNResult{
					lib.WarnTempdir(fmt.Sprintf("export filename=%s; < %s %s", path.Base(inpath), inpath, data.Cmd)),
					inpath,
					outdir,
				}
			})
		}(inpath)
	}
	//
	var tempdirs []string
	defer func() {
		for _, tempdir := range tempdirs {
			Panic1(os.RemoveAll(tempdir))
		}
	}()
	//
	max_timeout := time.After(lib.MaxTimeout)
	puts := make(chan error, len(data.Args))
	var wg sync.WaitGroup
	for range data.Args {
		select {
		case <-max_timeout:
			w.WriteHeader(429)
			return
		case result := <-results:
			tempdirs = append(tempdirs, result.CmdResult.Tempdir)
			if result.CmdResult.Err != nil {
				w.WriteHeader(400)
				Panic2(fmt.Fprint(w, result.CmdResult.Stdout+"\n"+result.CmdResult.Stderr))
				return
			} else {
				go func(result MapToNResult) {
					for _, temp_path := range strings.Split(result.CmdResult.Stdout, "\n") {
						if temp_path == "" {
							continue
						}
						wg.Add(1)
						temp_path = lib.Join(result.CmdResult.Tempdir, temp_path)
						outkey := lib.Join(result.Outdir, path.Base(result.Inpath), path.Base(temp_path))
						go func(temp_path string, outkey string) {
							if lib.OnThisServer(outkey) {
								localPut(temp_path, outkey)
							} else {
								Panic1(retry.Do(func() error {
									var err error
									with(io_send_pool, func() { err = lib.Put(temp_path, outkey) })
									Assert(!errors.Is(err, lib.Err409), "%s", err)
									return err
								}))
							}
							wg.Done()
						}(temp_path, outkey)
					}
					puts <- nil
				}(result)
			}
		}
	}
	//
	for range data.Args {
		select {
		case <-puts:
		case <-max_timeout:
			w.WriteHeader(429)
			return
		}
	}
	//
	done := make(chan error)
	go func() {
		wg.Wait()
		done <- nil
	}()
	select {
	case <-done:
	case <-max_timeout:
		w.WriteHeader(429)
		return
	}
	w.WriteHeader(200)
}

func MapFromN(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	//
	outdirs := r.URL.Query()["outdir"]
	Assert(len(outdirs) == 1, "missing query parameter: outdir")
	outdir := outdirs[0]
	Assert(strings.HasPrefix(outdir, "s4://") && strings.HasSuffix(outdir, "/"), outdir)
	//
	var data lib.Data
	bytes := Panic2(ioutil.ReadAll(r.Body)).([]byte)
	Panic1(json.Unmarshal(bytes, &data))
	if strings.HasPrefix(data.Cmd, "while read") {
		data.Cmd = fmt.Sprintf("cat | %s", data.Cmd)
	}
	//
	results := make(chan MapResult, len(data.Args))
	for _, inkeys := range data.Args {
		var inpaths []string
		for _, inkey := range inkeys {
			Assert(lib.OnThisServer(inkey), inkey)
			inpath := strings.SplitN(inkey, "s4://", 2)[1]
			inpath = Panic2(filepath.Abs(inpath)).(string)
			inpaths = append(inpaths, inpath)
		}
		outkey := lib.Join(outdir, lib.KeyPrefix(inkeys[0])+lib.Suffix(inkeys))
		go func(inpaths []string) {
			with(cpu_pool, func() {
				stdin := strings.NewReader(strings.Join(inpaths, "\n") + "\n")
				results <- MapResult{
					lib.WarnTempdirStreamIn(stdin, fmt.Sprintf("%s > output", data.Cmd)),
					outkey,
				}
			})
		}(inpaths)
	}
	//
	var tempdirs []string
	defer func() {
		for _, tempdir := range tempdirs {
			Panic1(os.RemoveAll(tempdir))
		}
	}()
	//
	max_timeout := time.After(lib.MaxTimeout)
	puts := make(chan error, len(data.Args))
	for range data.Args {
		select {
		case result := <-results:
			tempdirs = append(tempdirs, result.CmdResult.Tempdir)
			if result.CmdResult.Err != nil {
				w.WriteHeader(400)
				Panic2(fmt.Fprint(w, result.CmdResult.Stdout+"\n"+result.CmdResult.Stderr))
				return
			} else {
				go func(result MapResult) {
					temp_path := lib.Join(result.CmdResult.Tempdir, "output")
					localPut(temp_path, result.Outkey)
					puts <- nil
				}(result)
			}
		case <-max_timeout:
			w.WriteHeader(429)
			return
		}
	}
	//
	for range data.Args {
		select {
		case <-puts:
		case <-max_timeout:
			w.WriteHeader(429)
			return
		}
	}
	w.WriteHeader(200)
}

func Eval(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	keys := r.URL.Query()["key"]
	Assert(len(keys) == 1, "missing query parameter: key")
	key := keys[0]
	cmd := Panic2(ioutil.ReadAll(r.Body)).([]byte)
	parts := strings.SplitN(key, "s4://", 2)
	path := parts[len(parts)-1]
	var exists bool
	with(solo_pool, func() { exists = lib.Exists(path) })
	if !exists {
		w.WriteHeader(404)
	} else {
		with(cpu_pool, func() {
			res := lib.Warn("< %s %s", path, cmd)
			if res.Err == nil {
				Panic2(fmt.Fprintf(w, res.Stdout))
			} else {
				w.WriteHeader(400)
				w.Header().Set("Content-Type", "application/json")
				bytes := Panic2(json.Marshal(res))
				Panic2(w.Write(bytes.([]byte)))
			}
		})
	}
}

func List(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	//
	prefixes := r.URL.Query()["prefix"]
	Assert(len(prefixes) == 1, "missing query parameter: prefix")
	prefix := prefixes[0]
	//
	Assert(strings.HasPrefix(prefix, "s4://"), prefix)
	prefix = strings.Split(prefix, "s4://")[1]
	_prefix := prefix
	if !strings.HasSuffix(_prefix, "/") {
		_prefix = lib.Dir(_prefix) + "/"
	}
	//
	recursive := false
	recursives := r.URL.Query()["recursive"]
	if len(recursives) == 1 && recursives[0] == "true" {
		recursive = true
	}
	//
	var xs [][]string
	var res *lib.CmdResult
	if recursive {
		//
		if !strings.HasSuffix(prefix, "/") {
			prefix += "*"
		}
		// misc pool
		with(misc_pool, func() { res = lib.Warn("find %s -type f ! -name '*.xxh3' %s", prefix, lib.Printf) })
		//
		Assert(res.Err == nil || strings.Contains(res.Stderr, "No such file or directory"), res.Stdout+"\n"+res.Stderr)
		for _, line := range strings.Split(res.Stdout, "\n") {
			parts := strings.Split(line, " ")
			if len(parts) == 4 {
				date, time, size, path := parts[0], parts[1], parts[2], parts[3]
				xs = append(xs, []string{date, strings.Split(time, ".")[0], size, strings.Join(strings.Split(path, "/")[1:], "/")})
			}
		}
	} else {
		//
		name := ""
		if !strings.HasSuffix(prefix, "/") {
			name = path.Base(prefix)
			name = fmt.Sprintf("-name '%s*'", name)
			prefix = lib.Dir(prefix)
		}
		with(misc_pool, func() { res = lib.Warn("find %s -maxdepth 1 -type f ! -name '*.xxh3' %s %s", prefix, name, lib.Printf) })
		Assert(res.Err == nil || strings.Contains(res.Stderr, "No such file or directory"), res.Stdout+"\n"+res.Stderr)
		var files [][]string
		for _, line := range strings.Split(res.Stdout, "\n") {
			parts := strings.Split(line, " ")
			if len(parts) > 0 && len(strings.TrimSpace(parts[len(parts)-1])) > 0 {
				files = append(files, parts)
			}
		}
		with(misc_pool, func() { res = lib.Warn("find %s -mindepth 1 -maxdepth 1 -type d ! -name '*.xxh3' %s", prefix, name) })
		Assert(res.Err == nil || strings.Contains(res.Stderr, "No such file or directory"), res.Stdout+"\n"+res.Stderr)
		var dirs [][]string
		for _, line := range strings.Split(res.Stdout, "\n") {
			if line != "" {
				dirs = append(dirs, []string{"", "", "PRE", line + "/"})
			}
		}
		//
		for _, parts := range append(files, dirs...) {
			date, time, size, path := parts[0], parts[1], parts[2], parts[3]
			time = strings.Split(time, ".")[0]
			parts := strings.SplitN(path, _prefix, 2)
			path = parts[len(parts)-1]
			xs = append(xs, []string{date, time, size, path})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	bytes := Panic2(json.Marshal(xs))
	Panic2(w.Write(bytes.([]byte)))
}

func ListBuckets(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var stdout string
	with(misc_pool, func() { stdout = lib.Run("find -maxdepth 1 -mindepth 1 -type d ! -name '_*' %s", lib.Printf) })
	var res [][]string
	for _, line := range strings.Split(stdout, "\n") {
		parts := strings.Split(line, " ")
		if len(parts) == 4 {
			date, time, size, path := parts[0], parts[1], parts[2], parts[3]
			res = append(res, []string{date, time, size, strings.TrimLeft(path, "./")})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	bytes := Panic2(json.Marshal(res))
	Panic2(w.Write(bytes.([]byte)))
}

func Health(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	Panic2(fmt.Fprintf(w, "healthy\n"))
}

func PanicHandler(w http.ResponseWriter, r *http.Request, err interface{}) {
	w.WriteHeader(500)
	Panic2(fmt.Fprintf(w, "%s\n", err))
}

func main() {
	lib.Port = flag.Int("port", 0, "specify port instead of matching a single conf entry by ipv4")
	lib.Conf = flag.String("conf", "", "specify conf path to use instead of ~/.s4.conf")
	flag.Parse()
	Panic1(os.Setenv("LC_ALL", "C"))
	if !lib.Exists("s4_data") {
		Panic1(os.MkdirAll("s4_data/_tempfiles", os.ModePerm))
		Panic1(os.MkdirAll("s4_data/_tempdirs", os.ModePerm))
		Panic1(os.Chdir("s4_data"))
	}
	router := httprouter.New()
	router.POST("/prepare_put", PreparePut)
	router.POST("/confirm_put", ConfirmPut)
	router.POST("/prepare_get", PrepareGet)
	router.POST("/confirm_get", ConfirmGet)
	router.POST("/delete", Delete)
	router.POST("/map", Map)
	router.POST("/map_to_n", MapToN)
	router.POST("/map_from_n", MapFromN)
	router.POST("/eval", Eval)
	router.GET("/list", List)
	router.GET("/list_buckets", ListBuckets)
	router.GET("/health", Health)
	router.PanicHandler = PanicHandler
	port := fmt.Sprintf(":%s", lib.HttpPort())
	lib.Logger.Println("s4-server", port)
	Panic1(http.ListenAndServe(port, &lib.LoggingHandler{Handler: router}))
}
