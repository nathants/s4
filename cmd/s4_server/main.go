package main

import (
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

	"github.com/julienschmidt/httprouter"
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
	io_jobs      = &sync.Map{}
)

type GetJob struct {
	start           time.Time
	server_checksum chan string
	fail            chan error
	disk_checksum   string
}

func PrepareGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	port := lib.QueryParam(r, "port")
	key := lib.QueryParam(r, "key")
	assert(panic2(lib.OnThisServer(key)).(bool), "wrong server for request\n")
	remote := strings.SplitN(r.RemoteAddr, ":", 2)[0]
	if remote == "127.0.0.1" {
		remote = "0.0.0.0"
	}
	path := strings.SplitN(key, "s4://", 2)[1]
	var exists bool
	lib.With(solo_pool, func() {
		exists = panic2(lib.Exists(path)).(bool)
	})
	if !exists {
		w.WriteHeader(404)
		return
	}
	uid := uuid.NewV4().String()
	started := make(chan bool, 1)
	fail := make(chan error)
	server_checksum := make(chan string, 1)
	go lib.With(io_send_pool, func() {
		started <- true
		chk, err := lib.SendFile(path, remote, port)
		fail <- err
		server_checksum <- chk
	})
	var disk_checksum string
	lib.With(solo_pool, func() {
		disk_checksum = panic2(lib.ChecksumRead(path)).(string)
	})
	job := &GetJob{
		time.Now(),
		server_checksum,
		fail,
		disk_checksum,
	}
	_, loaded := io_jobs.LoadOrStore(uid, job)
	assert(!loaded, uid)
	select {
	case <-time.After(lib.Timeout):
		io_jobs.Delete(uid)
		w.WriteHeader(429)
	case <-started:
		panic2(w.Write([]byte(uid)))
	}
}

func ConfirmGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	uid := lib.QueryParam(r, "uuid")
	client_checksum := lib.QueryParam(r, "checksum")
	v, ok := io_jobs.LoadAndDelete(uid)
	assert(ok, uid)
	job := v.(*GetJob)
	panic1(<-job.fail)
	server_checksum := <-job.server_checksum
	disk_checksum := job.disk_checksum
	assert(client_checksum == server_checksum && server_checksum == disk_checksum, "checksum mismatch: %s %s %s\n", client_checksum, server_checksum, disk_checksum)
	w.WriteHeader(200)
}

type PutJob struct {
	start           time.Time
	server_checksum chan string
	fail            chan error
	path            string
	temp_path       string
}

func PreparePut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := lib.QueryParam(r, "key")
	assert(!strings.Contains(key, " "), "key contains spaces: %s\n", key)
	assert(panic2(lib.OnThisServer(key)).(bool), "wronger server for request")
	path := strings.SplitN(key, "s4://", 2)[1]
	assert(!strings.HasPrefix(path, "_"), path)
	fail := make(chan error)
	var exists bool
	var temp_path string
	lib.With(solo_pool, func() {
		exists = panic2(lib.Exists(path)).(bool)
		temp_path = lib.NewTempPath("_tempfiles")
	})
	if exists {
		w.WriteHeader(409)
		return
	}
	uid := uuid.NewV4().String()
	port := make(chan string, 1)
	server_checksum := make(chan string, 1)
	go lib.With(io_recv_pool, func() {
		chk, err := lib.RecvFile(temp_path, port)
		fail <- err
		server_checksum <- chk
	})
	job := &PutJob{time.Now(), server_checksum, fail, path, temp_path}
	_, loaded := io_jobs.LoadOrStore(uid, job)
	assert(!loaded, uid)
	select {
	case <-time.After(lib.Timeout):
		io_jobs.Delete(uid)
		_ = os.Remove(path)
		_ = os.Remove(panic2(lib.ChecksumPath(path)).(string))
		w.WriteHeader(429)
	case p := <-port:
		w.Header().Set("Content-Type", "application/text")
		panic2(fmt.Fprintf(w, "%s %s", uid, p))
	}
}

func ConfirmPut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	uid := lib.QueryParam(r, "uuid")
	client_checksum := lib.QueryParam(r, "checksum")
	v, ok := io_jobs.LoadAndDelete(uid)
	assert(ok, "no such job: %s", uid)
	job := v.(*PutJob)
	panic1(<-job.fail)
	server_checksum := <-job.server_checksum
	assert(client_checksum == server_checksum, "checksum mismatch: %s %s\n", client_checksum, server_checksum)
	lib.With(solo_pool, func() {
		panic1(os.MkdirAll(lib.Dir(job.path), os.ModePerm))
		assert(!panic2(lib.Exists(job.path)).(bool), job.path)
		panic1(ioutil.WriteFile(panic2(lib.ChecksumPath(job.path)).(string), []byte(server_checksum), 0o444))
		panic1(os.Chmod(job.temp_path, 0o444))
		panic1(os.Rename(job.temp_path, job.path))
	})
	w.WriteHeader(200)
}

func Delete(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	prefix := lib.QueryParam(r, "prefix")
	prefix = strings.SplitN(prefix, "s4://", 2)[1]
	assert(!strings.HasPrefix(prefix, "/"), prefix)
	recursive := lib.QueryParamDefault(r, "recursive", "false") == "true"
	lib.With(solo_pool, func() {
		if recursive {
			files, dirs := list_recursive(prefix, false)
			for _, info := range *files {
				panic1(os.Remove(info.Path))
				panic1(os.Remove(panic2(lib.ChecksumPath(info.Path)).(string)))
			}
			for _, info := range *dirs {
				panic1(os.RemoveAll(info.Path))
			}
		} else {
			panic1(os.Remove(prefix))
			panic1(os.Remove(panic2(lib.ChecksumPath(prefix)).(string)))
		}
	})
}

type MapResult struct {
	WarnResult *lib.WarnResultTempdir
	Outkey     string
}

func Map(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var data lib.Data
	bytes := panic2(ioutil.ReadAll(r.Body)).([]byte)
	panic1(json.Unmarshal(bytes, &data))
	if strings.HasPrefix(data.Cmd, "while read") {
		data.Cmd = fmt.Sprintf("cat | %s", data.Cmd)
	}
	results := make(chan MapResult, len(data.Args))
	for _, arg := range data.Args {
		assert(len(arg) == 2, fmt.Sprint(arg))
		inkey := arg[0]
		outkey := arg[1]
		assert(panic2(lib.OnThisServer(inkey)).(bool), inkey)
		assert(panic2(lib.OnThisServer(outkey)).(bool), outkey)
		inpath := panic2(filepath.Abs(strings.SplitN(inkey, "s4://", 2)[1])).(string)
		go func(inpath string) {
			lib.With(cpu_pool, func() {
				result := lib.WarnTempdir(fmt.Sprintf("export filename=%s; < %s %s > output", path.Base(inpath), inpath, data.Cmd))
				results <- MapResult{result, outkey}
			})
		}(inpath)
	}
	var tempdirs []string
	defer cleanup(&tempdirs)
	timeout := time.After(lib.MaxTimeout)
	jobs := make(chan error, len(data.Args))
	fail := make(chan error)
	go func() {
		for range data.Args {
			result := <-results
			tempdirs = append(tempdirs, result.WarnResult.Tempdir)
			if result.WarnResult.Err != nil {
				fail <- fmt.Errorf(result.WarnResult.Stdout + "\n" + result.WarnResult.Stderr)
				break
			} else {
				go func(result MapResult) {
					temp_path := lib.Join(result.WarnResult.Tempdir, "output")
					err := localPut(temp_path, result.Outkey)
					if err != nil {
						fail <- err
					}
					jobs <- nil
				}(result)
			}
		}
	}()
	for range data.Args {
		select {
		case <-jobs:
		case err := <-fail:
			w.WriteHeader(500)
			panic2(fmt.Fprintf(w, "%s", err))
			return
		case <-timeout:
			w.WriteHeader(429)
			return
		}
	}
	w.WriteHeader(200)
}

func localPut(temp_path string, key string) error {
	if strings.Contains(key, " ") {
		return fmt.Errorf("key contains space: %s", key)
	}
	on_this_server, err := lib.OnThisServer(key)
	if err != nil {
		return err
	}
	if !on_this_server {
		return fmt.Errorf("wrong server for key: %s", key)
	}
	path := strings.SplitN(key, "s4://", 2)[1]
	if strings.HasPrefix(path, "_") {
		return fmt.Errorf("path cannot start with underscore: %s", path)
	}
	var checksum string
	lib.With(misc_pool, func() {
		checksum, err = lib.Checksum(temp_path)
	})
	if err != nil {
		return err
	}
	lib.With(solo_pool, func() {
		err = confirmLocalPut(temp_path, path, checksum)
	})
	return err
}

func confirmLocalPut(temp_path string, path string, checksum string) error {
	err := os.MkdirAll(lib.Dir(path), os.ModePerm)
	if err != nil {
		return err
	}
	exists, err := lib.Exists(path)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("fatal: key already exists s4://%s", path)
	}
	err = os.Chmod(temp_path, 0o444)
	if err != nil {
		return err
	}
	err = os.Rename(temp_path, path)
	if err != nil {
		return err
	}
	return lib.ChecksumWrite(path, checksum)
}

type MapToNResult struct {
	WarnResult *lib.WarnResultTempdir
	Inpath     string
	Outdir     string
}

func cleanup(tempdirs *[]string) {
	for _, tempdir := range *tempdirs {
		panic1(os.RemoveAll(tempdir))
	}
}

func MapToN(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var data lib.Data
	bytes := panic2(ioutil.ReadAll(r.Body)).([]byte)
	panic1(json.Unmarshal(bytes, &data))
	if strings.HasPrefix(data.Cmd, "while read") {
		data.Cmd = fmt.Sprintf("cat | %s", data.Cmd)
	}
	results := make(chan MapToNResult, len(data.Args))
	for _, arg := range data.Args {
		assert(len(arg) == 2, fmt.Sprint(arg))
		inkey := arg[0]
		outdir := arg[1]
		assert(panic2(lib.OnThisServer(inkey)).(bool), inkey)
		assert(strings.HasPrefix(outdir, "s4://") && strings.HasSuffix(outdir, "/"), outdir)
		inpath := panic2(filepath.Abs(strings.SplitN(inkey, "s4://", 2)[1])).(string)
		go func(inpath string) {
			lib.With(cpu_pool, func() {
				result := lib.WarnTempdir(fmt.Sprintf("export filename=%s; < %s %s", path.Base(inpath), inpath, data.Cmd))
				results <- MapToNResult{result, inpath, outdir}
			})
		}(inpath)
	}
	var tempdirs []string
	defer cleanup(&tempdirs)
	timeout := time.After(lib.MaxTimeout)
	fail := make(chan error)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for range data.Args {
			result := <-results
			tempdirs = append(tempdirs, result.WarnResult.Tempdir)
			if result.WarnResult.Err != nil {
				fail <- fmt.Errorf(result.WarnResult.Stdout + "\n" + result.WarnResult.Stderr)
				break
			} else {
				for _, temp_path := range strings.Split(result.WarnResult.Stdout, "\n") {
					if temp_path != "" {
						wg.Add(1)
						temp_path = lib.Join(result.WarnResult.Tempdir, temp_path)
						outkey := lib.Join(result.Outdir, path.Base(result.Inpath), path.Base(temp_path))
						go mapToNPut(&wg, fail, temp_path, outkey)
					}
				}
			}
		}
		wg.Done()
	}()
	select {
	case err := <-fail:
		w.WriteHeader(500)
		panic2(fmt.Fprintf(w, "%s", err))
	case <-lib.Await(&wg):
		w.WriteHeader(200)
	case <-timeout:
		w.WriteHeader(429)
	}
}

func mapToNPut(wg *sync.WaitGroup, fail chan<- error, temp_path string, outkey string) {
	defer wg.Done()
	on_this_server, err := lib.OnThisServer(outkey)
	if err != nil {
		fail <- err
		return
	}
	if on_this_server {
		err := localPut(temp_path, outkey)
		if err != nil {
			fail <- err
		}
	} else {
		err := lib.Retry(func() error {
			var err error
			lib.With(io_send_pool, func() {
				err = lib.Put(temp_path, outkey)
			})
			if errors.Is(err, lib.Err409) {
				fail <- err
				return nil
			}
			return err
		})
		if err != nil {
			fail <- err
		}
	}
}

func MapFromN(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	outdir := lib.QueryParam(r, "outdir")
	assert(strings.HasPrefix(outdir, "s4://") && strings.HasSuffix(outdir, "/"), outdir)
	var data lib.Data
	bytes := panic2(ioutil.ReadAll(r.Body)).([]byte)
	panic1(json.Unmarshal(bytes, &data))
	if strings.HasPrefix(data.Cmd, "while read") {
		data.Cmd = fmt.Sprintf("cat | %s", data.Cmd)
	}
	results := make(chan MapResult, len(data.Args))
	for _, inkeys := range data.Args {
		var inpaths []string
		for _, inkey := range inkeys {
			assert(panic2(lib.OnThisServer(inkey)).(bool), inkey)
			inpath := strings.SplitN(inkey, "s4://", 2)[1]
			inpath = panic2(filepath.Abs(inpath)).(string)
			inpaths = append(inpaths, inpath)
		}
		outkey := lib.Join(outdir, lib.KeyPrefix(inkeys[0])+lib.Suffix(inkeys))
		go func(inpaths []string) {
			lib.With(cpu_pool, func() {
				stdin := strings.NewReader(strings.Join(inpaths, "\n") + "\n")
				result := lib.WarnTempdirStreamIn(stdin, fmt.Sprintf("%s > output", data.Cmd))
				results <- MapResult{result, outkey}
			})
		}(inpaths)
	}
	var tempdirs []string
	defer cleanup(&tempdirs)
	timeout := time.After(lib.MaxTimeout)
	jobs := make(chan error, len(data.Args))
	fail := make(chan error)
	go func() {
		for range data.Args {
			result := <-results
			tempdirs = append(tempdirs, result.WarnResult.Tempdir)
			if result.WarnResult.Err != nil {
				fail <- fmt.Errorf(result.WarnResult.Stdout + "\n" + result.WarnResult.Stderr)
				break
			} else {
				go func(result MapResult) {
					temp_path := lib.Join(result.WarnResult.Tempdir, "output")
					err := localPut(temp_path, result.Outkey)
					if err != nil {
						fail <- err
					}
					jobs <- nil
				}(result)
			}
		}
	}()
	for range data.Args {
		select {
		case <-jobs:
		case err := <-fail:
			w.WriteHeader(500)
			panic2(fmt.Fprintf(w, "%s", err))
			return
		case <-timeout:
			w.WriteHeader(429)
			return
		}
	}
	w.WriteHeader(200)
}

func Eval(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := lib.QueryParam(r, "key")
	cmd := panic2(ioutil.ReadAll(r.Body)).([]byte)
	path := strings.SplitN(key, "s4://", 2)[1]
	var exists bool
	lib.With(solo_pool, func() {
		exists = panic2(lib.Exists(path)).(bool)
	})
	if !exists {
		w.WriteHeader(404)
	} else {
		lib.With(cpu_pool, func() {
			res := lib.Warn("< %s %s", path, cmd)
			if res.Err != nil {
				w.WriteHeader(500)
				panic2(fmt.Fprintf(w, res.Stdout+"\n"+res.Stderr))
			} else {
				w.WriteHeader(200)
				panic2(fmt.Fprintf(w, res.Stdout))
			}
		})
	}
}

type File struct {
	ModTime time.Time
	Size    string
	Path    string
}

func list_recursive(prefix string, strip_bucket bool) (*[]*File, *[]*File) {
	root := prefix
	if !strings.HasSuffix(prefix, "/") && strings.Count(prefix, "/") > 0 {
		root = lib.Dir(prefix)
	}
	var files []*File
	var dirs []*File
	_, err := os.Stat(root)
	if err == nil {
		panic1(filepath.Walk(root, func(fullpath string, info os.FileInfo, err error) error {
			panic1(err)
			matched := strings.HasPrefix(fullpath, prefix)
			is_checksum := lib.IsChecksum(fullpath)
			if matched && !is_checksum {
				path := fullpath
				if strip_bucket {
					path = strings.Join(strings.Split(fullpath, "/")[1:], "/")
				}
				if info.IsDir() {
					dirs = append(dirs, &File{info.ModTime(), "", path})
				} else {
					files = append(files, &File{info.ModTime(), fmt.Sprint(info.Size()), path})
				}
			}
			return nil
		}))
	}
	return &files, &dirs
}

func list(prefix string) *[]*File {
	root := prefix
	if !strings.HasSuffix(prefix, "/") && strings.Count(prefix, "/") > 0 {
		root = lib.Dir(prefix)
	}
	var res []*File
	_, err := os.Stat(root)
	if err == nil {
		for _, info := range panic2(ioutil.ReadDir(root)).([]os.FileInfo) {
			name := info.Name()
			matched := strings.HasPrefix(lib.Join(root, name), prefix)
			is_checksum := lib.IsChecksum(name)
			if matched && !is_checksum {
				if info.IsDir() {
					res = append(res, &File{info.ModTime(), "PRE", name + "/"})
				} else {
					res = append(res, &File{info.ModTime(), fmt.Sprint(info.Size()), info.Name()})
				}
			}
		}
	}
	return &res
}

func List(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	prefix := lib.QueryParam(r, "prefix")
	assert(strings.HasPrefix(prefix, "s4://"), prefix)
	prefix = strings.Split(prefix, "s4://")[1]
	recursive := lib.QueryParamDefault(r, "recursive", "false") == "true"
	var res *[]*File
	lib.With(misc_pool, func() {
		if recursive {
			res, _ = list_recursive(prefix, true)
		} else {
			res = list(prefix)
		}
	})
	var vals [][]string
	for _, file := range *res {
		parts := []string{"", ""}
		if file.Size != "PRE" {
			parts = strings.SplitN(file.ModTime.Format(time.RFC3339), "T", 2)
		}
		vals = append(vals, []string{parts[0], parts[1], file.Size, file.Path})
	}
	w.Header().Set("Content-Type", "application/json")
	bytes := panic2(json.Marshal(vals))
	panic2(w.Write(bytes.([]byte)))
}

func ListBuckets(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var res [][]string
	for _, info := range panic2(ioutil.ReadDir(".")).([]os.FileInfo) {
		name := info.Name()
		if info.IsDir() && !strings.HasPrefix(name, "_") {
			parts := strings.SplitN(info.ModTime().Format(time.RFC3339), "T", 2)
			res = append(res, []string{parts[0], parts[1], fmt.Sprint(info.Size()), name})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	bytes := panic2(json.Marshal(res))
	panic2(w.Write(bytes.([]byte)))
}

func Health(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	panic2(fmt.Fprintf(w, "healthy\n"))
}

func PanicHandler(w http.ResponseWriter, r *http.Request, err interface{}) {
	lib.Logger.Println("panic handled: %s", err)
	w.WriteHeader(500)
	panic2(fmt.Fprintf(w, "%s\n", err))
}

func expireJobs() {
	io_jobs.Range(func(k, v interface{}) bool {
		var start time.Time
		var path string
		var temp_path string
		switch v := v.(type) {
		case *PutJob:
			start = v.start
			path = v.path
			temp_path = v.temp_path
		case *GetJob:
			start = v.start
		}
		if time.Since(start) > lib.MaxTimeout {
			lib.Logger.Printf("gc expired job: %s %s %s\n", k, path, temp_path)
			go func() {
				lib.With(misc_pool, func() {
					if path != "" {
						_ = os.Remove(path)
					}
					if temp_path != "" {
						_ = os.Remove(temp_path)
					}
				})
			}()
			io_jobs.Delete(k)
		}
		return true
	})
}

func expireFiles() {
	root := "_tempfiles"
	for _, info := range panic2(ioutil.ReadDir(root)).([]os.FileInfo) {
		if time.Since(info.ModTime()) > lib.MaxTimeout {
			path := lib.Join(root, info.Name())
			lib.Logger.Printf("gc expired tempfile: %s\n", path)
			_ = os.Remove(path)
		}
	}
}

func expireDirs() {
	root := "_tempdirs"
	for _, info := range panic2(ioutil.ReadDir(root)).([]os.FileInfo) {
		if time.Since(info.ModTime()) > lib.MaxTimeout {
			path := lib.Join(root, info.Name())
			lib.Logger.Printf("gc expired tempdir: %s\n", path)
			_ = os.RemoveAll(path)
		}
	}
}

func expiredDataDeleter() {
	for {
		expireJobs()
		expireFiles()
		expireDirs()
		time.Sleep(time.Second * 5)
	}
}

func main() {
	lib.Port = flag.Int("port", 0, "specify port instead of matching a single conf entry by ipv4")
	lib.Conf = flag.String("conf", "", "specify conf path to use instead of ~/.s4.conf")
	flag.Parse()
	panic1(os.Setenv("LC_ALL", "C"))
	_ = lib.Servers()
	panic1(os.MkdirAll("s4_data/_tempfiles", os.ModePerm))
	panic1(os.MkdirAll("s4_data/_tempdirs", os.ModePerm))
	panic1(os.Chdir("s4_data"))
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
	go expiredDataDeleter()
	panic1(http.ListenAndServe(port, &lib.LoggingHandler{Handler: router}))
}

func assert(cond bool, format string, a ...interface{}) {
	if !cond {
		panic(fmt.Sprintf(format, a...))
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
