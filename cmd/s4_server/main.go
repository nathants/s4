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
	"strings"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"golang.org/x/sync/semaphore"
	"github.com/nathants/s4"

)

var (
	io_jobs      = &sync.Map{}
	io_send_pool *semaphore.Weighted
	io_recv_pool *semaphore.Weighted
	cpu_pool     *semaphore.Weighted
	misc_pool    *semaphore.Weighted
	solo_pool    *semaphore.Weighted
)

type GetJob struct {
	start           time.Time
	server_checksum chan string
	fail            chan error
	disk_checksum   string
}

func PrepareGet(w http.ResponseWriter, r *http.Request) {
	port := s4.QueryParam(r, "port")
	key := s4.QueryParam(r, "key")
	assert(panic2(s4.OnThisServer(key)).(bool), "wrong server for request\n")
	remote := strings.SplitN(r.RemoteAddr, ":", 2)[0]
	if remote == "127.0.0.1" {
		remote = "0.0.0.0"
	}
	path := strings.SplitN(key, "s4://", 2)[1]
	var exists bool
	s4.With(solo_pool, func() {
		exists = panic2(s4.Exists(path)).(bool)
	})
	if !exists {
		w.WriteHeader(404)
		return
	}
	uid := uuid.NewV4().String()
	started := make(chan bool, 1)
	fail := make(chan error, 1)
	server_checksum := make(chan string, 1)
	go s4.With(io_send_pool, func() {
		started <- true
		chk, err := s4.SendFile(path, remote, port)
		if err != nil {
			s4.Logger.Println("send error:", err)
		}
		fail <- err
		server_checksum <- chk
	})
	var disk_checksum string
	s4.With(solo_pool, func() {
		disk_checksum = panic2(s4.ChecksumRead(path)).(string)
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
	case <-time.After(s4.Timeout):
		io_jobs.Delete(uid)
		w.WriteHeader(429)
	case <-started:
		panic2(w.Write([]byte(uid)))
	}
}

func ConfirmGet(w http.ResponseWriter, r *http.Request) {
	uid := s4.QueryParam(r, "uuid")
	client_checksum := s4.QueryParam(r, "checksum")
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

func PreparePut(w http.ResponseWriter, r *http.Request) {
	key := s4.QueryParam(r, "key")
	assert(!strings.Contains(key, " "), "key contains spaces: %s\n", key)
	assert(panic2(s4.OnThisServer(key)).(bool), "wronger server for request")
	path := strings.SplitN(key, "s4://", 2)[1]
	assert(!strings.HasPrefix(path, "_"), path)
	var exists bool
	var temp_path string
	s4.With(solo_pool, func() {
		exists = panic2(s4.Exists(path)).(bool)
		temp_path = s4.NewTempPath("_tempfiles")
	})
	if exists {
		w.WriteHeader(409)
		return
	}
	uid := uuid.NewV4().String()
	port := make(chan string, 1)
	fail := make(chan error, 1)
	server_checksum := make(chan string, 1)
	go s4.With(io_recv_pool, func() {
		chk, err := s4.RecvFile(temp_path, port)
		if err != nil {
			s4.Logger.Println("recv error:", err)
		}
		fail <- err
		server_checksum <- chk
	})
	job := &PutJob{time.Now(), server_checksum, fail, path, temp_path}
	_, loaded := io_jobs.LoadOrStore(uid, job)
	assert(!loaded, uid)
	select {
	case <-time.After(s4.Timeout):
		io_jobs.Delete(uid)
		_ = os.Remove(path)
		_ = os.Remove(panic2(s4.ChecksumPath(path)).(string))
		w.WriteHeader(429)
	case p := <-port:
		w.Header().Set("Content-Type", "application/text")
		panic2(fmt.Fprintf(w, "%s %s", uid, p))
	}
}

func ConfirmPut(w http.ResponseWriter, r *http.Request) {
	uid := s4.QueryParam(r, "uuid")
	client_checksum := s4.QueryParam(r, "checksum")
	v, ok := io_jobs.LoadAndDelete(uid)
	assert(ok, "no such job: %s", uid)
	job := v.(*PutJob)
	panic1(<-job.fail)
	server_checksum := <-job.server_checksum
	assert(client_checksum == server_checksum, "checksum mismatch: %s %s\n", client_checksum, server_checksum)
	s4.With(solo_pool, func() {
		panic1(os.MkdirAll(s4.Dir(job.path), os.ModePerm))
		assert(!panic2(s4.Exists(job.path)).(bool), job.path)
		panic1(ioutil.WriteFile(panic2(s4.ChecksumPath(job.path)).(string), []byte(server_checksum), 0o444))
		panic1(os.Chmod(job.temp_path, 0o444))
		panic1(os.Rename(job.temp_path, job.path))
	})
	w.WriteHeader(200)
}

func Delete(w http.ResponseWriter, r *http.Request) {
	prefix := s4.QueryParam(r, "prefix")
	prefix = strings.SplitN(prefix, "s4://", 2)[1]
	assert(!strings.HasPrefix(prefix, "/"), prefix)
	recursive := s4.QueryParamDefault(r, "recursive", "false") == "true"
	s4.With(solo_pool, func() {
		if recursive {
			files, dirs := list_recursive(prefix, false)
			for _, info := range *files {
				panic1(os.Remove(info.Path))
				panic1(os.Remove(panic2(s4.ChecksumPath(info.Path)).(string)))
			}
			for _, info := range *dirs {
				panic1(os.RemoveAll(info.Path))
			}
		} else {
			panic1(os.Remove(prefix))
			panic1(os.Remove(panic2(s4.ChecksumPath(prefix)).(string)))
		}
	})
}

type MapResult struct {
	WarnResult *s4.WarnResultTempdir
	Outkey     string
}

func Map(w http.ResponseWriter, r *http.Request) {
	var data s4.MapArgs
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
		assert(panic2(s4.OnThisServer(inkey)).(bool), inkey)
		assert(panic2(s4.OnThisServer(outkey)).(bool), outkey)
		inpath := panic2(filepath.Abs(strings.SplitN(inkey, "s4://", 2)[1])).(string)
		go func(inpath string) {
			s4.With(cpu_pool, func() {
				result := s4.WarnTempdir(fmt.Sprintf("export filename=%s; < %s %s > output", path.Base(inpath), inpath, data.Cmd))
				results <- MapResult{result, outkey}
			})
		}(inpath)
	}
	var tempdirs []string
	defer cleanup(&tempdirs)
	timeout := time.After(s4.MaxTimeout)
	jobs := make(chan error, len(data.Args))
	fail := make(chan error, 1)
	go func() {
		for range data.Args {
			result := <-results
			tempdirs = append(tempdirs, result.WarnResult.Tempdir)
			if result.WarnResult.Err != nil {
				fail <- fmt.Errorf(result.WarnResult.Stdout + "\n" + result.WarnResult.Stderr)
				break
			} else {
				go func(result MapResult) {
					temp_path := s4.Join(result.WarnResult.Tempdir, "output")
					err := localPut(temp_path, result.Outkey)
					if err != nil {
						fail <- err
					} else {
						jobs <- nil
					}
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
	on_this_server, err := s4.OnThisServer(key)
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
	s4.With(misc_pool, func() {
		checksum, err = s4.Checksum(temp_path)
	})
	if err != nil {
		return err
	}
	s4.With(solo_pool, func() {
		err = confirmLocalPut(temp_path, path, checksum)
	})
	return err
}

func confirmLocalPut(temp_path string, path string, checksum string) error {
	err := os.MkdirAll(s4.Dir(path), os.ModePerm)
	if err != nil {
		return err
	}
	exists, err := s4.Exists(path)
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
	return s4.ChecksumWrite(path, checksum)
}

type MapToNResult struct {
	WarnResult *s4.WarnResultTempdir
	Inpath     string
	Outdir     string
}

func cleanup(tempdirs *[]string) {
	for _, tempdir := range *tempdirs {
		panic1(os.RemoveAll(tempdir))
	}
}

func MapToN(w http.ResponseWriter, r *http.Request) {
	var data s4.MapArgs
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
		assert(panic2(s4.OnThisServer(inkey)).(bool), inkey)
		assert(strings.HasPrefix(outdir, "s4://") && strings.HasSuffix(outdir, "/"), outdir)
		inpath := panic2(filepath.Abs(strings.SplitN(inkey, "s4://", 2)[1])).(string)
		go func(inpath string) {
			s4.With(cpu_pool, func() {
				result := s4.WarnTempdir(fmt.Sprintf("export filename=%s; < %s %s", path.Base(inpath), inpath, data.Cmd))
				results <- MapToNResult{result, inpath, outdir}
			})
		}(inpath)
	}
	var tempdirs []string
	defer cleanup(&tempdirs)
	timeout := time.After(s4.MaxTimeout)
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
						temp_path = s4.Join(result.WarnResult.Tempdir, temp_path)
						outkey := s4.Join(result.Outdir, path.Base(result.Inpath), path.Base(temp_path))
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
	case <-s4.Await(&wg):
		w.WriteHeader(200)
	case <-timeout:
		w.WriteHeader(429)
	}
}

func mapToNPut(wg *sync.WaitGroup, fail chan<- error, temp_path string, outkey string) {
	defer wg.Done()
	on_this_server, err := s4.OnThisServer(outkey)
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
		err := s4.Retry(func() error {
			var err error
			s4.With(io_send_pool, func() {
				err = s4.Put(temp_path, outkey)
			})
			if errors.Is(err, s4.Err409) {
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

func MapFromN(w http.ResponseWriter, r *http.Request) {
	outdir := s4.QueryParam(r, "outdir")
	assert(strings.HasPrefix(outdir, "s4://") && strings.HasSuffix(outdir, "/"), outdir)
	var data s4.MapArgs
	bytes := panic2(ioutil.ReadAll(r.Body)).([]byte)
	panic1(json.Unmarshal(bytes, &data))
	if strings.HasPrefix(data.Cmd, "while read") {
		data.Cmd = fmt.Sprintf("cat | %s", data.Cmd)
	}
	results := make(chan MapResult, len(data.Args))
	for _, inkeys := range data.Args {
		var inpaths []string
		for _, inkey := range inkeys {
			assert(panic2(s4.OnThisServer(inkey)).(bool), inkey)
			inpath := strings.SplitN(inkey, "s4://", 2)[1]
			inpath = panic2(filepath.Abs(inpath)).(string)
			inpaths = append(inpaths, inpath)
		}
		outkey := s4.Join(outdir, s4.KeyPrefix(inkeys[0])+s4.Suffix(inkeys))
		go func(inpaths []string) {
			s4.With(cpu_pool, func() {
				stdin := strings.NewReader(strings.Join(inpaths, "\n") + "\n")
				result := s4.WarnTempdirStreamIn(stdin, fmt.Sprintf("%s > output", data.Cmd))
				results <- MapResult{result, outkey}
			})
		}(inpaths)
	}
	var tempdirs []string
	defer cleanup(&tempdirs)
	timeout := time.After(s4.MaxTimeout)
	jobs := make(chan error, len(data.Args))
	fail := make(chan error, 1)
	go func() {
		for range data.Args {
			result := <-results
			tempdirs = append(tempdirs, result.WarnResult.Tempdir)
			if result.WarnResult.Err != nil {
				fail <- fmt.Errorf(result.WarnResult.Stdout + "\n" + result.WarnResult.Stderr)
				break
			} else {
				go func(result MapResult) {
					temp_path := s4.Join(result.WarnResult.Tempdir, "output")
					err := localPut(temp_path, result.Outkey)
					if err != nil {
						fail <- err
					} else {
						jobs <- nil
					}
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

func Eval(w http.ResponseWriter, r *http.Request) {
	key := s4.QueryParam(r, "key")
	cmd := panic2(ioutil.ReadAll(r.Body)).([]byte)
	path := strings.SplitN(key, "s4://", 2)[1]
	var exists bool
	s4.With(solo_pool, func() {
		exists = panic2(s4.Exists(path)).(bool)
	})
	if !exists {
		w.WriteHeader(404)
	} else {
		s4.With(cpu_pool, func() {
			res := s4.Warn("< %s %s", path, cmd)
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
		root = s4.Dir(prefix)
	}
	var files []*File
	var dirs []*File
	_, err := os.Stat(root)
	if err == nil {
		panic1(filepath.Walk(root, func(fullpath string, info os.FileInfo, err error) error {
			panic1(err)
			matched := strings.HasPrefix(fullpath, prefix)
			is_checksum := s4.IsChecksum(fullpath)
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
		root = s4.Dir(prefix)
	}
	var res []*File
	_, err := os.Stat(root)
	if err == nil {
		for _, info := range panic2(ioutil.ReadDir(root)).([]os.FileInfo) {
			name := info.Name()
			matched := strings.HasPrefix(s4.Join(root, name), prefix)
			is_checksum := s4.IsChecksum(name)
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

func List(w http.ResponseWriter, r *http.Request) {
	prefix := s4.QueryParam(r, "prefix")
	assert(strings.HasPrefix(prefix, "s4://"), prefix)
	prefix = strings.Split(prefix, "s4://")[1]
	recursive := s4.QueryParamDefault(r, "recursive", "false") == "true"
	var res *[]*File
	s4.With(misc_pool, func() {
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

func ListBuckets(w http.ResponseWriter, r *http.Request) {
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

func Health(w http.ResponseWriter, r *http.Request) {
	panic2(fmt.Fprintf(w, "healthy\n"))
}

func NotFound(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(404)
	panic2(fmt.Fprintf(w, "404\n"))
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
		if time.Since(start) > s4.MaxTimeout {
			s4.Logger.Printf("gc expired job: %s %s %s\n", k, path, temp_path)
			go func() {
				s4.With(misc_pool, func() {
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
		if time.Since(info.ModTime()) > s4.MaxTimeout {
			path := s4.Join(root, info.Name())
			s4.Logger.Printf("gc expired tempfile: %s\n", path)
			_ = os.Remove(path)
		}
	}
}

func expireDirs() {
	root := "_tempdirs"
	for _, info := range panic2(ioutil.ReadDir(root)).([]os.FileInfo) {
		if time.Since(info.ModTime()) > s4.MaxTimeout {
			path := s4.Join(root, info.Name())
			s4.Logger.Printf("gc expired tempdir: %s\n", path)
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

func initPools(max_io_jobs int, max_cpu_jobs int) {
	io_send_pool = semaphore.NewWeighted(int64(max_io_jobs))
	io_recv_pool = semaphore.NewWeighted(int64(max_io_jobs))
	cpu_pool = semaphore.NewWeighted(int64(max_cpu_jobs))
	misc_pool = semaphore.NewWeighted(int64(max_cpu_jobs))
	solo_pool = semaphore.NewWeighted(int64(1))
}

func router(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		switch r.URL.Path {
		case "/list":
			List(w, r)
		case "/list_buckets":
			ListBuckets(w, r)
		case "/health":
			Health(w, r)
		default:
			NotFound(w, r)
		}
	case "POST":
		switch r.URL.Path {
		case "/prepare_put":
			PreparePut(w, r)
		case "/confirm_put":
			ConfirmPut(w, r)
		case "/prepare_get":
			PrepareGet(w, r)
		case "/confirm_get":
			ConfirmGet(w, r)
		case "/delete":
			Delete(w, r)
		case "/map":
			Map(w, r)
		case "/map_to_n":
			MapToN(w, r)
		case "/map_from_n":
			MapFromN(w, r)
		case "/eval":
			Eval(w, r)
		default:
			NotFound(w, r)
		}
	default:
		NotFound(w, r)
	}
}

func main() {
	num_cpus := runtime.GOMAXPROCS(0)
	s4.Port = flag.Int("port", 0, "specify port instead of matching a single conf entry by ipv4")
	max_io_jobs := flag.Int("max-io-jobs", num_cpus*4, "specify max-io-jobs to use instead of cpus*4")
	max_cpu_jobs := flag.Int("max-cpu-jobs", num_cpus+2, "specify max-cpu-jobs to use instead of cpus+2")
	s4.Conf = flag.String("conf", "", "specify conf path to use instead of ~/.s4.conf")
	flag.Parse()
	initPools(*max_io_jobs, *max_cpu_jobs)
	panic1(os.Setenv("LC_ALL", "C"))
	_ = s4.Servers()
	panic1(os.MkdirAll("s4_data/_tempfiles", os.ModePerm))
	panic1(os.MkdirAll("s4_data/_tempdirs", os.ModePerm))
	panic1(os.Chdir("s4_data"))
	port := fmt.Sprintf(":%s", s4.HttpPort())
	s4.Logger.Println("s4-server", port)
	go expiredDataDeleter()
	server := &http.Server{
		ReadTimeout:  s4.MaxTimeout,
		WriteTimeout: s4.MaxTimeout,
		IdleTimeout:  s4.MaxTimeout,
		Addr:         port,
		Handler:      &s4.RootHandler{Handler: router},
	}
	panic1(server.ListenAndServe())
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
