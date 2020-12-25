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

	"github.com/nathants/s4"
	"github.com/nathants/s4/lib"
	"github.com/satori/go.uuid"
	"golang.org/x/sync/semaphore"
)

var (
	ioJobs     = &sync.Map{}
	ioSendPool *semaphore.Weighted
	ioRecvPool *semaphore.Weighted
	cpuPool    *semaphore.Weighted
	miscPool   *semaphore.Weighted
	soloPool   *semaphore.Weighted
)

type GetJob struct {
	start          time.Time
	serverChecksum chan string
	fail           chan error
	diskChecksum   string
}

func prepareGetHandler(w http.ResponseWriter, r *http.Request, this lib.Server, servers []lib.Server) {
	port := lib.QueryParam(r, "port")
	key := lib.QueryParam(r, "key")
	assert(panic2(lib.OnThisServer(key, this, servers)).(bool), "wrong server for request\n")
	remote := strings.SplitN(r.RemoteAddr, ":", 2)[0]
	if remote == "127.0.0.1" {
		remote = "0.0.0.0"
	}
	path := strings.SplitN(key, "s4://", 2)[1]
	var exists bool
	lib.With(soloPool, func() {
		exists = panic2(lib.Exists(path)).(bool)
	})
	if !exists {
		w.WriteHeader(404)
		return
	}
	uid := uuid.NewV4().String()
	started := make(chan bool, 1)
	fail := make(chan error, 1)
	serverChecksum := make(chan string, 1)
	go lib.With(ioSendPool, func() {
		started <- true
		chk, err := lib.SendFile(path, remote, port)
		if err != nil {
			lib.Logger.Println("send error:", err)
		}
		fail <- err
		serverChecksum <- chk
	})
	var diskChecksum string
	lib.With(soloPool, func() {
		diskChecksum = panic2(lib.ChecksumRead(path)).(string)
	})
	job := &GetJob{
		time.Now(),
		serverChecksum,
		fail,
		diskChecksum,
	}
	_, loaded := ioJobs.LoadOrStore(uid, job)
	assert(!loaded, uid)
	select {
	case <-time.After(lib.Timeout):
		ioJobs.Delete(uid)
		w.WriteHeader(429)
	case <-started:
		panic2(w.Write([]byte(uid)))
	}
}

func confirmGetHandler(w http.ResponseWriter, r *http.Request) {
	uid := lib.QueryParam(r, "uuid")
	clientChecksum := lib.QueryParam(r, "checksum")
	v, ok := ioJobs.LoadAndDelete(uid)
	assert(ok, uid)
	job := v.(*GetJob)
	panic1(<-job.fail)
	serverChecksum := <-job.serverChecksum
	diskChecksum := job.diskChecksum
	assert(clientChecksum == serverChecksum && serverChecksum == diskChecksum, "checksum mismatch: %s %s %s\n", clientChecksum, serverChecksum, diskChecksum)
	w.WriteHeader(200)
}

type PutJob struct {
	start          time.Time
	serverChecksum chan string
	fail           chan error
	path           string
	tempPath       string
}

func preparePutHandler(w http.ResponseWriter, r *http.Request, this lib.Server, servers []lib.Server) {
	key := lib.QueryParam(r, "key")
	assert(!strings.Contains(key, " "), "key contains spaces: %s\n", key)
	assert(panic2(lib.OnThisServer(key, this, servers)).(bool), "wrong server for request")
	path := strings.SplitN(key, "s4://", 2)[1]
	assert(!strings.HasPrefix(path, "_"), path)
	var exists bool
	var tempPath string
	lib.With(soloPool, func() {
		exists = panic2(lib.Exists(path)).(bool)
		tempPath = lib.NewTempPath("_tempfiles")
	})
	if exists {
		w.WriteHeader(409)
		return
	}
	uid := uuid.NewV4().String()
	port := make(chan string, 1)
	fail := make(chan error, 1)
	serverChecksum := make(chan string, 1)
	go lib.With(ioRecvPool, func() {
		chk, err := lib.RecvFile(tempPath, port)
		if err != nil {
			lib.Logger.Println("recv error:", err)
		}
		fail <- err
		serverChecksum <- chk
	})
	job := &PutJob{time.Now(), serverChecksum, fail, path, tempPath}
	_, loaded := ioJobs.LoadOrStore(uid, job)
	assert(!loaded, uid)
	select {
	case <-time.After(lib.Timeout):
		ioJobs.Delete(uid)
		_ = os.Remove(path)
		_ = os.Remove(panic2(lib.ChecksumPath(path)).(string))
		w.WriteHeader(429)
	case p := <-port:
		w.Header().Set("Content-Type", "application/text")
		panic2(fmt.Fprintf(w, "%s %s", uid, p))
	}
}

func confirmPutHandler(w http.ResponseWriter, r *http.Request) {
	uid := lib.QueryParam(r, "uuid")
	clientChecksum := lib.QueryParam(r, "checksum")
	v, ok := ioJobs.LoadAndDelete(uid)
	assert(ok, "no such job: %s", uid)
	job := v.(*PutJob)
	panic1(<-job.fail)
	serverChecksum := <-job.serverChecksum
	assert(clientChecksum == serverChecksum, "checksum mismatch: %s %s\n", clientChecksum, serverChecksum)
	exists := false
	lib.With(soloPool, func() {
		panic1(os.MkdirAll(lib.Dir(job.path), os.ModePerm))
		exists = panic2(lib.Exists(job.path)).(bool)
		if !exists {
			panic1(ioutil.WriteFile(panic2(lib.ChecksumPath(job.path)).(string), []byte(serverChecksum), 0o444))
			panic1(os.Chmod(job.tempPath, 0o444))
			panic1(os.Rename(job.tempPath, job.path))
		}
	})
	if exists {
		w.WriteHeader(409)
	} else {
		w.WriteHeader(200)
	}
}

func deleteHandler(r *http.Request, this lib.Server, servers []lib.Server) {
	prefix := lib.QueryParam(r, "prefix")
	recursive := lib.QueryParamDefault(r, "recursive", "false") == "true"
	if !recursive {
		assert(panic2(lib.OnThisServer(prefix, this, servers)).(bool), "wrong server for request")
	}
	prefix = strings.SplitN(prefix, "s4://", 2)[1]
	assert(!strings.HasPrefix(prefix, "/"), prefix)
	cwd := path.Base(panic2(os.Getwd()).(string))
	assert(cwd == "s4_data", cwd)
	lib.With(soloPool, func() {
		if recursive {
			files, dirs := listRecursive(prefix, false)
			for _, info := range *files {
				assert(!strings.HasPrefix(info.Path, "/"), info.Path)
				panic1(os.Remove(info.Path))
				panic1(os.Remove(panic2(lib.ChecksumPath(info.Path)).(string)))
			}
			for _, info := range *dirs {
				assert(!strings.HasPrefix(info.Path, "/"), info.Path)
				panic1(os.RemoveAll(info.Path))
			}
		} else {
			assert(!strings.HasPrefix(prefix, "/"), prefix)
			panic1(os.Remove(prefix))
			panic1(os.Remove(panic2(lib.ChecksumPath(prefix)).(string)))
		}
	})
}

type MapResult struct {
	WarnResult *lib.WarnResultTempdir
	Outkey     string
}

func mapHandler(w http.ResponseWriter, r *http.Request, this lib.Server, servers []lib.Server) {
	var data lib.MapArgs
	bytes := panic2(ioutil.ReadAll(r.Body)).([]byte)
	panic1(json.Unmarshal(bytes, &data))
	indir, glob := lib.ParseGlob(data.Indir)
	outdir := data.Outdir
	assert(strings.HasSuffix(indir, "/"), fmt.Sprintf("indir not a directory: %s", indir))
	assert(strings.HasSuffix(outdir, "/"), fmt.Sprintf("outdir not a directory: %s", outdir))
	pth := strings.SplitN(indir, "://", 2)[1]
	files, _ := listRecursive(pth, true)
	pth = strings.SplitN(pth, "/", 2)[1]
	if strings.HasPrefix(data.Cmd, "while read") {
		data.Cmd = fmt.Sprintf("cat | %s", data.Cmd)
	}
	results := make(chan MapResult, len(*files))
	count := 0
	for _, file := range *files {
		size := file.Size
		key := file.Path
		if pth != "" {
			key = strings.SplitN(key, pth, 2)[1]
		}
		if size == "PRE" {
			continue
		}
		if glob != "" {
			match := panic2(path.Match(glob, key)).(bool)
			fmt.Println(match, glob, key)
			if !match {
				continue
			}
		}
		inkey := lib.Join(indir, key)
		outkey := lib.Join(outdir, key)
		inpath := panic2(filepath.Abs(strings.SplitN(inkey, "s4://", 2)[1])).(string)
		go func(inpath string) {
			lib.With(cpuPool, func() {
				result := lib.WarnTempdir(fmt.Sprintf("export filename=%s; < %s %s > output", path.Base(inpath), inpath, data.Cmd))
				results <- MapResult{result, outkey}
			})
		}(inpath)
		count++
	}
	var tempdirs []string
	defer cleanup(&tempdirs)
	timeout := time.After(lib.MaxTimeout)
	jobs := make(chan error, len(*files))
	fail := make(chan error, 1)
	go func() {
		for i := 0; i < count; i++ {
			result := <-results
			tempdirs = append(tempdirs, result.WarnResult.Tempdir)
			if result.WarnResult.Err != nil {
				fail <- fmt.Errorf(result.WarnResult.Stdout + "\n" + result.WarnResult.Stderr)
				break
			} else {
				go func(result MapResult) {
					tempPath := lib.Join(result.WarnResult.Tempdir, "output")
					err := localPut(tempPath, result.Outkey, this, servers)
					if err != nil {
						fail <- err
					} else {
						jobs <- nil
					}
				}(result)
			}
		}
	}()
	for i := 0; i < count; i++ {
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

func localPut(tempPath string, key string, this lib.Server, servers []lib.Server) error {
	if strings.Contains(key, " ") {
		return fmt.Errorf("key contains space: %s", key)
	}
	onThisServer, err := lib.OnThisServer(key, this, servers)
	if err != nil {
		return err
	}
	if !onThisServer {
		return fmt.Errorf("wrong server for key: %s", key)
	}
	path := strings.SplitN(key, "s4://", 2)[1]
	if strings.HasPrefix(path, "_") {
		return fmt.Errorf("path cannot start with underscore: %s", path)
	}
	var checksum string
	lib.With(miscPool, func() {
		checksum, err = lib.Checksum(tempPath)
	})
	if err != nil {
		return err
	}
	lib.With(soloPool, func() {
		err = confirmLocalPut(tempPath, path, checksum)
	})
	return err
}

func confirmLocalPut(tempPath string, path string, checksum string) error {
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
	err = os.Chmod(tempPath, 0o444)
	if err != nil {
		return err
	}
	err = os.Rename(tempPath, path)
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

func mapToNHandler(w http.ResponseWriter, r *http.Request, this lib.Server, servers []lib.Server) {
	var data lib.MapArgs
	bytes := panic2(ioutil.ReadAll(r.Body)).([]byte)
	panic1(json.Unmarshal(bytes, &data))
	indir, glob := lib.ParseGlob(data.Indir)
	outdir := data.Outdir
	assert(strings.HasSuffix(indir, "/"), fmt.Sprintf("indir not a directory: %s", indir))
	assert(strings.HasSuffix(outdir, "/"), fmt.Sprintf("outdir not a directory: %s", outdir))
	assert(strings.HasPrefix(outdir, "s4://"), fmt.Sprintf("outdir must start with s4://, got: %s", outdir))
	pth := strings.SplitN(indir, "://", 2)[1]
	files, _ := listRecursive(pth, true)
	pth = strings.SplitN(pth, "/", 2)[1]
	if strings.HasPrefix(data.Cmd, "while read") {
		data.Cmd = fmt.Sprintf("cat | %s", data.Cmd)
	}
	count := 0
	results := make(chan MapToNResult, len(*files))
	for _, file := range *files {
		size := file.Size
		key := file.Path
		if pth != "" {
			key = strings.SplitN(key, pth, 2)[1]
		}
		assert(size != "PRE", fmt.Sprintf("map-to-n got a directory instead of a key: %s", key))
		if glob != "" {
			match := panic2(path.Match(glob, key)).(bool)
			if !match {
				continue
			}
		}
		inkey := lib.Join(indir, key)
		inpath := panic2(filepath.Abs(strings.SplitN(inkey, "s4://", 2)[1])).(string)
		go func(inpath string) {
			lib.With(cpuPool, func() {
				result := lib.WarnTempdir(fmt.Sprintf("export filename=%s; < %s %s", path.Base(inpath), inpath, data.Cmd))
				results <- MapToNResult{result, inpath, outdir}
			})
		}(inpath)
		count++
	}
	var tempdirs []string
	defer cleanup(&tempdirs)
	timeout := time.After(lib.MaxTimeout)
	fail := make(chan error)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < count; i++ {
			result := <-results
			tempdirs = append(tempdirs, result.WarnResult.Tempdir)
			if result.WarnResult.Err != nil {
				fail <- fmt.Errorf(result.WarnResult.Stdout + "\n" + result.WarnResult.Stderr)
				break
			} else {
				for _, tempPath := range strings.Split(result.WarnResult.Stdout, "\n") {
					if tempPath != "" {
						wg.Add(1)
						tempPath = lib.Join(result.WarnResult.Tempdir, tempPath)
						outkey := lib.Join(result.Outdir, path.Base(result.Inpath), path.Base(tempPath))
						go mapToNPut(&wg, fail, tempPath, outkey, this, servers)
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

func mapToNPut(wg *sync.WaitGroup, fail chan<- error, tempPath string, outkey string, this lib.Server, servers []lib.Server) {
	defer wg.Done()
	onThisServer, err := lib.OnThisServer(outkey, this, servers)
	if err != nil {
		fail <- err
		return
	}
	if onThisServer {
		err := localPut(tempPath, outkey, this, servers)
		if err != nil {
			fail <- err
		}
	} else {
		err := lib.Retry(func() error {
			var err error
			lib.With(ioSendPool, func() {
				err = s4.PutFile(tempPath, outkey, servers)
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

func mapFromNHandler(w http.ResponseWriter, r *http.Request, this lib.Server, servers []lib.Server) {
	var data lib.MapArgs
	bytes := panic2(ioutil.ReadAll(r.Body)).([]byte)
	panic1(json.Unmarshal(bytes, &data))
	indir, glob := lib.ParseGlob(data.Indir)
	outdir := data.Outdir
	assert(strings.HasSuffix(indir, "/"), fmt.Sprintf("indir not a directory: %s", indir))
	assert(strings.HasSuffix(outdir, "/"), fmt.Sprintf("outdir not a directory: %s", outdir))
	assert(strings.HasPrefix(outdir, "s4://") && strings.HasSuffix(outdir, "/"), outdir)
	pth := strings.Split(indir, "://")[1]
	files, _ := listRecursive(pth, true)
	parts := strings.SplitN(pth, "/", 2)
	bucket := parts[0]
	indir = parts[1]
	if strings.HasPrefix(data.Cmd, "while read") {
		data.Cmd = fmt.Sprintf("cat | %s", data.Cmd)
	}
	prefixes := make(map[string][]string)
	for _, file := range *files {
		key := file.Path
		if indir != "" {
			key = strings.SplitN(key, indir, 2)[1]
		}
		if glob != "" {
			match := panic2(path.Match(glob, key)).(bool)
			if !match {
				continue
			}
		}
		prefix := lib.KeyPrefix(key)
		prefixes[prefix] = append(prefixes[prefix], panic2(filepath.Abs(lib.Join(bucket, indir, key))).(string))
	}
	results := make(chan MapResult, len(prefixes))
	for prefix, inpaths := range prefixes {
		outkey := lib.Join(outdir, prefix+lib.Suffix(inpaths))
		go func(inpaths []string) {
			lib.With(cpuPool, func() {
				stdin := strings.NewReader(strings.Join(inpaths, "\n") + "\n")
				result := lib.WarnTempdirStreamIn(stdin, fmt.Sprintf("%s > output", data.Cmd))
				results <- MapResult{result, outkey}
			})
		}(inpaths)
	}
	var tempdirs []string
	defer cleanup(&tempdirs)
	timeout := time.After(lib.MaxTimeout)
	jobs := make(chan error, len(prefixes))
	fail := make(chan error, 1)
	go func() {
		for range prefixes {
			result := <-results
			tempdirs = append(tempdirs, result.WarnResult.Tempdir)
			if result.WarnResult.Err != nil {
				fail <- fmt.Errorf(result.WarnResult.Stdout + "\n" + result.WarnResult.Stderr)
				break
			} else {
				go func(result MapResult) {
					tempPath := lib.Join(result.WarnResult.Tempdir, "output")
					err := localPut(tempPath, result.Outkey, this, servers)
					if err != nil {
						fail <- err
					} else {
						jobs <- nil
					}
				}(result)
			}
		}
	}()
	for range prefixes {
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

func evalHandler(w http.ResponseWriter, r *http.Request, this lib.Server, servers []lib.Server) {
	key := lib.QueryParam(r, "key")
	assert(panic2(lib.OnThisServer(key, this, servers)).(bool), "wrong server for request")
	cmd := panic2(ioutil.ReadAll(r.Body)).([]byte)
	path := strings.SplitN(key, "s4://", 2)[1]
	var exists bool
	lib.With(soloPool, func() {
		exists = panic2(lib.Exists(path)).(bool)
	})
	if !exists {
		w.WriteHeader(404)
	} else {
		lib.With(cpuPool, func() {
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

func listRecursive(prefix string, stripBucket bool) (*[]*File, *[]*File) {
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
			isChecksum := lib.IsChecksum(fullpath)
			if matched && !isChecksum {
				path := fullpath
				if stripBucket {
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
			isChecksum := lib.IsChecksum(name)
			if matched && !isChecksum {
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

func listHandler(w http.ResponseWriter, r *http.Request) {
	prefix := lib.QueryParam(r, "prefix")
	assert(strings.HasPrefix(prefix, "s4://"), prefix)
	prefix = strings.Split(prefix, "s4://")[1]
	recursive := lib.QueryParamDefault(r, "recursive", "false") == "true"
	var res *[]*File
	lib.With(miscPool, func() {
		if recursive {
			res, _ = listRecursive(prefix, true)
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

func listBucketsHandler(w http.ResponseWriter) {
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

func healthHandler(w http.ResponseWriter) {
	panic2(fmt.Fprintf(w, "healthy\n"))
}

func notFoundHandler(w http.ResponseWriter) {
	w.WriteHeader(404)
	panic2(fmt.Fprintf(w, "404\n"))
}

func expireJobs() {
	ioJobs.Range(func(k, v interface{}) bool {
		var start time.Time
		var path string
		var tempPath string
		switch v := v.(type) {
		case *PutJob:
			start = v.start
			path = v.path
			tempPath = v.tempPath
		case *GetJob:
			start = v.start
		}
		if time.Since(start) > lib.MaxTimeout {
			lib.Logger.Printf("gc expired job: %s %s %s\n", k, path, tempPath)
			go func() {
				lib.With(miscPool, func() {
					if path != "" {
						_ = os.Remove(path)
					}
					if tempPath != "" {
						_ = os.Remove(tempPath)
					}
				})
			}()
			ioJobs.Delete(k)
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

func initPools(maxIOJobs int, maxCPUJobs int) {
	ioSendPool = semaphore.NewWeighted(int64(maxIOJobs))
	ioRecvPool = semaphore.NewWeighted(int64(maxIOJobs))
	cpuPool = semaphore.NewWeighted(int64(maxCPUJobs))
	miscPool = semaphore.NewWeighted(int64(maxCPUJobs))
	soloPool = semaphore.NewWeighted(int64(1))
}

func rootHandler(w http.ResponseWriter, r *http.Request, this lib.Server, servers []lib.Server) {
	switch r.Method {
	case "GET":
		switch r.URL.Path {
		case "/list":
			listHandler(w, r)
		case "/list_buckets":
			listBucketsHandler(w)
		case "/health":
			healthHandler(w)
		default:
			notFoundHandler(w)
		}
	case "POST":
		switch r.URL.Path {
		case "/prepare_put":
			preparePutHandler(w, r, this, servers)
		case "/confirm_put":
			confirmPutHandler(w, r)
		case "/prepare_get":
			prepareGetHandler(w, r, this, servers)
		case "/confirm_get":
			confirmGetHandler(w, r)
		case "/delete":
			deleteHandler(r, this, servers)
		case "/map":
			mapHandler(w, r, this, servers)
		case "/map_to_n":
			mapToNHandler(w, r, this, servers)
		case "/map_from_n":
			mapFromNHandler(w, r, this, servers)
		case "/eval":
			evalHandler(w, r, this, servers)
		default:
			notFoundHandler(w)
		}
	default:
		notFoundHandler(w)
	}
}

func main() {
	panic1(os.Setenv("LC_ALL", "C"))
	panic1(os.MkdirAll("s4_data/_tempfiles", os.ModePerm))
	panic1(os.MkdirAll("s4_data/_tempdirs", os.ModePerm))
	panic1(os.Chdir("s4_data"))
	numCpus := runtime.GOMAXPROCS(0)
	port := flag.Int("port", 0, "specify port instead of matching a single conf entry by ipv4")
	maxIOJobs := flag.Int("max-io-jobs", numCpus*4, "specify max-io-jobs to use instead of cpus*4")
	maxCPUJobs := flag.Int("max-cpu-jobs", numCpus+2, "specify max-cpu-jobs to use instead of cpus+2")
	confPath := flag.String("conf", lib.DefaultConfPath(), "specify conf path to use instead of ~/.s4.conf")
	flag.Parse()
	initPools(*maxIOJobs, *maxCPUJobs)
	servers := panic2(lib.GetServers(*confPath)).([]lib.Server)
	this := lib.ThisServer(*port, servers)
	portStr := fmt.Sprintf(":%s", this.Port)
	lib.Logger.Println("s4-server", portStr)
	go expiredDataDeleter()
	server := &http.Server{
		ReadTimeout:  lib.MaxTimeout,
		WriteTimeout: lib.MaxTimeout,
		IdleTimeout:  lib.MaxTimeout,
		Addr:         portStr,
		Handler: &lib.RootHandler{
			Handler: rootHandler,
			This:    this,
			Servers: servers,
		},
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
