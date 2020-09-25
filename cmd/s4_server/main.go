package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"runtime"
	"s4/lib"
	"strings"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/phayes/freeport"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/sync/semaphore"
)

const timeout = 5 * time.Second

const printf = "-printf '%TY-%Tm-%Td %TH:%TM:%TS %s %p\n'"

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
	result        chan *lib.Result
	disk_checksum string
}

type PutJob struct {
	start     time.Time
	result    chan *lib.Result
	path      string
	temp_path string
}

func ConfirmGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	uids := r.URL.Query()["uuid"]
	if len(uids) != 1 {
		w.WriteHeader(400)
		return
	}
	uid := uids[0]
	client_checksums := r.URL.Query()["uuid"]
	if len(client_checksums) != 1 {
		w.WriteHeader(400)
		return
	}
	client_checksum := client_checksums[0]
	v, ok := io_jobs.LoadAndDelete(uid)
	if !ok {
		w.WriteHeader(404)
		return
	}
	job := v.(*GetJob)
	result := <-job.result
	if result.Err != nil {
		w.WriteHeader(500)
		Panic2(fmt.Fprintf(w, result.Stdout+"\n"+result.Stderr))
		return
	}
	server_checksum := result.Stderr
	if client_checksum != server_checksum {
		w.WriteHeader(500)
		Panic2(fmt.Fprintf(w, "checksum mismatch: %s %s\n", client_checksum, server_checksum))
		return
	}
	w.WriteHeader(200)
}

func PrepareGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	ports := r.URL.Query()["port"]
	if len(ports) != 1 {
		w.WriteHeader(400)
		return
	}
	port := ports[0]
	keys := r.URL.Query()["key"]
	if len(keys) != 1 {
		w.WriteHeader(400)
		return
	}
	key := keys[0]
	if !lib.OnThisServer(key) {
		w.WriteHeader(500)
		Panic2(fmt.Fprintf(w, "wrong server for request\n"))
		return
	}
	remote := r.RemoteAddr
	path := strings.Split(key, "s4://")[1]

	Panic1(solo_pool.Acquire(context.TODO(), 1))
	if !lib.Exists(path) || !lib.Exists(checksum_path(path)) {
		w.WriteHeader(404)
		return
	}
	solo_pool.Release(1)
	uid := fmt.Sprintf("%s", uuid.NewV4())
	started := make(chan bool, 1)
	result := make(chan *lib.Result, 1)
	go func() {
		Panic1(io_send_pool.Acquire(context.TODO(), 1))
		started <- true
		result <- lib.Warn("< %s s4-xxh --stream | send %s %s", remote, port)
		io_send_pool.Release(1)
	}()
	Panic1(solo_pool.Acquire(context.TODO(), 1))
	if !lib.Exists(path) || !lib.Exists(checksum_path(path)) {
		w.WriteHeader(404)
		return
	}
	disk_checksum := Panic2(ioutil.ReadFile(checksum_path(path))).(string)
	solo_pool.Release(1)
	job := &GetJob{
		time.Now(),
		result,
		disk_checksum,
	}
	_, loaded := io_jobs.LoadOrStore(uid, job)
	if loaded {
		panic(uid)
	}
	select {
	case <-time.After(timeout):
		io_jobs.Delete(uid)
		w.WriteHeader(429)
	case <-started:
		Panic2(w.Write([]byte(uid)))
	}
}

func PreparePut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	keys := r.URL.Query()["key"]
	if len(keys) != 1 {
		w.WriteHeader(400)
		return
	}
	key := keys[0]
	if strings.Contains(key, " ") {
		w.WriteHeader(500)
		Panic2(fmt.Fprintf(w, "key contains spaces: %s\n", key))
		return
	}
	if !lib.OnThisServer(key) {
		w.WriteHeader(500)
		Panic2(fmt.Fprintf(w, "wrong server for request\n"))
		return
	}
	path := strings.Split(key, "s4://")[1]
	if strings.HasSuffix(path, "_") {
		panic(path)
	}
	// reserve file
	Panic1(solo_pool.Acquire(context.TODO(), 1))
	port, err := freeport.GetFreePort()
	if lib.Exists(path) || lib.Exists(checksum_path(path)) {
		w.WriteHeader(409)
		return
	}
	temp_path := lib.NewTempPath("_tempfiles")
	if err != nil {
		panic(err)
	}
	solo_pool.Release(1)
	uid := fmt.Sprintf("%s", uuid.NewV4())
	started := make(chan bool, 1)
	result := make(chan *lib.Result, 1)
	go func() {
		Panic1(io_recv_pool.Acquire(context.TODO(), 1))
		started <- true
		result <- lib.Warn("recv %d | s4-xxh --stream > %s", port, temp_path)
		io_recv_pool.Release(1)
	}()
	job := &PutJob{
		time.Now(),
		result,
		path,
		temp_path,
	}
	_, loaded := io_jobs.LoadOrStore(uid, job)
	if loaded {
		panic(uid)
	}
	select {
	case <-time.After(timeout):
		io_jobs.Delete(uid)
		_ = os.Remove(path)
		_ = os.Remove(checksum_path(path))
		w.WriteHeader(429)
	case <-started:
		w.Header().Set("Content-Type", "application/json")
		bytes := Panic2(json.Marshal([]string{uid, string(port)}))
		Panic2(w.Write(bytes.([]byte)))
	}
}

func ConfirmPut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	uids := r.URL.Query()["uuid"]
	if len(uids) != 1 {
		w.WriteHeader(400)
		return
	}
	uid := uids[0]
	client_checksums := r.URL.Query()["checksum"]
	if len(client_checksums) != 1 {
		w.WriteHeader(400)
		return
	}
	client_checksum := client_checksums[0]
	v, ok := io_jobs.LoadAndDelete(uid)
	if !ok {
		w.WriteHeader(404)
		return
	}
	job := v.(*PutJob)
	result := <-job.result
	if result.Err != nil {
		w.WriteHeader(500)
		Panic2(fmt.Fprintf(w, result.Stdout+"\n"+result.Stderr))
		return
	}
	server_checksum := result.Stderr
	if client_checksum != server_checksum {
		w.WriteHeader(500)
		Panic2(fmt.Fprintf(w, "checksum mismatch: %s %s\n", client_checksum, server_checksum))
		return
	}
	Panic1(solo_pool.Acquire(context.TODO(), 1))
	defer solo_pool.Release(1)
	Panic1(os.MkdirAll(path.Dir(job.path), os.ModePerm))
	if lib.Exists(job.path) {
		w.WriteHeader(500)
		Panic2(fmt.Fprintf(w, "already lib.Exists: %s\n", job.path))
		return
	}
	if lib.Exists(checksum_path(job.path)) {
		w.WriteHeader(500)
		Panic2(fmt.Fprintf(w, "already lib.Exists: %s\n", checksum_path(job.path)))
		return
	}
	Panic1(ioutil.WriteFile(job.path, []byte(server_checksum), 0o444))
	Panic1(os.Chmod(job.temp_path, 0o444))
	Panic1(os.Rename(job.temp_path, job.path))
	w.WriteHeader(200)
}

func Delete(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	prefixes := r.URL.Query()["prefix"]
	if len(prefixes) != 1 {
		w.WriteHeader(400)
		return
	}
	prefix := prefixes[0]
	parts := strings.SplitN(prefix, "s4://", 2)
	prefix = parts[len(parts)-1]
	if strings.HasPrefix(prefix, "/") {
		panic(prefix)
	}
	recursive := false
	recursives := r.URL.Query()["recursive"]
	if len(recursives) == 1 && recursives[0] == "true" {
		recursive = true
	}
	Panic1(solo_pool.Acquire(context.TODO(), 1))
	defer solo_pool.Release(1)
	if recursive {
		lib.Run("rm -rf %s*", prefix)
	} else {
		lib.Run("rm -rf %s %s", prefix, checksum_path(prefix))
	}
}

func checksum_path(prefix string) string {
	if strings.HasSuffix(prefix, "/") {
		panic(prefix)
	}
	return fmt.Sprintf("%s.xxh3", prefix)
}

func Eval(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	keys := r.URL.Query()["prefix"]
	if len(keys) != 1 {
		w.WriteHeader(400)
		return
	}
	key := keys[0]
	cmd := Panic2(ioutil.ReadAll(r.Body)).([]byte)
	parts := strings.SplitN(key, "s4://", 2)
	path := parts[len(parts)-1]
	Panic1(solo_pool.Acquire(context.TODO(), 1))
	path_exists := lib.Exists(path)
	solo_pool.Release(1)
	if !path_exists {
		w.WriteHeader(404)
	} else {
		Panic1(cpu_pool.Acquire(context.TODO(), 1))
		defer cpu_pool.Release(1)
		res := lib.Warn("< %s %s", path, cmd)
		if res.Err == nil {
			Panic2(fmt.Fprintf(w, res.Stdout))
		} else {
			w.Header().Set("Content-Type", "application/json")
			bytes := Panic2(json.Marshal(res))
			Panic2(w.Write(bytes.([]byte)))
		}
	}
}

func List(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	prefixes := r.URL.Query()["prefix"]
	if len(prefixes) != 1 {
		w.WriteHeader(400)
		return
	}
	prefix := prefixes[0]
	if !strings.HasPrefix(prefix, "s4://") {
		w.WriteHeader(400)
		return
	}
	prefix = strings.Split(prefix, "s4://")[1]
	_prefix := prefix
	if !strings.HasSuffix(_prefix, "/") {
		_prefix += "/"
	}
	recursive := false
	recursives := r.URL.Query()["recursive"]
	if len(recursives) == 1 && recursives[0] == "true" {
		recursive = true
	}
	var xs [][]string
	if recursive {
		if !strings.HasSuffix(prefix, "/") {
			prefix += "*"
		}
		Panic1(misc_pool.Acquire(context.TODO(), 1))
		res := lib.Warn("find %s -type f ! -name '*.xxh3' %s", prefix, printf)
		misc_pool.Release(1)
		if !(res.Err == nil || strings.Contains(res.Stderr, "No such file or directory")) {
			panic(res.Stdout + "\n" + res.Stderr)
		}
		for _, line := range strings.Split(res.Stdout, "\n") {
			parts := strings.Split(line, " ")
			if len(parts) == 4 {
				date, time, size, path := parts[0], parts[1], parts[2], parts[3]
				xs = append(xs, []string{date, strings.Split(time, ".")[0], size, strings.Join(strings.Split(path, "/")[1:], "/")})
			}
		}
	} else {
		name := ""
		if !strings.HasSuffix(prefix, "/") {
			name = path.Base(prefix)
			name = fmt.Sprintf("-name '%s*'", name)
			prefix = path.Dir(prefix)
		}
		Panic1(misc_pool.Acquire(context.TODO(), 1))
		res := lib.Warn("find %s -maxdepth 1 -type f ! -name '*.xxh3' %s %s", prefix, name, printf)
		misc_pool.Release(1)
		if !(res.Err == nil || strings.Contains(res.Stderr, "No such file or directory")) {
			panic(res.Stdout + "\n" + res.Stderr)
		}
		var files [][]string
		for _, line := range strings.Split(res.Stdout, "\n") {
			parts := strings.Split(line, " ")
			if len(parts) > 0 && len(strings.TrimSpace(parts[len(parts)-1])) > 0 {
				files = append(files, parts)
			}
		}
		Panic1(misc_pool.Acquire(context.TODO(), 1))
		res = lib.Warn("find %s -mindepth 1 -maxdepth 1 -type d ! -name '*.xxh3' %s", prefix, name)
		misc_pool.Release(1)
		if !(res.Err == nil || strings.Contains(res.Stderr, "No such file or directory")) {
			panic(res.Stdout + "\n" + res.Stderr)
		}
		var dirs [][]string
		for _, line := range strings.Split(res.Stdout, "\n") {
			if line != "" {
				dirs = append(dirs, []string{"", "", "PRE", line + "/"})
			}
		}
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
	Panic1(misc_pool.Acquire(context.TODO(), 1))
	stdout := lib.Run("find -maxdepth 1 -mindepth 1 -type d ! -name '_*' %s", printf)
	misc_pool.Release(1)
	var res [][]string
	for _, line := range strings.Split(stdout, "\n") {
		parts := strings.Split(line, " ")
		if len(parts) == 4 {
			date, time, size, path := parts[0], parts[1], parts[2], parts[3]
			res = append(res, []string{date, time, size, path})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	bytes := Panic2(json.Marshal(res))
	Panic2(w.Write(bytes.([]byte)))
}

func Health(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	Panic2(fmt.Fprintf(w, "healthy\n"))
}

func main() {
	Panic1(os.Setenv("LC_ALL", "C"))
	_, err := os.Stat("s4_data")
	if err == nil {
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
	router.POST("/eval", Eval)
	router.GET("/list", List)
	router.GET("/list_buckets", ListBuckets)
	router.GET("/health", Health)
	port := fmt.Sprintf(":%d", 8080)
	fmt.Println("s4-server", port)
	Panic1(http.ListenAndServe(port, router))
}
