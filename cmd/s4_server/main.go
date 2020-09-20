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

	"github.com/julienschmidt/httprouter"
	cmap "github.com/orcaman/concurrent-map"
	"golang.org/x/sync/semaphore"
)

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
	io_jobs      = cmap.New()
)

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

type Job struct {
	result    chan lib.Result
	path      string
	temp_path string
}

func ConfirmGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	panic2(fmt.Fprintf(w, "404!!\n"))
}

func PrepareGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	panic2(fmt.Fprintf(w, "404!!\n"))
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
		panic2(fmt.Fprintf(w, "key contains spaces: %s\n", key))
		return
	}
	if !lib.OnThisServer(key) {
		w.WriteHeader(500)
		panic2(fmt.Fprintf(w, "wrong server for request\n"))
		return
	}

}

func ConfirmPut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	uuids := r.URL.Query()["uuid"]
	if len(uuids) != 1 {
		w.WriteHeader(400)
		return
	}
	uuid := uuids[0]
	client_checksums := r.URL.Query()["checksum"]
	if len(client_checksums) != 1 {
		w.WriteHeader(400)
		return
	}
	client_checksum := client_checksums[0]
	v, ok := io_jobs.Pop(uuid)
	job := v.(Job)
	if !ok {
		w.WriteHeader(404)
		return
	}
	result := <-job.result
	if result.Err != nil {
		w.WriteHeader(500)
		panic2(fmt.Fprintf(w, result.Stdout+"\n"+result.Stderr))
		return
	}
	server_checksum := result.Stderr
	if client_checksum != server_checksum {
		w.WriteHeader(500)
		panic2(fmt.Fprintf(w, "checksum mismatch: %s %s\n", client_checksum, server_checksum))
		return
	}
	panic1(solo_pool.Acquire(context.TODO(), 1))
	defer solo_pool.Release(1)
	panic1(os.MkdirAll(path.Dir(job.path), os.ModePerm))
	if exists(job.path) {
		w.WriteHeader(500)
		panic2(fmt.Fprintf(w, "already exists: %s\n", job.path))
		return
	}
	if exists(checksum_path(job.path)) {
		w.WriteHeader(500)
		panic2(fmt.Fprintf(w, "already exists: %s\n", checksum_path(job.path)))
		return
	}
	panic1(ioutil.WriteFile(job.path, []byte(server_checksum), 0o444))
	panic1(os.Chmod(job.temp_path, 0o444))
	panic1(os.Rename(job.temp_path, job.path))
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
	panic1(solo_pool.Acquire(context.TODO(), 1))
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
	cmd := panic2(ioutil.ReadAll(r.Body)).([]byte)
	parts := strings.SplitN(key, "s4://", 2)
	path := parts[len(parts)-1]
	panic1(solo_pool.Acquire(context.TODO(), 1))
	path_exists := exists(path)
	solo_pool.Release(1)
	if !path_exists {
		w.WriteHeader(404)
	} else {
		panic1(cpu_pool.Acquire(context.TODO(), 1))
		defer cpu_pool.Release(1)
		res := lib.Warn("< %s %s", path, cmd)
		if res.Err == nil {
			panic2(fmt.Fprintf(w, res.Stdout))
		} else {
			w.Header().Set("Content-Type", "application/json")
			bytes := panic2(json.Marshal(res))
			panic2(w.Write(bytes.([]byte)))
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
		res := lib.Warn("find %s -type f ! -name '*.xxh3' %s", prefix, printf)
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
		res := lib.Warn("find %s -maxdepth 1 -type f ! -name '*.xxh3' %s %s", prefix, name, printf)
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
		res = lib.Warn("find %s -mindepth 1 -maxdepth 1 -type d ! -name '*.xxh3' %s", prefix, name)
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
	bytes := panic2(json.Marshal(xs))
	panic2(w.Write(bytes.([]byte)))
}

func ListBuckets(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	stdout := lib.Run("find -maxdepth 1 -mindepth 1 -type d ! -name '_*' %s", printf)
	var res [][]string
	for _, line := range strings.Split(stdout, "\n") {
		parts := strings.Split(line, " ")
		if len(parts) == 4 {
			date, time, size, path := parts[0], parts[1], parts[2], parts[3]
			res = append(res, []string{date, time, size, path})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	bytes := panic2(json.Marshal(res))
	panic2(w.Write(bytes.([]byte)))
}

func Health(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	panic2(fmt.Fprintf(w, "healthy\n"))
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return false
	} else {
		_, err = os.Stat(path + ".xxh3")
		return err == nil
	}
}

func main() {
	panic1(os.Setenv("LC_ALL", "C"))
	_, err := os.Stat("s4_data")
	if err == nil {
		panic1(os.MkdirAll("s4_data/_tempfiles", os.ModePerm))
		panic1(os.MkdirAll("s4_data/_tempdirs", os.ModePerm))
		panic1(os.Chdir("s4_data"))
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
	panic1(http.ListenAndServe(port, router))
}
