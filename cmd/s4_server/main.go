package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"s4/lib"
	"strings"

	"github.com/julienschmidt/httprouter"
)

const printf = "-printf '%TY-%Tm-%Td %TH:%TM:%TS %s %p\n'"

func P1(e error) {
	if e != nil {
		panic(e)
	}
}

func P2(x interface{}, e error) interface{} {
	if e != nil {
		panic(e)
	}
	return x
}

func PreparePut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	P2(fmt.Fprintf(w, "404!!\n"))
}

func ConfirmPut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	P2(fmt.Fprintf(w, "404!!\n"))
}

func PrepareGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	P2(fmt.Fprintf(w, "404!!\n"))
}

func ConfirmGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	P2(fmt.Fprintf(w, "404!!\n"))
}

func Delete(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	P2(fmt.Fprintf(w, "404!!\n"))
}

func Eval(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	keys := r.URL.Query()["prefix"]
	if len(keys) != 1 {
		w.WriteHeader(400)
		return
	}
	key := keys[0]
	cmd := P2(ioutil.ReadAll(r.Body)).([]byte)

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
			panic(res.Stdout + res.Stderr)
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
			panic(res.Stdout + res.Stderr)
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
			panic(res.Stdout + res.Stderr)
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
			parts := strings.SplitN(path, _prefix, 1)
			path = parts[len(parts)-1]
			xs = append(xs, []string{date, time, size, path})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	bytes := P2(json.Marshal(xs))
	P2(w.Write(bytes.([]byte)))
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
	bytes := P2(json.Marshal(res))
	P2(w.Write(bytes.([]byte)))
}

func Health(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	P2(fmt.Fprintf(w, "healthy\n"))
}

func main() {
	P1(os.Setenv("LC_ALL", "C"))
	_, err := os.Stat("s4_data")
	if err == nil {
		P1(os.MkdirAll("s4_data/_tempfiles", os.ModePerm))
		P1(os.MkdirAll("s4_data/_tempdirs", os.ModePerm))
		P1(os.Chdir("s4_data"))
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
	P1(http.ListenAndServe(port, router))
}
