package s4

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/nathants/s4/lib"
)

func List(prefix string, recursive bool, servers []lib.Server) ([][]string, error) {
	recursiveParam := ""
	if recursive {
		recursiveParam = "&recursive=true"
	}
	results := make(chan *lib.HTTPResult)
	for _, server := range servers {
		go func(server lib.Server) {
			results <- lib.Get(fmt.Sprintf("http://%s:%s/list?prefix=%s%s", server.Address, server.Port, prefix, recursiveParam))
		}(server)
	}
	var lines [][]string
	for range servers {
		result := <-results
		if result.Err != nil {
			return [][]string{}, result.Err
		}
		if result.StatusCode != 200 {
			return [][]string{}, fmt.Errorf("%d %s", result.StatusCode, result.Body)
		}
		var tmp [][]string
		err := json.Unmarshal(result.Body, &tmp)
		if err != nil {
			return [][]string{}, err
		}
		lines = append(lines, tmp...)
	}
	sort.Slice(lines, func(i, j int) bool { return lines[i][3] < lines[j][3] })
	var deduped [][]string
	for _, val := range lines {
		if len(deduped) == 0 || deduped[len(deduped)-1][3] != val[3] {
			deduped = append(deduped, val)
		}
	}
	return deduped, nil
}

type httpRequest struct {
	url  string
	Data []byte
}

type httpResult struct {
	StatusCode int
	Body       []byte
	Err        error
	url        string
}

func postAll(requests []httpRequest, progress func()) error {
	results := make(chan *httpResult, len(requests))
	for _, request := range requests {
		go func(request httpRequest) {
			result := lib.Post(request.url, "application/json", bytes.NewBuffer(request.Data))
			results <- &httpResult{result.StatusCode, result.Body, result.Err, request.url}
		}(request)
	}
	for range requests {
		result := <-results
		if result.Err != nil {
			return result.Err
		}
		if result.StatusCode != 200 {
			return fmt.Errorf("fatal: %d %s\n%s", result.StatusCode, result.url, result.Body)
		}
		progress()
	}
	return nil
}

func Map(indir string, outdir string, cmd string, servers []lib.Server, progress func()) error {
	var requests []httpRequest
	for _, server := range servers {
		url := fmt.Sprintf("http://%s:%s/map", server.Address, server.Port)
		d := lib.MapArgs{Cmd: cmd, Indir: indir, Outdir: outdir}
		bytes, err := json.Marshal(d)
		if err != nil {
			return err
		}
		requests = append(requests, httpRequest{url, bytes})
	}
	return postAll(requests, progress)
}

func MapToN(indir string, outdir string, cmd string, servers []lib.Server, progress func()) error {
	var requests []httpRequest
	for _, server := range servers {
		url := fmt.Sprintf("http://%s:%s/map_to_n", server.Address, server.Port)
		d := lib.MapArgs{Cmd: cmd, Indir: indir, Outdir: outdir}
		bytes, err := json.Marshal(d)
		if err != nil {
			return err
		}
		requests = append(requests, httpRequest{url, bytes})
	}
	return postAll(requests, progress)
}

func MapFromN(indir string, outdir string, cmd string, servers []lib.Server, progress func()) error {
	var requests []httpRequest
	for _, server := range servers {
		url := fmt.Sprintf("http://%s:%s/map_from_n", server.Address, server.Port)
		d := lib.MapArgs{Cmd: cmd, Indir: indir, Outdir: outdir}
		bytes, err := json.Marshal(d)
		if err != nil {
			return err
		}
		requests = append(requests, httpRequest{url, bytes})
	}
	return postAll(requests, progress)
}

func Rm(prefix string, recursive bool, servers []lib.Server) error {
	if !strings.HasPrefix(prefix, "s4://") {
		return fmt.Errorf("missing s4:// prefix: %s", prefix)
	}
	if recursive {
		results := make(chan *lib.HTTPResult)
		for _, server := range servers {
			go func(server lib.Server) {
				results <- lib.Post(fmt.Sprintf("http://%s:%s/delete?prefix=%s&recursive=true", server.Address, server.Port, prefix), "application/text", bytes.NewBuffer([]byte{}))
			}(server)
		}
		for range servers {
			result := <-results
			if result.Err != nil {
				return result.Err
			}
			if result.StatusCode != 200 {
				return fmt.Errorf("%d %s", result.StatusCode, result.Body)
			}
		}
	} else {
		server, err := lib.PickServer(prefix, servers)
		if err != nil {
			return err
		}
		result := lib.Post(fmt.Sprintf("http://%s:%s/delete?prefix=%s", server.Address, server.Port, prefix), "application/text", bytes.NewBuffer([]byte{}))
		if result.Err != nil {
			return result.Err
		}
		if result.StatusCode != 200 {
			return fmt.Errorf("%d %s", result.StatusCode, result.Body)
		}
	}
	return nil
}

func Eval(key string, cmd string, servers []lib.Server) (string, error) {
	server, err := lib.PickServer(key, servers)
	if err != nil {
		return "", err
	}
	url := fmt.Sprintf("http://%s:%s/eval?key=%s", server.Address, server.Port, key)
	result := lib.Post(url, "application/text", bytes.NewBuffer([]byte(cmd)))
	if result.Err != nil {
		return "", result.Err
	}
	switch result.StatusCode {
	case 404:
		return "", fmt.Errorf("no such key: %s", key)
	case 200:
		return string(result.Body), nil
	default:
		return "", fmt.Errorf("%d %s", result.StatusCode, result.Body)
	}
}

func ListBuckets(servers []lib.Server) ([][]string, error) {
	results := make(chan *lib.HTTPResult)
	for _, server := range servers {
		go func(server lib.Server) {
			results <- lib.Get(fmt.Sprintf("http://%s:%s/list_buckets", server.Address, server.Port))
		}(server)
	}
	buckets := make(map[string][]string)
	for range servers {
		result := <-results
		if result.Err != nil {
			return [][]string{}, result.Err
		}
		if result.StatusCode != 200 {
			return [][]string{}, fmt.Errorf("%d %s", result.StatusCode, result.Body)
		}
		var res [][]string
		err := json.Unmarshal(result.Body, &res)
		if err != nil {
			return [][]string{}, err
		}
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
	return lines, nil
}

func getRecursive(src string, dst string, servers []lib.Server) error {
	part := strings.SplitN(src, "s4://", 2)[1]
	part = strings.TrimRight(part, "/")
	parts := strings.Split(part, "/")
	bucket := parts[0]
	parts = parts[1:]
	prefix := fmt.Sprintf("%s/", bucket)
	if len(parts) != 0 {
		prefix = strings.Join(parts, "/")
	}
	lines, err := List(src, true, servers)
	if err != nil {
		return err
	}
	for _, line := range lines {
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
			err := os.MkdirAll(lib.Dir(pth), os.ModePerm)
			if err != nil {
				return err
			}
		}
		err := Cp(fmt.Sprintf("s4://%s", lib.Join(bucket, key)), pth, false, servers)
		if err != nil {
			return err
		}
	}
	return nil
}

func putRecursive(src string, dst string, servers []lib.Server) error {
	return filepath.Walk(src, func(fullpath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			src = strings.TrimRight(src, "/")
			file := path.Base(fullpath)
			dirpath := lib.Dir(fullpath)
			pth := strings.TrimLeft(lib.Last(strings.SplitN(dirpath, src, 2)), "/")
			err := Cp(lib.Join(dirpath, file), lib.Join(dst, pth, file), false, servers)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func GetFile(src string, dst string, servers []lib.Server) error {
	server, err := lib.PickServer(src, servers)
	if err != nil {
		return err
	}
	port := make(chan string, 1)
	fail := make(chan error)
	tempPath := fmt.Sprintf("%s.temp", dst)
	defer func() { _ = os.Remove(tempPath) }()
	var clientChecksum string
	go func() {
		var err error
		clientChecksum, err = lib.RecvFile(tempPath, port)
		fail <- err
	}()
	url := fmt.Sprintf("http://%s:%s/prepare_get?key=%s&port=%s", server.Address, server.Port, src, <-port)
	result := lib.Post(url, "application/text", bytes.NewBuffer([]byte{}))
	if result.StatusCode == 404 {
		return fmt.Errorf("no such key: %s", src)
	}
	if result.StatusCode != 200 {
		return fmt.Errorf("%d %s", result.StatusCode, result.Body)
	}
	uid := result.Body
	err = <-fail
	if err != nil {
		return err
	}
	url = fmt.Sprintf("http://%s:%s/confirm_get?uuid=%s&checksum=%s", server.Address, server.Port, uid, clientChecksum)
	result = lib.Post(url, "application/text", bytes.NewBuffer([]byte{}))
	if result.StatusCode != 200 {
		return fmt.Errorf("%d %s", result.StatusCode, result.Body)
	}
	if strings.HasSuffix(dst, "/") {
		err := os.MkdirAll(dst, os.ModePerm)
		if err != nil {
			return err
		}
		dst = lib.Join(dst, path.Base(src))
	} else if dst == "." {
		dst = path.Base(src)
	}
	err = os.Rename(tempPath, dst)
	if err != nil {
		return err
	}
	return nil
}

func GetWriter(src string, dst io.Writer, servers []lib.Server) error {
	server, err := lib.PickServer(src, servers)
	if err != nil {
		return err
	}
	port := make(chan string, 1)
	fail := make(chan error)
	var clientChecksum string
	go func() {
		var err error
		clientChecksum, err = lib.Recv(os.Stdout, port)
		fail <- err
	}()
	url := fmt.Sprintf("http://%s:%s/prepare_get?key=%s&port=%s", server.Address, server.Port, src, <-port)
	result := lib.Post(url, "application/text", bytes.NewBuffer([]byte{}))
	if result.StatusCode == 404 {
		return fmt.Errorf("no such key: %s", src)
	}
	if result.StatusCode != 200 {
		return fmt.Errorf("%d %s", result.StatusCode, result.Body)
	}
	uid := result.Body
	err = <-fail
	if err != nil {
		return err
	}
	url = fmt.Sprintf("http://%s:%s/confirm_get?uuid=%s&checksum=%s", server.Address, server.Port, uid, clientChecksum)
	result = lib.Post(url, "application/text", bytes.NewBuffer([]byte{}))
	if result.StatusCode != 200 {
		return fmt.Errorf("%d %s", result.StatusCode, result.Body)
	}
	return nil
}

var Err409 = errors.New("409")

func PutFile(src string, dst string, servers []lib.Server) error {
	if strings.HasSuffix(dst, "/") {
		dst = lib.Join(dst, path.Base(src))
	}
	server, err := lib.PickServer(dst, servers)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s:%s/prepare_put?key=%s", server.Address, server.Port, dst)
	result := lib.Post(url, "application/text", bytes.NewBuffer([]byte{}))
	if result.Err != nil {
		return result.Err
	}
	if result.StatusCode == 409 {
		return fmt.Errorf("key already exists: %s %w", dst, Err409)
	}
	if result.StatusCode != 200 {
		return fmt.Errorf("%d %s", result.StatusCode, result.Body)
	}
	vals := strings.Split(string(result.Body), " ")
	if len(vals) != 2 {
		return fmt.Errorf("bad put response: %s", result.Body)
	}
	uid := vals[0]
	port := vals[1]
	clientChecksum, err := lib.SendFile(src, server.Address, port)
	if err != nil {
		return err
	}
	url = fmt.Sprintf("http://%s:%s/confirm_put?uuid=%s&checksum=%s", server.Address, server.Port, uid, clientChecksum)
	result = lib.Post(url, "application/text", bytes.NewBuffer([]byte{}))
	if result.Err != nil {
		return result.Err
	}
	if result.StatusCode != 200 {
		return fmt.Errorf("%d %s", result.StatusCode, result.Body)
	}
	return nil
}

func PutReader(src io.Reader, dst string, servers []lib.Server) error {
	server, err := lib.PickServer(dst, servers)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s:%s/prepare_put?key=%s", server.Address, server.Port, dst)
	result := lib.Post(url, "application/text", bytes.NewBuffer([]byte{}))
	if result.Err != nil {
		return result.Err
	}
	if result.StatusCode == 409 {
		return fmt.Errorf("key already exists: %s %w", dst, Err409)
	}
	if result.StatusCode != 200 {
		return fmt.Errorf("%d %s", result.StatusCode, result.Body)
	}
	vals := strings.Split(string(result.Body), " ")
	if len(vals) != 2 {
		return fmt.Errorf("bad put response: %s", result.Body)
	}
	uid := vals[0]
	port := vals[1]
	clientChecksum, err := lib.Send(os.Stdin, server.Address, port)
	if err != nil {
		return err
	}
	url = fmt.Sprintf("http://%s:%s/confirm_put?uuid=%s&checksum=%s", server.Address, server.Port, uid, clientChecksum)
	result = lib.Post(url, "application/text", bytes.NewBuffer([]byte{}))
	if result.Err != nil {
		return result.Err
	}
	if result.StatusCode != 200 {
		return fmt.Errorf("%d %s", result.StatusCode, result.Body)
	}
	return nil
}

func Cp(src string, dst string, recursive bool, servers []lib.Server) error {
	if strings.HasPrefix(src, "s4://") && strings.HasPrefix(dst, "s4://") {
		return fmt.Errorf("there is no move, there is only cp and rm")
	}
	if strings.Contains(src, " ") || strings.Contains(dst, " ") {
		return fmt.Errorf("spaces in keys are not allowed")
	}
	if strings.HasPrefix(src, "s4://") && strings.HasPrefix(strings.SplitN(src, "s4://", 2)[1], "_") {
		return fmt.Errorf("buckets cannot start with underscore")
	}
	if strings.HasPrefix(dst, "s4://") && strings.HasPrefix(strings.SplitN(dst, "s4://", 2)[1], "_") {
		return fmt.Errorf("buckets cannot start with underscore")
	}
	if recursive {
		if strings.HasPrefix(src, "s4://") {
			return getRecursive(src, dst, servers)
		} else if strings.HasPrefix(dst, "s4://") {
			return putRecursive(src, dst, servers)
		}
		return fmt.Errorf("fatal: src or dst needs s4://")
	} else if strings.HasPrefix(src, "s4://") {
		if dst == "-" {
			return GetWriter(src, os.Stdout, servers)
		}
		return GetFile(src, dst, servers)
	} else if strings.HasPrefix(dst, "s4://") {
		if src == "-" {
			return PutReader(os.Stdin, dst, servers)
		}
		return PutFile(src, dst, servers)
	} else {
		return fmt.Errorf("fatal: src or dst needs s4://")
	}
}
