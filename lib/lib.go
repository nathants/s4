package lib

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/cespare/xxhash"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/crypto/blake2s"
	"golang.org/x/sync/semaphore"
)

const (
	Timeout    = 5 * time.Minute
	MaxTimeout = Timeout*2 + 15*time.Second
	bufSize    = 4096
	ioTimeout  = 5 * time.Second
)

var (
	client = http.Client{Timeout: MaxTimeout}
	Logger = log.New(os.Stdout, "", log.Ldate|log.Ltime)
)

type MapArgs struct {
	Cmd    string `json:"cmd"`
	Indir  string `json:"indir"`
	Outdir string `json:"outidr"`
}

func DefaultConfPath() string {
	env := os.Getenv("S4_CONF_PATH")
	if env != "" {
		return env
	}
	usr := panic2(user.Current()).(*user.User)
	return Join(usr.HomeDir, ".s4.conf")
}

type WarnResult struct {
	Stdout string
	Stderr string
	Err    error
}

func Warn(format string, args ...interface{}) *WarnResult {
	str := fmt.Sprintf(format, args...)
	str = fmt.Sprintf("set -eou pipefail; %s", str)
	cmd := exec.Command("bash", "-c", str)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	result := make(chan *WarnResult)
	go func() {
		err := cmd.Run()
		result <- &WarnResult{
			strings.TrimRight(stdout.String(), "\n"),
			strings.TrimRight(stderr.String(), "\n"),
			err,
		}
	}()
	select {
	case r := <-result:
		return r
	case <-time.After(Timeout):
		_ = cmd.Process.Kill()
		return &WarnResult{
			"",
			"",
			errors.New("cmd timeout"),
		}
	}
}

type WarnResultTempdir struct {
	Stdout  string
	Stderr  string
	Err     error
	Tempdir string
}

func WarnTempdir(format string, args ...interface{}) *WarnResultTempdir {
	tempdir := panic2(ioutil.TempDir("_tempdirs", "")).(string)
	str := fmt.Sprintf(format, args...)
	str = fmt.Sprintf("set -eou pipefail; cd %s; %s", tempdir, str)
	cmd := exec.Command("bash", "-c", str)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	result := make(chan *WarnResultTempdir)
	go func() {
		err := cmd.Run()
		result <- &WarnResultTempdir{
			strings.TrimRight(stdout.String(), "\n"),
			strings.TrimRight(stderr.String(), "\n"),
			err,
			tempdir,
		}
	}()
	select {
	case r := <-result:
		return r
	case <-time.After(Timeout):
		_ = cmd.Process.Kill()
		panic1(os.RemoveAll(tempdir))
		return &WarnResultTempdir{
			"",
			"",
			errors.New("cmd timeout"),
			"",
		}
	}
}

func WarnTempdirStreamIn(stdin io.Reader, format string, args ...interface{}) *WarnResultTempdir {
	tempdir := panic2(ioutil.TempDir("_tempdirs", "")).(string)
	str := fmt.Sprintf(format, args...)
	str = fmt.Sprintf("set -eou pipefail; cd %s; %s", tempdir, str)
	cmd := exec.Command("bash", "-c", str)
	cmd.Stdin = stdin
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	result := make(chan *WarnResultTempdir)
	go func() {
		err := cmd.Run()
		result <- &WarnResultTempdir{
			strings.TrimRight(stdout.String(), "\n"),
			strings.TrimRight(stderr.String(), "\n"),
			err,
			tempdir,
		}
	}()
	select {
	case r := <-result:
		return r
	case <-time.After(Timeout):
		_ = cmd.Process.Kill()
		panic1(os.RemoveAll(tempdir))
		return &WarnResultTempdir{
			"",
			"",
			errors.New("cmd timeout"),
			"",
		}
	}
}

type Server struct {
	Address string
	Port    string
}

func GetServers(confPath string) ([]Server, error) {
	var servers []Server
	bytes, err := ioutil.ReadFile(confPath)
	if err != nil {
		return []Server{}, err
	}
	lines := strings.Split(string(bytes), "\n")
	localAddresses, err := localAddresses()
	if err != nil {
		return []Server{}, err
	}
	for _, line := range lines {
		if strings.Trim(line, " ") == "" {
			continue
		}
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			return []Server{}, fmt.Errorf("bad config line: %s", line)
		}
		server := Server{parts[0], parts[1]}
		for _, address := range localAddresses {
			if server.Address == address {
				server.Address = "0.0.0.0"
				break
			}
		}
		servers = append(servers, server)
	}
	if len(servers) == 0 {
		return []Server{}, fmt.Errorf("empty config file")
	}
	return servers, nil
}

func localAddresses() ([]string, error) {
	vals := []string{"0.0.0.0", "localhost", "127.0.0.1"}
	ifaces, err := net.Interfaces()
	if err != nil {
		return []string{}, err
	}
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			return []string{}, err
		}
		for _, addr := range addrs {
			vals = append(vals, strings.SplitN(addr.String(), "/", 2)[0])
		}
	}
	return vals, nil
}

type HTTPResult struct {
	StatusCode int
	Body       []byte
	Err        error
}

func Post(url, contentType string, body io.Reader) *HTTPResult {
	resp, err := client.Post(url, contentType, body)
	if err == nil {
		body, err := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return &HTTPResult{-1, []byte{}, err}
		}
		return &HTTPResult{resp.StatusCode, body, nil}
	}
	return &HTTPResult{-1, []byte{}, err}
}

func Get(url string) *HTTPResult {
	resp, err := client.Get(url)
	if err == nil {
		body, err := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return &HTTPResult{-1, []byte{}, err}
		}
		return &HTTPResult{resp.StatusCode, body, nil}
	}
	return &HTTPResult{-1, []byte{}, err}
}

type rwcCallback struct {
	rwc io.ReadWriteCloser
	cb  func()
}

func (rwc rwcCallback) Read(p []byte) (int, error) {
	n, err := rwc.rwc.Read(p)
	go rwc.cb()
	return n, err
}

func (rwc rwcCallback) Write(p []byte) (int, error) {
	n, err := rwc.rwc.Write(p)
	go rwc.cb()
	return n, err
}

func (rwc rwcCallback) Close() error {
	err := rwc.rwc.Close()
	go rwc.cb()
	return err
}

func hash(str string) uint64 {
	h := blake2s.Sum256([]byte(str))
	return binary.LittleEndian.Uint64(h[:])
}

func OnThisServer(key string, this Server, servers []Server) (bool, error) {
	if !strings.HasPrefix(key, "s4://") {
		return false, fmt.Errorf("missing s4:// prefix: %s", key)
	}
	picked, err := PickServer(key, servers)
	if err != nil {
		return false, err
	}
	return picked.Address == this.Address && picked.Port == this.Port, nil
}

func PickServer(key string, servers []Server) (Server, error) {
	if strings.HasSuffix(key, "/") {
		return Server{}, fmt.Errorf("needed key, got directory: %s", key)
	}
	if !strings.HasPrefix(key, "s4://") {
		return Server{}, fmt.Errorf("missing s4:// prefix: %s", key)
	}
	prefix := KeyPrefix(key)
	tmp, err := strconv.Atoi(prefix)
	var val uint64
	if err != nil {
		val = hash(prefix)
	} else {
		val = uint64(tmp)
	}
	index := val % uint64(len(servers))
	return servers[index], nil
}

func isDigits(str string) bool {
	_, err := strconv.Atoi(str)
	return err == nil
}

func KeyPrefix(key string) string {
	key = Last(strings.Split(key, "/"))
	prefix := strings.Split(key, "_")[0]
	if !isDigits(prefix) {
		prefix = key
	}
	return prefix
}

func keySuffix(key string) (string, bool) {
	if !isDigits(KeyPrefix(key)) {
		return "", false
	}
	part := Last(strings.Split(key, "/"))
	parts := strings.SplitN(part, "_", 2)
	if len(parts) == 2 {
		return parts[1], true
	}
	return "", false
}

func Suffix(keys []string) string {
	suffixes := make(map[string]string)
	var suffix string
	var ok bool
	for _, key := range keys {
		suffix, ok = keySuffix(key)
		if !ok {
			return ""
		}
		suffixes[suffix] = ""
		if len(suffixes) != 1 {
			return ""
		}
	}
	if len(suffixes) != 1 {
		return ""
	}
	return fmt.Sprintf("_%s", suffix)
}

func NewTempPath(dir string) string {
	for i := 0; i < 5; i++ {
		uid := uuid.NewV4().String()
		tempPath := panic2(filepath.Abs(Join(dir, uid))).(string)
		_, err := os.Stat(tempPath)
		if err != nil {
			f := panic2(os.Create(tempPath)).(*os.File)
			panic1(f.Close())
			return tempPath
		}
	}
	panic("failure")
}

func ChecksumWrite(path string, checksum string) error {
	checksumPath, err := ChecksumPath(path)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(checksumPath, []byte(checksum), 0o444)
}

func ChecksumRead(path string) (string, error) {
	checksumPath, err := ChecksumPath(path)
	if err != nil {
		return "", err
	}
	bytes, err := ioutil.ReadFile(checksumPath)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func Checksum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	bf := bufio.NewReaderSize(f, bufSize)
	val := xxh(bf)
	err = f.Close()
	if err != nil {
		return "", err
	}
	return val, nil
}

func ChecksumPath(prefix string) (string, error) {
	if strings.HasSuffix(prefix, "/") {
		return "", fmt.Errorf("checksum path is not file: %s", prefix)
	}
	return fmt.Sprintf("%s.xxh", prefix), nil
}

func IsChecksum(path string) bool {
	return strings.HasSuffix(path, ".xxh")
}

func Last(parts []string) string {
	return parts[len(parts)-1]
}

func RecvFile(path string, port chan<- string) (string, error) {
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	bf := bufio.NewWriterSize(f, bufSize)
	checksum, err := Recv(bf, port)
	if err != nil {
		return "", err
	}
	err = bf.Flush()
	if err != nil {
		return "", err
	}
	err = f.Close()
	if err != nil {
		return "", err
	}
	return checksum, nil
}

func resetableTimeout(duration time.Duration) (func(), <-chan error) {
	reset := make(chan error, 1)
	timeout := make(chan error, 1)
	start := time.Now()
	go func() {
		for {
			select {
			case <-reset:
				start = time.Now()
			case <-time.After(duration - time.Since(start)):
				timeout <- nil
				return
			}
		}
	}()
	resetFn := func() {
		reset <- nil
	}
	return resetFn, timeout
}

func Recv(w io.Writer, port chan<- string) (string, error) {
	fail := make(chan error)
	checksum := make(chan string)
	reset, timeout := resetableTimeout(ioTimeout)
	go func() {
		h := xxhash.New()
		var li net.Listener
		var err error
		err = Retry(func() error {
			li, err = net.Listen("tcp", ":0")
			return err
		})
		if err != nil {
			fail <- err
			return
		}
		port <- Last(strings.Split(li.Addr().String(), ":"))
		conn, err := li.Accept()
		if err != nil {
			fail <- err
			return
		}
		rwc := rwcCallback{rwc: conn, cb: reset}
		t := io.TeeReader(rwc, h)
		_, err = io.Copy(w, t)
		if err != nil {
			fail <- err
			return
		}
		err = rwc.Close()
		if err != nil {
			fail <- err
			return
		}
		err = li.Close()
		if err != nil {
			fail <- err
			return
		}
		checksum <- fmt.Sprintf("%x", h.Sum64())
	}()
	select {
	case chk := <-checksum:
		return chk, nil
	case <-timeout:
		return "", fmt.Errorf("recv timeout")
	case err := <-fail:
		return "", err
	}
}

func SendFile(path string, addr string, port string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	bf := bufio.NewReaderSize(f, bufSize)
	checksum, err := Send(bf, addr, port)
	if err != nil {
		return "", err
	}
	err = f.Close()
	if err != nil {
		return "", err
	}
	return checksum, nil
}

func Send(r io.Reader, addr string, port string) (string, error) {
	reset, timeout := resetableTimeout(ioTimeout)
	fail := make(chan error)
	checksum := make(chan string)
	go func() {
		h := xxhash.New()
		dst := fmt.Sprintf("%s:%s", addr, port)
		var conn net.Conn
		err := Retry(func() error {
			var err error
			conn, err = net.Dial("tcp", dst)
			return err
		})
		if err != nil {
			fail <- err
			return
		}
		rwc := rwcCallback{rwc: conn, cb: reset}
		t := io.TeeReader(r, h)
		_, err = io.Copy(rwc, t)
		if err != nil {
			fail <- err
			return
		}
		err = rwc.Close()
		if err != nil {
			fail <- err
			return
		}
		checksum <- fmt.Sprintf("%x", h.Sum64())
	}()
	select {
	case chk := <-checksum:
		return chk, nil
	case <-timeout:
		return "", fmt.Errorf("Send timeout")
	case err := <-fail:
		return "", err
	}
}

func xxh(r io.Reader) string {
	h := xxhash.New()
	panic2(io.Copy(h, r))
	sum := h.Sum64()
	return fmt.Sprintf("%x", sum)
}

type responseObserver struct {
	http.ResponseWriter
	Status int
}

func (o *responseObserver) Write(p []byte) (int, error) {
	return o.ResponseWriter.Write(p)
}

func (o *responseObserver) WriteHeader(code int) {
	o.ResponseWriter.WriteHeader(code)
	o.Status = code
}

type RootHandler struct {
	Handler func(w http.ResponseWriter, r *http.Request, this Server, servers []Server)
	This    Server
	Servers []Server
}

func (h *RootHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		if err := recover(); err != nil {
			Logger.Printf("panic handled: %s\n", err)
			w.WriteHeader(500)
			panic2(fmt.Fprintf(w, "%s\n", err))
			seconds := fmt.Sprintf("%.5f", time.Since(start).Seconds())
			Logger.Println(500, r.Method, r.URL.Path+"?"+r.URL.RawQuery, strings.Split(r.RemoteAddr, ":")[0], seconds)
		}
	}()
	wo := &responseObserver{w, 200}
	h.Handler(wo, r, h.This, h.Servers)
	seconds := fmt.Sprintf("%.5f", time.Since(start).Seconds())
	Logger.Println(wo.Status, r.Method, r.URL.Path+"?"+r.URL.RawQuery, strings.Split(r.RemoteAddr, ":")[0], seconds)
}

func Dir(pth string) string {
	pth = path.Dir(pth)
	if pth == "." {
		pth = ""
	}
	return pth
}

func Join(parts ...string) string {
	res := parts[0]
	for _, part := range parts[1:] {
		res = strings.TrimRight(res, "/")
		if strings.HasPrefix(part, "/") {
			res = part
		} else {
			res = fmt.Sprintf("%s/%s", res, part)
		}
	}
	return res
}

func Exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		return false, nil
	}
	checksumPath, err := ChecksumPath(path)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(checksumPath)
	return err == nil, nil
}

func Contains(parts []string, part string) bool {
	for _, p := range parts {
		if p == part {
			return true
		}
	}
	return false
}

func Await(wg *sync.WaitGroup) <-chan error {
	done := make(chan error)
	go func() {
		wg.Wait()
		done <- nil
	}()
	return done
}

func Retry(fn func() error) error {
	return retry.Do(
		fn,
		retry.LastErrorOnly(true),
		retry.Attempts(10),
		retry.Delay(10*time.Millisecond),
	)
}

func ParseGlob(indir string) (string, string) {
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
	return indir, glob
}

func With(pool *semaphore.Weighted, fn func()) {
	panic1(pool.Acquire(context.Background(), 1))
	defer func() { pool.Release(1) }()
	fn()
}

func QueryParam(r *http.Request, name string) string {
	vals := r.URL.Query()[name]
	assert(len(vals) == 1, "missing query parameter: %s", name)
	return vals[0]
}

func QueryParamDefault(r *http.Request, name string, defaultVal string) string {
	vals := r.URL.Query()[name]
	switch len(vals) {
	case 0:
		return defaultVal
	case 1:
		return vals[0]
	default:
		panic(len(vals))
	}
}

func ThisServer(port int, servers []Server) Server {
	var this Server
	if port == 0 {
		count := 0
		for _, server := range servers {
			if server.Address == "0.0.0.0" {
				this = server
				count++
			}
		}
		assert(count == 1, "unless -port is specified, conf should have exactly one entry per server address")
	} else {
		count := 0
		for _, server := range servers {
			if server.Address == "0.0.0.0" && server.Port == fmt.Sprint(port) {
				this = server
				count++
			}
		}
		assert(count == 1, "when -port is specified, conf should have exactly one entry per server (address, port)")
	}
	return this
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
