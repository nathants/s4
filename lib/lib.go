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
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/crypto/blake2s"
	"golang.org/x/sync/semaphore"
)

const (
	Timeout    = 5 * time.Minute
	MaxTimeout = Timeout*2 + 15*time.Second
	bufSize    = 4096
)

var (
	client = http.Client{Timeout: MaxTimeout}
	Logger = log.New(os.Stdout, "", log.Ldate|log.Ltime)
)

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
	} else {
		checksum_path, err := ChecksumPath(path)
		if err != nil {
			return false, err
		}
		_, err = os.Stat(checksum_path)
		return err == nil, nil
	}
}

var Conf *string

func ConfPath() string {
	if Conf != nil && *Conf != "" {
		return *Conf
	} else if os.Getenv("S4_CONF_PATH") != "" {
		return os.Getenv("S4_CONF_PATH")
	} else {
		usr := panic2(user.Current()).(*user.User)
		return Join(usr.HomeDir, ".s4.conf")
	}
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

type rwcCallback struct {
	rwc io.ReadWriteCloser
	cb  func()
}

func (rwc rwcCallback) Read(p []byte) (n int, err error) {
	defer rwc.cb()
	return rwc.rwc.Read(p)
}

func (rwc rwcCallback) Write(p []byte) (n int, err error) {
	defer rwc.cb()
	return rwc.rwc.Write(p)
}

func (rwc rwcCallback) Close() error {
	defer rwc.cb()
	return rwc.rwc.Close()
}

func hash(str string) int {
	h := blake2s.Sum256([]byte(str))
	return int(binary.BigEndian.Uint32(h[:]))
}

func OnThisServer(key string) (bool, error) {
	if !strings.HasPrefix(key, "s4://") {
		return false, fmt.Errorf("missing s4:// prefix: %s", key)
	}
	server, err := PickServer(key)
	if err != nil {
		return false, err
	}
	return server.Address == "0.0.0.0" && server.Port == HttpPort(), nil
}

func PickServer(key string) (*Server, error) {
	if strings.HasSuffix(key, "/") {
		return nil, fmt.Errorf("needed key, got directory: %s", key)
	}
	if !strings.HasPrefix(key, "s4://") {
		return nil, fmt.Errorf("missing s4:// prefix: %s", key)
	}
	prefix := KeyPrefix(key)
	val, err := strconv.Atoi(prefix)
	if err != nil {
		val = hash(prefix)
	}
	servers := Servers()
	index := val % len(servers)
	return &servers[index], nil
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
	} else {
		return "", false
	}
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

var servers *[]Server

type Server struct {
	Address string
	Port    string
}

func Servers() []Server {
	if servers == nil {
		servers = &[]Server{}
		bytes := panic2(ioutil.ReadFile(ConfPath())).([]byte)
		lines := strings.Split(string(bytes), "\n")
		local_addresses := localAddresses()
		for _, line := range lines {
			if strings.Trim(line, " ") == "" {
				continue
			}
			parts := strings.Split(line, ":")
			assert(len(parts) == 2, fmt.Sprint(parts))
			server := Server{parts[0], parts[1]}
			for _, address := range local_addresses {
				if server.Address == address {
					server.Address = "0.0.0.0"
					break
				}
			}
			*servers = append(*servers, server)
		}
	}
	assert(len(*servers) > 0, "%d", len(*servers))
	return *servers
}

func localAddresses() []string {
	vals := []string{"0.0.0.0", "localhost", "127.0.0.1"}
	ifaces, err := net.Interfaces()
	assert(err == nil, "%s", err)
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		assert(err == nil, "%s", err)
		for _, addr := range addrs {
			vals = append(vals, strings.SplitN(addr.String(), "/", 2)[0])
		}
	}
	return vals
}

var Port *int

func HttpPort() string {
	if Port != nil && *Port != 0 {
		return fmt.Sprint(*Port)
	} else {
		for _, server := range Servers() {
			if server.Address == "0.0.0.0" {
				return server.Port
			}
		}
		panic("impossible")
	}
}

type HttpResult struct {
	StatusCode int
	Body       []byte
	Err        error
}

func Post(url, contentType string, body io.Reader) *HttpResult {
	resp, err := client.Post(url, contentType, body)
	if err == nil {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return &HttpResult{-1, []byte{}, err}
		}
		return &HttpResult{resp.StatusCode, body, nil}
	} else {
		return &HttpResult{-1, []byte{}, err}
	}
}

func Get(url string) *HttpResult {
	resp, err := client.Get(url)
	if err == nil {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return &HttpResult{-1, []byte{}, err}
		}
		return &HttpResult{resp.StatusCode, body, nil}
	} else {
		return &HttpResult{-1, []byte{}, err}
	}
}

var Err409 = errors.New("409")

func Put(src string, dst string) error {
	if strings.HasSuffix(dst, "/") {
		dst = Join(dst, path.Base(src))
	}
	server, err := PickServer(dst)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s:%s/prepare_put?key=%s", server.Address, server.Port, dst)
	result := Post(url, "application/text", bytes.NewBuffer([]byte{}))
	if result.Err != nil {
		return result.Err
	}
	if result.StatusCode == 409 {
		return fmt.Errorf("fatal: key already exists: %s %w", dst, Err409)
	}
	if result.StatusCode != 200 {
		return fmt.Errorf("%d %s", result.StatusCode, result.Body)
	}
	vals := strings.Split(string(result.Body), " ")
	assert(len(vals) == 2, fmt.Sprint(vals))
	uid := vals[0]
	port := vals[1]
	var client_checksum string
	if src == "-" {
		client_checksum, err = send(os.Stdin, server.Address, port)
	} else {
		client_checksum, err = SendFile(src, server.Address, port)
	}
	if err != nil {
		return err
	}
	url = fmt.Sprintf("http://%s:%s/confirm_put?uuid=%s&checksum=%s", server.Address, server.Port, uid, client_checksum)
	result = Post(url, "application/text", bytes.NewBuffer([]byte{}))
	if result.Err != nil {
		return result.Err
	}
	if result.StatusCode != 200 {
		return fmt.Errorf("%d %s", result.StatusCode, result.Body)
	}
	return nil
}

type Data struct {
	Cmd  string     `json:"cmd"`
	Args [][]string `json:"args"`
}

func NewTempPath(dir string) string {
	for i := 0; i < 5; i++ {
		uid := uuid.NewV4().String()
		temp_path := panic2(filepath.Abs(Join(dir, uid))).(string)
		_, err := os.Stat(temp_path)
		if err != nil {
			f := panic2(os.Create(temp_path)).(*os.File)
			panic1(f.Close())
			return temp_path
		}
	}
	panic("failure")
}

func With(pool *semaphore.Weighted, fn func()) {
	panic1(pool.Acquire(context.Background(), 1))
	fn()
	pool.Release(1)
}

func QueryParam(r *http.Request, name string) string {
	vals := r.URL.Query()[name]
	assert(len(vals) == 1, "missing query parameter: %s", name)
	return vals[0]
}

func QueryParamDefault(r *http.Request, name string, default_val string) string {
	vals := r.URL.Query()[name]
	switch len(vals) {
	case 0:
		return default_val
	case 1:
		return vals[0]
	default:
		panic(len(vals))
	}
}

func ChecksumWrite(path string, checksum string) error {
	checksum_path, err := ChecksumPath(path)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(checksum_path, []byte(checksum), 0o444)
}

func ChecksumRead(path string) (string, error) {
	checksum_path, err := ChecksumPath(path)
	if err != nil {
		return "", err
	}
	bytes, err := ioutil.ReadFile(checksum_path)
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

func Recv(w io.Writer, port chan<- string) (string, error) {
	stop := make(chan error)
	fail := make(chan error)
	checksum := make(chan string)
	var start atomic.Value
	start.Store(time.Now())
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				if time.Since(start.Load().(time.Time)) > 5*time.Second {
					fail <- fmt.Errorf("recv timeout")
					return
				}
				time.Sleep(time.Microsecond * 10000)
			}
		}
	}()
	go func() {
		h := xxhash.New()
		li := panic2(net.Listen("tcp", ":0")).(net.Listener)
		port <- Last(strings.Split(li.Addr().String(), ":"))
		conn := panic2(li.Accept()).(net.Conn)
		rwc := rwcCallback{rwc: conn, cb: func() { start.Store(time.Now()) }}
		t := io.TeeReader(rwc, h)
		_, err := io.Copy(w, t)
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
	case c := <-checksum:
		stop <- nil
		return c, nil
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
	checksum, err := send(bf, addr, port)
	if err != nil {
		return "", err
	}
	err = f.Close()
	if err != nil {
		return "", err
	}
	return checksum, nil
}

func send(r io.Reader, addr string, port string) (string, error) {
	stop := make(chan error)
	fail := make(chan error)
	checksum := make(chan string)
	var start atomic.Value
	start.Store(time.Now())
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				if time.Since(start.Load().(time.Time)) > 5*time.Second {
					fail <- fmt.Errorf("send timeout")
					return
				}
				time.Sleep(time.Microsecond * 10000)
			}
		}
	}()
	go func() {
		h := xxhash.New()
		dst := fmt.Sprintf("%s:%s", addr, port)
		var conn net.Conn
		var err error
		for {
			conn, err = net.Dial("tcp", dst)
			if err == nil {
				break
			}
			time.Sleep(time.Microsecond * 10000)
		}
		rwc := rwcCallback{rwc: conn, cb: func() { start.Store(time.Now()) }}
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
		stop <- nil
		return chk, nil
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

type LoggingHandler struct {
	Handler http.Handler
}

func (l *LoggingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	wo := &responseObserver{w, 200}
	l.Handler.ServeHTTP(wo, r)
	seconds := fmt.Sprintf("%.5f", time.Since(start).Seconds())
	Logger.Println(wo.Status, r.Method, r.URL.Path+"?"+r.URL.RawQuery, strings.Split(r.RemoteAddr, ":")[0], seconds)
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
