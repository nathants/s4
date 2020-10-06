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

	"github.com/cespare/xxhash"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/crypto/blake2s"
	"golang.org/x/sync/semaphore"
)

const (
	Timeout    = 5 * time.Minute
	MaxTimeout = Timeout*2 + 15*time.Second
	Printf     = "-printf '%TY-%Tm-%Td %TH:%TM:%TS %s %p\n'"
	BufSize    = 4096
)

var (
	Logger = log.New(os.Stdout, "", log.Ldate|log.Ltime)
	Client = http.Client{Timeout: MaxTimeout}
)

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

func Exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return false
	} else {
		_, err = os.Stat(path + ".xxh3")
		return err == nil
	}
}

var Conf *string

func ConfPath() string {
	if Conf != nil && *Conf != "" {
		return *Conf
	} else if os.Getenv("S4_CONF_PATH") != "" {
		return os.Getenv("S4_CONF_PATH")
	} else {
		usr := Panic2(user.Current()).(*user.User)
		return Join(usr.HomeDir, ".s4.conf")
	}
}

func Run(format string, args ...interface{}) string {
	cmd := exec.Command("bash", "-c", fmt.Sprintf(format, args...))
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	Assert(cmd.Run() == nil, stdout.String()+"\n"+stderr.String())
	return strings.TrimRight(stdout.String(), "\n")
}

type CmdResult struct {
	Stdout string
	Stderr string
	Err    error
}

type CmdResultTempdir struct {
	Stdout  string
	Stderr  string
	Err     error
	Tempdir string
}

func Warn(format string, args ...interface{}) *CmdResult {
	str := fmt.Sprintf(format, args...)
	str = fmt.Sprintf("set -eou pipefail; %s", str)
	cmd := exec.Command("bash", "-c", str)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	result := make(chan *CmdResult)
	go func() {
		err := cmd.Run()
		result <- &CmdResult{
			strings.TrimRight(stdout.String(), "\n"),
			strings.TrimRight(stderr.String(), "\n"),
			err,
		}
	}()
	select {
	case r := <-result:
		return r
	case <-time.After(Timeout):
		Panic1(cmd.Process.Kill())
		return &CmdResult{
			"",
			"",
			errors.New("cmd timeout"),
		}
	}
}

func WarnTempdir(format string, args ...interface{}) *CmdResultTempdir {
	tempdir := Panic2(ioutil.TempDir("_tempdirs", "")).(string)
	str := fmt.Sprintf(format, args...)
	str = fmt.Sprintf("set -eou pipefail; cd %s; %s", tempdir, str)
	cmd := exec.Command("bash", "-c", str)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	result := make(chan *CmdResultTempdir)
	go func() {
		err := cmd.Run()
		result <- &CmdResultTempdir{
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
		Panic1(cmd.Process.Kill())
		Panic1(os.RemoveAll(tempdir))
		return &CmdResultTempdir{
			"",
			"",
			errors.New("cmd timeout"),
			"",
		}
	}
}

func WarnTempdirStreamIn(stdin io.Reader, format string, args ...interface{}) *CmdResultTempdir {
	tempdir := Panic2(ioutil.TempDir("_tempdirs", "")).(string)
	str := fmt.Sprintf(format, args...)
	str = fmt.Sprintf("set -eou pipefail; cd %s; %s", tempdir, str)
	cmd := exec.Command("bash", "-c", str)
	cmd.Stdin = stdin
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	result := make(chan *CmdResultTempdir)
	go func() {
		err := cmd.Run()
		result <- &CmdResultTempdir{
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
		Panic1(cmd.Process.Kill())
		Panic1(os.RemoveAll(tempdir))
		return &CmdResultTempdir{
			"",
			"",
			errors.New("cmd timeout"),
			"",
		}
	}
}

func WarnStreamIn(stdin io.Reader, format string, args ...interface{}) *CmdResult {
	str := fmt.Sprintf(format, args...)
	str = fmt.Sprintf("set -eou pipefail; %s", str)
	cmd := exec.Command("bash", "-c", str)
	cmd.Stdin = stdin
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	result := make(chan *CmdResult)
	go func() {
		err := cmd.Run()
		result <- &CmdResult{
			strings.TrimRight(stdout.String(), "\n"),
			strings.TrimRight(stderr.String(), "\n"),
			err,
		}
	}()
	select {
	case r := <-result:
		return r
	case <-time.After(Timeout):
		Panic1(cmd.Process.Kill())
		return &CmdResult{
			"",
			"",
			errors.New("cmd timeout"),
		}
	}
}

func WarnStreamOut(stdout io.Writer, format string, args ...interface{}) *CmdResult {
	str := fmt.Sprintf(format, args...)
	str = fmt.Sprintf("set -eou pipefail; %s", str)
	cmd := exec.Command("bash", "-c", str)
	cmd.Stdout = stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	result := make(chan *CmdResult)
	go func() {
		err := cmd.Run()
		result <- &CmdResult{
			"",
			strings.TrimRight(stderr.String(), "\n"),
			err,
		}
	}()
	select {
	case r := <-result:
		return r
	case <-time.After(Timeout):
		Panic1(cmd.Process.Kill())
		return &CmdResult{
			"",
			"",
			errors.New("cmd timeout"),
		}
	}
}

type RWCallback struct {
	Rw io.ReadWriteCloser
	Cb func()
}

func (rwc RWCallback) Read(p []byte) (n int, err error) {
	defer rwc.Cb()
	return rwc.Rw.Read(p)
}

func (rwc RWCallback) Write(p []byte) (n int, err error) {
	defer rwc.Cb()
	return rwc.Rw.Write(p)
}

func (rwc RWCallback) Close() error {
	defer rwc.Cb()
	return rwc.Rw.Close()
}

func OnThisServer(key string) bool {
	Assert(strings.HasPrefix(key, "s4://"), key)
	server := PickServer(key)
	return server.Address == "0.0.0.0" && server.Port == HttpPort()
}

func hash(str string) int {
	h := blake2s.Sum256([]byte(str))
	return int(binary.BigEndian.Uint32(h[:]))
}

func PickServer(key string) Server {
	Assert(!strings.HasSuffix(key, "/"), key)
	Assert(strings.HasPrefix(key, "s4://"), key)
	prefix := KeyPrefix(key)
	val, err := strconv.Atoi(prefix)
	if err != nil {
		val = hash(prefix)
	}
	servers := Servers()
	index := val % len(servers)
	return servers[index]
}

func IsDigits(str string) bool {
	_, err := strconv.Atoi(str)
	return err == nil
}

func KeyPrefix(key string) string {
	key = Last(strings.Split(key, "/"))
	prefix := strings.Split(key, "_")[0]
	if !IsDigits(prefix) {
		prefix = key
	}
	return prefix
}

func KeySuffix(key string) (string, bool) {
	if !IsDigits(KeyPrefix(key)) {
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
		suffix, ok = KeySuffix(key)
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

var cache = sync.Map{}

type Server struct {
	Address string
	Port    string
}

func Servers() []Server {
	val, ok := cache.Load("servers")
	var servers []Server
	if ok {
		servers = val.([]Server)
	} else {
		bytes := Panic2(ioutil.ReadFile(ConfPath())).([]byte)
		lines := strings.Split(string(bytes), "\n")
		local_addresses := localAddresses()
		for _, line := range lines {
			if strings.Trim(line, " ") == "" {
				continue
			}
			parts := strings.Split(line, ":")
			Assert(len(parts) == 2, fmt.Sprint(parts))
			server := Server{parts[0], parts[1]}
			for _, address := range local_addresses {
				if server.Address == address {
					server.Address = "0.0.0.0"
					break
				}
			}
			servers = append(servers, server)
		}
		cache.Store("servers", servers)
	}
	return servers
}

func localAddresses() []string {
	vals := []string{"0.0.0.0", "localhost", "127.0.0.1"}
	ifaces, err := net.Interfaces()
	Assert(err == nil, "%s", err)
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		Assert(err == nil, "%s", err)
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
	resp, err := Client.Post(url, contentType, body)
	if err == nil {
		body := Panic2(ioutil.ReadAll(resp.Body)).([]byte)
		return &HttpResult{resp.StatusCode, body, nil}
	} else {
		return &HttpResult{-1, []byte{}, err}
	}
}

func Get(url string) *HttpResult {
	resp, err := Client.Get(url)
	if err == nil {
		body := Panic2(ioutil.ReadAll(resp.Body)).([]byte)
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
	server := PickServer(dst)
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
	Assert(len(vals) == 2, fmt.Sprint(vals))
	uid := vals[0]
	port := vals[1]
	var client_checksum string
	if src == "-" {
		client_checksum = Send(os.Stdin, server.Address, port)
	} else {
		client_checksum = SendFile(src, server.Address, port)
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
		temp_path := Panic2(filepath.Abs(Join(dir, uid))).(string)
		if !Exists(temp_path) {
			return temp_path
		}
	}
	panic("failure")
}

func With(pool *semaphore.Weighted, fn func()) {
	Panic1(pool.Acquire(context.Background(), 1))
	fn()
	pool.Release(1)
}

func QueryParam(r *http.Request, name string) string {
	vals := r.URL.Query()[name]
	Assert(len(vals) == 1, "missing query parameter: %s", name)
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

func ChecksumWrite(path string, checksum string) {
	Panic1(ioutil.WriteFile(ChecksumPath(path), []byte(checksum), 0o444))
}

func ChecksumRead(path string) string {
	return string(Panic2(ioutil.ReadFile(ChecksumPath(path))).([]byte))
}

func Checksum(path string) string {
	f := Panic2(os.Open(path)).(*os.File)
	bf := bufio.NewReaderSize(f, BufSize)
	val := XXH(bf)
	Panic1(f.Close())
	return val
}

func ChecksumPath(prefix string) string {
	Assert(!strings.HasSuffix(prefix, "/"), prefix)
	return fmt.Sprintf("%s.xxh3", prefix)
}

func Last(parts []string) string {
	return parts[len(parts)-1]
}

func RecvFile(path string, port string) string {
	f := Panic2(os.Create(path)).(*os.File)
	bf := bufio.NewWriterSize(f, BufSize)
	checksum := Recv(bf, port)
	Panic1(bf.Flush())
	Panic1(f.Close())
	return checksum
}

func Recv(w io.Writer, port string) string {
	h := xxhash.New()
	timeout := 5 * time.Second
	src := fmt.Sprintf(":%s", port)
	li := Panic2(net.Listen("tcp", src)).(net.Listener)
	start := time.Now()
	go func() {
		for {
			Assert(time.Since(start) < timeout, "timeout reading")
			time.Sleep(time.Microsecond * 10000)
		}
	}()
	conn := Panic2(li.Accept()).(net.Conn)
	start = time.Now()
	rwc := RWCallback{Rw: conn, Cb: func() { start = time.Now() }}
	t := io.TeeReader(rwc, h)
	Panic2(io.Copy(w, t))
	Panic1(rwc.Close())
	Panic1(li.Close())
	checksum := fmt.Sprintf("%x", h.Sum64())
	return checksum
}

func SendFile(path string, addr string, port string) string {
	f := Panic2(os.Open(path)).(*os.File)
	bf := bufio.NewReaderSize(f, BufSize)
	checksum := Send(bf, addr, port)
	Panic1(f.Close())
	return checksum
}

func Send(r io.Reader, addr string, port string) string {
	h := xxhash.New()
	timeout := 5 * time.Second
	dst := fmt.Sprintf("%s:%s", addr, port)
	var conn net.Conn
	var err error
	start := time.Now()
	for {
		conn, err = net.DialTimeout("tcp", dst, timeout)
		if err == nil {
			break
		}
		Assert(time.Since(start) < timeout, "timeout dialing")
		time.Sleep(time.Microsecond * 10000)
	}
	start = time.Now()
	go func() {
		for {
			Assert(time.Since(start) < timeout, "timeout writing")
			time.Sleep(time.Microsecond * 10000)
		}
	}()
	rwc := RWCallback{Rw: conn, Cb: func() { start = time.Now() }}
	t := io.TeeReader(r, h)
	Panic2(io.Copy(rwc, t))
	Panic1(rwc.Close())
	checksum := fmt.Sprintf("%x", h.Sum64())
	return checksum
}

func XXH(r io.Reader) string {
	h := xxhash.New()
	Panic2(io.Copy(h, r))
	sum := h.Sum64()
	return fmt.Sprintf("%x", sum)
}
