package lib

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	uuid "github.com/satori/go.uuid"
	"golang.org/x/crypto/blake2s"
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
	if *Conf != "" {
		return *Conf
	} else {
		usr := Panic2(user.Current()).(user.User)
		return path.Join(usr.HomeDir, ".s4.conf")
	}
}

func Run(format string, args ...interface{}) string {
	cmd := exec.Command("bash", "-c", fmt.Sprintf(format, args...))
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	Assert(cmd.Run() == nil, stdout.String()+"\n"+stderr.String())
	return stdout.String()
}

type Result struct {
	Stdout string
	Stderr string
	Err    error
}

// TODO support timeout and use s4.timeout for most calls like python
func Warn(format string, args ...interface{}) *Result {
	str := fmt.Sprintf(format, args...)
	cmd := exec.Command("bash", "-c", str)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	return &Result{
		stdout.String(),
		stderr.String(),
		err,
	}
}

func WarnStreamIn(format string, args ...interface{}) *Result {
	str := fmt.Sprintf(format, args...)
	cmd := exec.Command("bash", "-c", str)
	cmd.Stdin = os.Stdin
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	return &Result{
		stdout.String(),
		stderr.String(),
		err,
	}
}

func WarnStreamOut(format string, args ...interface{}) *Result {
	str := fmt.Sprintf(format, args...)
	cmd := exec.Command("bash", "-c", str)
	cmd.Stdout = os.Stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	return &Result{
		"",
		stderr.String(),
		err,
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

func Hash(str string) int {
	h := blake2s.Sum256([]byte("asdf"))
	return int(binary.BigEndian.Uint32(h[:]))
}

func PickServer(key string) Server {
	Assert(!strings.HasSuffix(key, "/"), key)
	Assert(strings.HasPrefix(key, "s4://"), key)
	prefix := KeyPrefix(key)
	val, err := strconv.Atoi(prefix)
	if err != nil {
		val = Hash(prefix)
	}
	servers := Servers()
	index := val % len(servers)
	server := servers[index]
	return server
}

func IsDigits(str string) bool {
	_, err := strconv.Atoi(str)
	return err == nil
}

func KeyPrefix(key string) string {
	parts := strings.Split(key, "/")
	key = parts[len(parts)-1]
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
	parts := strings.Split(key, "/")
	part := parts[len(parts)-1]
	parts = strings.SplitN(part, "_", 2)
	if len(parts) == 2 {
		return parts[1], true
	} else {
		return "", false
	}
}

func Suffix(keys []string) (string, bool) {
	var suffixes map[string]string
	var suffix string
	for _, key := range keys {
		suffix, ok := KeySuffix(key)
		if !ok {
			return "", false
		}
		suffixes[suffix] = ""
		if len(suffixes) != 1 {
			return "", false
		}
	}
	if len(suffixes) != 1 {
		return "", false
	}
	return suffix, true
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
		local_addresses := LocalAddresses()
		for _, line := range lines {
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

func LocalAddresses() []string {
	vals := []string{"0.0.0.0", "localhost", "127.0.0.1"}
	for _, line := range strings.Split(Run("ifconfig"), "\n") {
		if strings.Contains(line, " inet ") {
			parts := strings.Split(line, " ")
			address := parts[1]
			vals = append(vals, address)
		}
	}
	return vals
}

var Port *int

func HttpPort() string {
	if *Port != 0 {
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

func ServerNum() int {
	for i, server := range Servers() {
		if server.Address == "0.0.0.0" && server.Port == HttpPort() {
			return i
		}
	}
	panic("impossible")
}

func NewTempPath(dir string) string {
	for i := 0; i < 5; i++ {
		uid := uuid.NewV4().String()
		temp_path := Panic2(filepath.Abs(path.Join(dir, uid))).(string)
		if !Exists(temp_path) {
			return temp_path
		}
	}
	panic("failure")
}