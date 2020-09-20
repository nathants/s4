package lib

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
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

func Run(format string, args ...interface{}) string {
	cmd := exec.Command("bash", "-c", fmt.Sprintf(format, args...))
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		panic(stdout.String() + stderr.String())
	}
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
	// fmt.Println(str)
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
	if !strings.HasPrefix(key, "s4://") {
		panic(key)
	}
}

func PickServer(key string) string {
	if strings.HasSuffix(key, "/") {
		panic(key)
	}
	if !strings.HasPrefix(key, "s4://") {
		panic(key)
	}
	prefix = key_prefix(key)
	val, err := strconv.Atoi(prefix)
}

func key_prefix(key string) string {
	parts := strings.Split(key, "/")
	key = parts[len(parts)-1]
	prefix := strings.Split(key, "_")[0]
	_, err := strconv.Atoi(prefix)
	if err != nil {
		prefix = key
	}
	return prefix
}

func key_suffix(key string) (string, bool) {
	_, err := strconv.Atoi(key_prefix(key))
	if err != nil {
		return "", false
	}
	parts :=
}
