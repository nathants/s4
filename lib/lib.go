package lib

import (
	"io"
)

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

func Test() string {
	return "asdf"
}
