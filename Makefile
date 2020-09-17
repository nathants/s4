.PHONY: all clean test s4 s4-server s4-send s4-recv

all: s4 s4-server s4-send s4-recv

clean:
	rm -rf bin

setup:
	mkdir -p bin

s4: setup
	go build -o bin/s4 cmd/s4/main.go

s4-server: setup
	go build -o bin/s4-server cmd/s4_server/main.go

s4-send: setup
	go build -o bin/s4-send cmd/s4_send/main.go

s4-recv: setup
	go build -o bin/s4-recv cmd/s4_recv/main.go
