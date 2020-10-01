.PHONY: all clean test s4 s4-server s4-send s4-recv s4-xxh check check-static check-ineff check-err check-vet

all: s4 s4-server s4-send s4-recv s4-xxh

clean:
	rm -rf bin

setup:
	mkdir -p bin

s4: setup
	go build -o bin/s4 cmd/s4/main.go

s4-xxh: setup
	go build -o bin/s4-xxh cmd/s4_xxh/main.go

s4-server: setup
	go build -o bin/s4-server cmd/s4_server/main.go

s4-send: setup
	go build -o bin/s4-send cmd/s4_send/main.go

s4-recv: setup
	go build -o bin/s4-recv cmd/s4_recv/main.go

check: check-static check-ineff check-err check-vet

check-static:
	find -name '*.go' | xargs -n1 staticcheck

check-ineff:
	find -name '*.go' | xargs -n1 ineffassign

check-err:
	find -name '*.go' | xargs -n1 errcheck

check-vet:
	find -name '*.go' | xargs -n1 go vet
