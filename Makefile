.PHONY: all clean test s4 s4-server check check-static check-ineff check-err check-vet test-lib

all: s4 s4-server

clean:
	rm -rf bin

setup:
	mkdir -p bin

s4: setup
	go build -o bin/s4 cmd/s4/main.go

s4-server: setup
	go build -o bin/s4-server cmd/s4_server/main.go

check: check-static check-ineff check-err check-vet check-lint

check-lint:
	find -name '*.go' | grep -v _test.go | xargs -n1 golint | grep -v unexported || true

check-static:
	find -name '*.go' | grep -v _test.go | xargs -n1 staticcheck

check-ineff:
	find -name '*.go' | grep -v _test.go | xargs -n1 ineffassign

check-err:
	find -name '*.go' | grep -v _test.go | xargs -n1 errcheck

check-vet:
	find -name '*.go' | grep -v _test.go | xargs -n1 go vet

test: test-lib tox

tox:
	tox

test-lib:
	go test -v lib/lib.go lib/lib_test.go
