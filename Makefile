.PHONY: all clean test s4 s4-server check check-static check-ineff check-err check-vet test-lib check-bodyclose check-nargs check-fmt check-hasdefault check-hasdefer

all: s4 s4-server

clean:
	rm -rf bin

setup:
	mkdir -p bin

s4: setup
	CGO_ENABLED=0 go build -ldflags='-s -w' -tags 'netgo osusergo' -o bin/s4 cmd/s4/main.go

s4-server: setup
	CGO_ENABLED=0 go build -ldflags='-s -w' -tags 'netgo osusergo' -o bin/s4-server cmd/s4_server/main.go

check: check-deps check-static check-ineff check-err check-vet check-lint check-bodyclose check-nargs check-fmt check-hasdefault check-hasdefer

check-deps:
	@which staticcheck >/dev/null   || (cd ~ && go install honnef.co/go/tools/cmd/staticcheck@latest)
	@which golint      >/dev/null   || (cd ~ && go install golang.org/x/lint/golint@latest)
	@which ineffassign >/dev/null   || (cd ~ && go install github.com/gordonklaus/ineffassign@latest)
	@which errcheck    >/dev/null   || (cd ~ && go install github.com/kisielk/errcheck@latest)
	@which bodyclose   >/dev/null   || (cd ~ && go install github.com/timakin/bodyclose@latest)
	@which nargs       >/dev/null   || (cd ~ && go install github.com/alexkohler/nargs/cmd/nargs@latest)
	@which go-hasdefault >/dev/null || (cd ~ && go install github.com/nathants/go-hasdefault@latest)
	@which go-hasdefer >/dev/null   || (cd ~ && go install github.com/nathants/go-hasdefer@latest)

check-hasdefer: check-deps
	@go-hasdefer $(shell find -type f -name "*.go") || true

check-hasdefault: check-deps
	@go-hasdefault $(shell find -type f -name "*.go") || true

check-fmt: check-deps
	@go fmt ./... >/dev/null

check-nargs: check-deps
	@nargs ./...

check-bodyclose: check-deps
	@go vet -vettool=$(shell which bodyclose) ./...

check-lint: check-deps
	@golint ./... | grep -v unexported || true

check-static: check-deps
	@staticcheck ./...

check-ineff: check-deps
	@ineffassign ./...

check-err: check-deps
	@errcheck ./...

check-vet: check-deps
	@go vet ./...

test: test-lib tox

tox:
	tox

test-lib:
	go test -v lib/lib.go lib/lib_test.go
