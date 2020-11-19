.PHONY: all clean test s4 s4-server check check-static check-ineff check-err check-vet test-lib check-bodyclose check-nargs check-fmt check-shadow

all: s4 s4-server

clean:
	rm -rf bin

setup:
	mkdir -p bin

s4: setup
	go build -o bin/s4 cmd/s4/main.go

s4-server: setup
	go build -o bin/s4-server cmd/s4_server/main.go

check: check-deps check-static check-ineff check-err check-vet check-lint check-bodyclose check-nargs check-fmt check-shadow

check-deps:
	@which staticcheck >/dev/null || (cd ~ && go get -u github.com/dominikh/go-tools/cmd/staticcheck)
	@which golint      >/dev/null || (cd ~ && go get -u golang.org/x/lint/golint)
	@which ineffassign >/dev/null || (cd ~ && go get -u github.com/gordonklaus/ineffassign)
	@which errcheck    >/dev/null || (cd ~ && go get -u github.com/kisielk/errcheck)
	@which bodyclose   >/dev/null || (cd ~ && go get -u github.com/timakin/bodyclose)
	@which nargs       >/dev/null || (cd ~ && go get -u github.com/alexkohler/nargs/cmd/nargs)
	@which shadow      >/dev/null || (cd ~ && go get -u golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow)

check-shadow: check-deps
	@go vet -vettool=$(shell which shadow) ./...

check-fmt: check-deps
	@go fmt ./...

check-nargs: check-deps
	@nargs ./...

check-bodyclose: check-deps
	@go vet -vettool=$(shell which bodyclose) ./...

check-lint: check-deps
	@golint ./... | grep -v unexported || true

check-static: check-deps
	@staticcheck ./...

check-ineff: check-deps
	@ineffassign ./*

check-err: check-deps
	@errcheck ./...

check-vet: check-deps
	@go vet ./...

test: test-lib tox

tox:
	tox

test-lib:
	go test -v lib/lib.go lib/lib_test.go
