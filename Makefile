.PHONY: all clean test s4 s4-server s4-send s4-recv

all: s4 s4-server s4-send s4-recv

clean:
	rm -f s4 s4-server s4-send s4-recv

s4:
	go build -o s4 s4.go

s4-server:
	go build -o s4-server s4_server.go

s4-send:
	go build -o s4-send s4_send.go

s4-recv:
	go build -o s4-recv s4_recv.go
