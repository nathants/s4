FROM archlinux:latest

RUN pacman -Sy --noconfirm \
    make \
    go

ENV GO111MODULE=on

RUN go get github.com/nathants/s4/cmd/s4 && \
    go get github.com/nathants/s4/cmd/s4_server && \
    mv -f $(go env GOPATH)/bin/s4 /usr/local/bin/s4 && \
    mv -f $(go env GOPATH)/bin/s4_server /usr/local/bin/s4-server
