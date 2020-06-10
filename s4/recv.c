#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define ASSERT(cond, ...) if (!(cond)) { fprintf(stderr, ##__VA_ARGS__); exit(1); }
#define BUFFER_SIZE 1024 * 1024 * 5

int main(int argc, char *argv[]) {
    ASSERT(argc == 2,                                  "usage: recv PORT > data");
    ASSERT(argc > 1 && strcmp(argv[1], "-h") != 0,     "usage: recv PORT > data");
    ASSERT(argc > 1 && strcmp(argv[1], "--help") != 0, "usage: recv PORT > data");

    // setup buffer
    char *buffer = malloc(BUFFER_SIZE);
    ASSERT(buffer != NULL, "fatal: failed to allocate memory\n");

    // setup sock
	struct sockaddr_in addr = {0};
	int32_t sock = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT(sock >= 0, "fatal: socket errno: %d\n", errno);
    int32_t buffsize = BUFFER_SIZE;
    ASSERT(0 == setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &buffsize, sizeof(buffsize)), "fatal: setsockopt\n");
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);
	addr.sin_port = htons(atoi(argv[1]));

    // listen on port
	ASSERT(bind(sock, (struct sockaddr*)&addr, sizeof(addr)) >= 0, "fatal: bind errno %d\n", errno);
	ASSERT(listen(sock, 1) >= 0, "fatal: listen errno: %d\n", errno);
    int32_t conn = accept(sock, NULL, NULL);

    // copy from socket to stdout
    int32_t size;
    while(1) {
        // read size of bytes from socket
        switch (read(conn, &size, sizeof(int32_t))) {
            // done
            case 0:
                ASSERT(0 == close(conn), "fatal: close errno: %d\n", errno);
                return 0;
            // keep reading
            case sizeof(int32_t):
                ASSERT(size == read(conn, buffer, size), "fatal: bad data read\n");
                ASSERT(size == fwrite_unlocked(buffer, 1, size, stdout), "fatal: bad write\n");
                ASSERT(0 == fflush_unlocked(stdout), "fatal: failed to flush\n"); \
                break;
            // failure
            default:
                ASSERT(0, "fatal: bad size read\n");
        }
    }

}
