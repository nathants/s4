#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define ASSERT(cond, ...) if (!(cond)) { fprintf(stderr, ##__VA_ARGS__); exit(1); }
#define TIMEOUT_SECONDS 5
#define BUFFER_SIZE 1024 * 1024 * 5

int main(int argc, char *argv[]) {
    ASSERT(argc == 3,                                  "usage: cat data | send ADDR PORT");
    ASSERT(argc > 1 && strcmp(argv[1], "-h") != 0,     "usage: cat data | send ADDR PORT");
    ASSERT(argc > 1 && strcmp(argv[1], "--help") != 0, "usage: cat data | send ADDR PORT");

    // setup buffer
    char *buffer = malloc(BUFFER_SIZE);
    ASSERT(buffer != NULL, "fatal: failed to allocate memory\n");

    // setup sock
	struct sockaddr_in addr = {0};
    int32_t sock = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT(sock >= 0, "fatal: socket errno: %d\n", errno);
    int32_t buffsize = BUFFER_SIZE;
    ASSERT(0 == setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buffsize, sizeof(buffsize)), "fatal: setsockopt\n");
	addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(argv[1]);
	addr.sin_port = htons(atoi(argv[2]));

    // retry connect, waiting TIMEOUT_SECONDS for server to come up
    int32_t count = 0;
    while (1) {
        if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) >= 0)
            break;
        usleep(10000);
        ASSERT(++count < TIMEOUT_SECONDS * 100, "fatal: connect timeout errno: %d\n", errno);
    }

    // copy from stdin to socket
    int32_t n;
    while (1) {
        // read from stdin
        n = fread_unlocked(buffer, 1, BUFFER_SIZE, stdin);
        // write size of bytes to socket
        ASSERT(sizeof(int32_t) == write(sock, &n, sizeof(int32_t)), "fatal: write errno: %d\n", errno);
        // write bytes to socket
        ASSERT(n == write(sock, buffer, n), "fatal: write errno: %d\n", errno);
        if (n != BUFFER_SIZE) {
            ASSERT(!ferror_unlocked(stdin), "error: couldnt read input\n");
            break;
        }
    }
    ASSERT(0 == close(sock), "fatal: close errno: %d\n", errno);

}
