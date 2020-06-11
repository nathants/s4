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
typedef int32_t i32;
typedef uint8_t u8;

i32 main(i32 argc, char *argv[]) {
    // show usage and exit
    ASSERT(argc == 3 && argc > 1 && strcmp(argv[1], "-h") != 0 && strcmp(argv[1], "--help") != 0, "usage: cat data | send ADDR PORT\n");

    // setup buffer
    u8 *buffer = malloc(BUFFER_SIZE);
    ASSERT(buffer != NULL, "fatal: failed to allocate memory\n");

    // setup sock
    i32 sock = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT(sock >= 0, "fatal: socket errno: %d\n", errno);
    i32 buffsize = BUFFER_SIZE;
    ASSERT(0 == setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buffsize, sizeof(buffsize)), "fatal: setsockopt\n");
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(argv[1]);
    addr.sin_port = htons(atoi(argv[2]));

    // retry connect, waiting TIMEOUT_SECONDS for server to come up
    i32 count = 0;
    while (1) {
        if (connect(sock, &addr, sizeof(addr)) >= 0)
            break;
        usleep(10000);
        ASSERT(++count < TIMEOUT_SECONDS * 100, "fatal: connect timeout errno: %d\n", errno);
    }

    // copy from stdin to socket
    FILE *stream = fdopen(sock, "wb");
    setvbuf(stream, NULL, _IONBF, 0);
    i32 size;
    while (1) {
        // read from stdin
        size = fread_unlocked(buffer, 1, BUFFER_SIZE, stdin);
        if (size > 0) {
            // write size of bytes to socket
            ASSERT(sizeof(i32) == fwrite_unlocked(&size, 1, sizeof(i32), stream), "fatal: write size errno: %d\n", errno);
            // write bytes to socket
            ASSERT(size == fwrite_unlocked(buffer, 1, size, stream), "fatal: write bytes errno: %d\n", errno);
        }
        // close and exit
        if (size != BUFFER_SIZE) {
            ASSERT(!ferror(stdin), "error: couldnt read input\n");
            ASSERT(0 == fclose(stream), "fatal: close stream errno: %d\n", errno);
            return 0;
        }
    }
}
