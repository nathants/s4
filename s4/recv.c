#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
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

void alarm_handler(int sig) {
    ASSERT(0, "fatal: timeout\n");
}

i32 main(i32 argc, char *argv[]) {
    // show usage and exit
    ASSERT(argc == 2 && argc > 1 && strcmp(argv[1], "-h") != 0 && strcmp(argv[1], "--help") != 0, "usage: recv PORT > data\n");

    // setup timeout
    signal(SIGALRM, alarm_handler);
    alarm(TIMEOUT_SECONDS);

    // setup buffer
    char *buffer = malloc(BUFFER_SIZE);
    ASSERT(buffer != NULL, "fatal: failed to allocate memory\n");

    // setup sock
    i32 sock = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT(sock >= 0, "fatal: socket errno: %d\n", errno);
    i32 buffsize = BUFFER_SIZE;
    ASSERT(0 == setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &buffsize, sizeof(buffsize)), "fatal: setsockopt bufsize\n");
    i32 enable = 1;
    ASSERT(0 == setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)), "fatal: setsockopt reuse\n");

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(atoi(argv[1]));

    // bind, listen and accept

    // retry bind, alaram() handles timeout
    while (1) {
        if (bind(sock, &addr, sizeof(addr)) >= 0)
            break;
        usleep(10000);
    }

    ASSERT(listen(sock, 1) >= 0, "fatal: listen errno: %d\n", errno);
    i32 conn = accept(sock, NULL, NULL);

    // copy from socket to stdout
    FILE *stream = fdopen(conn, "rb");
    setvbuf(stream, NULL, _IONBF, 0);
    i32 size;
    while(1) {
        // set timeout
        alarm(TIMEOUT_SECONDS);
        // read size of bytes from socket
        switch (fread_unlocked(&size, 1, sizeof(i32), stream)) {
            // close and exit
            case 0:
                ASSERT(0 == fclose(stream), "fatal: close stream errno: %d\n", errno);
                return 0;
            // keep reading
            case sizeof(i32):
                ASSERT(size == fread_unlocked(buffer, 1, size, stream), "fatal: bad data read\n");
                ASSERT(size == fwrite_unlocked(buffer, 1, size, stdout), "fatal: bad write\n");
                ASSERT(0 == fflush_unlocked(stdout), "fatal: failed to flush\n");
                break;
            // failure
            default:
                ASSERT(0, "fatal: bad size read\n");
        }
    }
}
