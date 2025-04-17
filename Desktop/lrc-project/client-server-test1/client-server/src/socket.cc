// socket.cc
#include "../include/socket.hh"
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

int Socket::InitClient() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    return fd;
}

int Socket::InitServer(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    
    bind(fd, (sockaddr*)&addr, sizeof(addr));
    listen(fd, 100);
    return fd;
}

ssize_t Socket::SendData(int fd, const char* buf, size_t len) {
    size_t sent = 0;
    while(sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, 0);
        if(n <= 0) return -1;
        sent += n;
    }
    return sent;
}

ssize_t Socket::RecvData(int fd, char* buf, size_t len) {
    size_t received = 0;
    while(received < len) {
        ssize_t n = recv(fd, buf + received, len - received, 0);
        if(n <= 0) return -1;
        received += n;
    }
    return received;
}

int Socket::Connect(int fd, const std::string& ip, int port) {
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);
    return connect(fd, (sockaddr*)&addr, sizeof(addr));
}

void Socket::Close(int fd) {
    close(fd);
}