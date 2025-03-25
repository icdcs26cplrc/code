#ifndef SOCKET_HH
#define SOCKET_HH

#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

class Socket {
public:
    Socket();
    ~Socket();

    bool connect(const std::string& address, int port);
    bool send(const std::string& data);
    std::string receive(size_t size);
    void close();

private:
    int sockfd;
    struct sockaddr_in server_addr;
};

#endif // SOCKET_HH