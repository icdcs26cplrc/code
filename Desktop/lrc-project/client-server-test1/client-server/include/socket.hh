// socket.hh
#ifndef SOCKET_HH
#define SOCKET_HH

#include <sys/socket.h>
#include <string>

class Socket {
public:
    // 基础功能
    int InitClient();
    int InitServer(int port);
    int InitCoordinator(int port);
    
    // 数据传输
    ssize_t SendData(int fd, const char* buf, size_t len);
    ssize_t RecvData(int fd, char* buf, size_t len);
    
    // 连接管理
    int Connect(int fd, const std::string& ip, int port);
    void Close(int fd);

private:
    char* DenormalizeIP(const char* ip);
};

#endif // SOCKET_HH