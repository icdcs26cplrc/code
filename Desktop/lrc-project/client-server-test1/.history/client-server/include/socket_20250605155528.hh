#ifndef _SOCKET_H_H_H_
#define _SOCKET_H_H_H_

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <netdb.h>
#include <fcntl.h>
#include <string>
#include <iostream>
#include <thread>

#define BLOCK_SIZE 0

// #define CLIENT_PORT 12345
// #define COORDINATOR_PORT 12378
// #define PROXY_PORT_BASE 23456 
// #define DATANODE_PORT_BASE 34567
// #define PROXY_PORT 34589
// #define PROXY_PORT_DATA 35645
// #define CLIENT_IP "127.0.0.1"
// #define COORDINATOR_IP "127.0.0.1"
// #define PROXY_IP  "127.0.0.1"
// #define DATANODE_IP_0 "127.0.0.1"
// #define DATANODE_IP_1 "127.0.0.1"
// #define DATANODE_IP_2 "127.0.0.1"
// #define DATANODE_IP_3 "127.0.0.1"
// #define DATANODE_IP_4 "127.0.0.1"
// #define DATANODE_IP_5 "127.0.0.1"
// #define DATANODE_IP_6 "127.0.0.1"
// #define DATANODE_IP_7 "127.0.0.1"
// #define DATANODE_IP_8 "127.0.0.1"
// #define DATANODE_IP_9 "127.0.0.1"
// #define DATANODE_IP_10 "127.0.0.1"
// #define DATANODE_IP_11 "127.0.0.1"
// #define DATANODE_IP_12 "127.0.0.1"
// #define DATANODE_IP_13 "127.0.0.1"
// #define DATANODE_IP_14 "127.0.0.1"
// #define DATANODE_IP_15 "127.0.0.1"

using namespace std;

class Socket{
  private:
    //char* denormalizeIP(const char* dest_ip);
    int initClient(void);
    int initServer(int port_num);
    void recvData(int connfd, char* buff, size_t chunk_size, size_t packet_size);//, int index, int* mark_recv);
    static std::mutex output_mutex_;  
    public:
    Socket();
    ~Socket();
      // send data
    void sendData(const char* buf, size_t chunk_size, size_t packet_size, const char* des_ip, int des_port_num);
      // receive data in parallel
    void paraRecvData(int server_port_num, char* total_recv_data, size_t chunk_size, size_t packet_size);//, int num_conn, int* mark_recv, int flag, char** source_IPs);
      // receive command
    size_t recvCmd(int server_port_num, size_t buf_size, char* buf);
    bool  SendDataReliably(int sock_fd, const void* data, size_t data_size) ;
    bool ReceiveDataReliably(int sock_fd, void* buffer, size_t data_size) ;
};

#endif