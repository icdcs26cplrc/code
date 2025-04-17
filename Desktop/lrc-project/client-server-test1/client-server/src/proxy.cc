// proxy.cc
#include "../include/proxy.hh"
#include <cstring>
#include <netinet/in.h>
#include <iostream>

Proxy::Proxy(int ctrl_port, int data_port) 
    : ctrl_port_(ctrl_port), data_port_(data_port) {}

void Proxy::Start() {
    // 控制端口监听
    std::thread([this]{
        int fd = socket_.InitServer(ctrl_port_);
        while(true) {
            sockaddr_in client_addr;
            socklen_t len = sizeof(client_addr);
            int client_fd = accept(fd, (sockaddr*)&client_addr, &len);
            std::thread(&Proxy::HandleControl, this, client_fd).detach();
        }
    }).detach();

    // 数据端口监听
    int data_fd = socket_.InitServer(data_port_);
    while(true) {
        sockaddr_in client_addr;
        socklen_t len = sizeof(client_addr);
        int client_fd = accept(data_fd, (sockaddr*)&client_addr, &len);
        std::thread(&Proxy::HandleData, this, client_fd).detach();
    }
}

void Proxy::HandleControl(int fd) {
    char buf[1024];
    ssize_t len = socket_.RecvData(fd, buf, sizeof(buf));
    
    // 解析节点注册请求
    if(strncmp(buf, "REGISTER", 8) == 0) {
        std::string node_id(buf+8, 16);
        std::string addr(buf+24, len-24);
        data_nodes_[node_id] = addr;
    }
    
    socket_.Close(fd);
}

void Proxy::HandleData(int fd) {
    // 接收数据头
    struct Header {
        uint32_t magic;
        uint64_t data_size;
    } header;
    
    socket_.RecvData(fd, reinterpret_cast<char*>(&header), sizeof(header));
    
    // 验证协议头
    if(header.magic != 0xDEADBEEF) {
        socket_.Close(fd);
        return;
    }
    
    // 接收实际数据
    char* data = new char[header.data_size];
    socket_.RecvData(fd, data, header.data_size);
    
    // 路由到数据节点
    RouteToDataNode(std::string(data, header.data_size));
    
    delete[] data;
    socket_.Close(fd);
}

void Proxy::RouteToDataNode(const std::string& data) {
    // 简单轮询选择数据节点
    static auto it = data_nodes_.begin();
    if(it == data_nodes_.end()) it = data_nodes_.begin();
    
    std::string addr = it->second;
    size_t colon = addr.find(':');
    std::string ip = addr.substr(0, colon);
    int port = std::stoi(addr.substr(colon+1));
    
    // 转发数据
    int fd = socket_.InitClient();
    socket_.Connect(fd, ip, port);
    socket_.SendData(fd, data.c_str(), data.size());
    socket_.Close(fd);
    
    ++it;
}

