// client.cpp
#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "../include/client.hh"

using namespace std;

using namespace ClientServer;

Client::Client()
{
    // Constructor implementation
}

void Client::StartListen()
{

    int opt;
    int ret;
    struct sockaddr_in my_addr;
    bzero(&my_addr, sizeof(my_addr));
    my_addr.sin_family = AF_INET;
    my_addr.sin_addr.s_addr = inet_addr(CLIENT_IP);
    my_addr.sin_port = htons(CLIENT_PORT);

    if ((client_socket_connect = socket(PF_INET, SOCK_STREAM, 0)) < 0)
    {
        perror("create server socket error!");
    }

    opt = 1;
    if ((ret = setsockopt(client_socket_connect, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt))) != 0)
    {
        perror("set server socket error!");
    }

    if ((::bind(client_socket_connect, (struct sockaddr *)&my_addr, sizeof(my_addr))) != 0)
    {
        perror("server socket bind error!");
    }
}

size_t Client::RecvACK(string des,char *&buf)
{
    // remote address
    struct sockaddr_in remote_addr;
    if (des == "proxy")
    {
        remote_addr.sin_addr.s_addr = inet_addr(PROXY_IP);
        //remote_addr.sin_port = htons(PROXY_PORT);
    }else if (des == "cor")
    {
        remote_addr.sin_addr.s_addr = inet_addr(COORDINATOR_IP);
        //remote_addr.sin_port = htons(COORDINATOR_PORT);
    }
    
    socklen_t length = sizeof(remote_addr);

    // listen
    if (listen(client_socket_connect, 100) == -1)
    {
        perror("server listen fail!");
    }
    while (true)
    {
        int client_fd = accept(client_socket_connect,(struct sockaddr*)&remote_addr, &length);
        if (client_fd < 0)
        {
            if (errno == EINTR)
                continue;
            throw std::runtime_error("accept() failed");
        }
        buf = new char[100];
        ssize_t received = read(client_fd, buf, sizeof(buf));
        if (received <= 0)
        {
            std::cerr << "recv() failed" << std::endl;
            break;
        }
        close(client_fd);
        return received;
    }
}

bool Client::FileToKeyValue(std::string file_path, std::string &key, std::string &value)
{
    // 打开文件
    ifstream file(file_path);
    if (!file.is_open())
    {
        cerr << "Error: Unable to open file " << file_path << endl;
        return false;
    }

    // 读取文件内容并生成key-value对
    string content((istreambuf_iterator<char>(file)), istreambuf_iterator<char>());
    file.close();

    // 简单生成key-value对，key为文件名，value为文件内容
    size_t lastSlash = file_path.find_last_of("/\\");
    key = (lastSlash == string::npos) ? file_path : file_path.substr(lastSlash + 1);
    value = content;
    return true;
}

bool Client::set(std::string key, std::string value)
{
    // send set request to coordinator

    int sock = InitClient();

    // Define the server address
    struct sockaddr_in remote_addr;
  bzero(&remote_addr, sizeof(remote_addr));
  remote_addr.sin_family = AF_INET;
  remote_addr.sin_port = htons(COORDINATOR_PORT);
  //char* denormalized_ip = denormalizeIP(des_ip);
  const char* denormalized_ip = COORDINATOR_IP;
  if(inet_aton(denormalized_ip, &remote_addr.sin_addr) == 0){
    cout<<"dest ip: "<<denormalized_ip<<endl;
    perror("inet_aton fail!");
  }

    // Connect to the server
    if (connect(sock, (struct sockaddr *)&remote_addr, sizeof(remote_addr)) < 0)
    {
        cerr << "Error: Connection failed" << endl;
        close(sock);
        return false;
    }

    // Prepare the message
    // std::string message = "SET " + key + " " + value + "\n";
    string message = "SET " + key + " valuelen_" + to_string(value.size()) + "\n";
    const char *message_cstr = message.c_str();

    // Send the message
    ssize_t bytes_sent = write(sock, message_cstr, message.size());
    //cout << "send bytes: " << bytes_sent << endl;
    if (bytes_sent < 0)
    {
        cerr << "Error: Failed to send data" << endl;
        close(sock);
        return false;
    }
    // Close the socket
    
    // wait for proxy to send the data
    char *ack;
    size_t recv = RecvACK("proxy",ack);
    if (string(ack) == "SET_ACK")
    {
        //cout << "ACK from proxy: " << ack << endl;
        SendValueToProxy(value);
    }
    close(sock);
    return true;
}

void Client::SendValueToProxy(string value)
{
    // client-side socket
    //cout<<"value size : "<<value.size()<<endl;
    int client_socket = InitClient();

    size_t ret;

    // set server-side sockaddr_in
    struct sockaddr_in remote_addr;
    bzero(&remote_addr, sizeof(remote_addr));
    remote_addr.sin_family = AF_INET;
    remote_addr.sin_port = htons(PROXY_PORT_DATA);
    // char* denormalized_ip = denormalizeIP(des_ip);
    const char *denormalized_ip = PROXY_IP;
    if (inet_aton(denormalized_ip, &remote_addr.sin_addr) == 0)
    {
        //cout << "dest ip: " << denormalized_ip << endl;
        perror("inet_aton fail!");
    }
    // connect, connect server
    // 建立连接 - 添加重试机制和错误处理
    int retry_count = 0;
    const int max_retries = 3;
    while (retry_count < max_retries) {
        if (connect(client_socket, (struct sockaddr *)&remote_addr, sizeof(remote_addr)) == 0) {
            break;  // 连接成功
        }
        retry_count++;
        if (retry_count >= max_retries) {
            perror("Failed to connect after retries");
            close(client_socket);
            return;
        }
        usleep(100000);  // 等待100ms后重试
    }
    size_t chunk_size = value.size();
    // 发送长度头 - 转换为网络字节序
    uint32_t data_length_net = htonl(chunk_size);
    if (write(client_socket, &data_length_net, sizeof(data_length_net)) != sizeof(data_length_net)) {
        cerr << "Failed to send data length header" << endl;
        close(client_socket);
        return;
    }
    
    cout << "Sending " << chunk_size << " bytes to proxy" << endl;
    
    // send data
    
    
    size_t packet_size = chunk_size; // 1MB
    size_t sent_len = 0;
    while (sent_len < chunk_size)
    {
        //cout << "send_len before write: " << sent_len << endl;
        ret = write(client_socket, value.c_str() + sent_len, packet_size);
        if (ret <=0)
        {
            cerr << "Error: Failed to send data" << endl;
            close(client_socket);
            return;
        }
        sent_len += ret;
        //cout<< "send_len after write: " << sent_len << endl;
    }
    //cout << "send_len : " << sent_len << endl;
    // close client-side socket
    if ((close(client_socket)) == -1)
    {
        cout << "close client_socket error!" << endl;
        exit(1);
    }
    cout << "Data sent to proxy successfully" << endl;
    // 发送完数据后，关闭连接
    close(client_socket);
    // cout << "send value to proxy" << endl;
}

bool Client::get(std::string key, std::string &value)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
    {
        cerr << "Error: Unable to create socket" << endl;
        return false;
    }

    // Define the server address
    struct sockaddr_in remote_addr;
  bzero(&remote_addr, sizeof(remote_addr));
  remote_addr.sin_family = AF_INET;
  remote_addr.sin_port = htons(COORDINATOR_PORT);
  //char* denormalized_ip = denormalizeIP(des_ip);
  const char* denormalized_ip = COORDINATOR_IP;
  if(inet_aton(denormalized_ip, &remote_addr.sin_addr) == 0){
    cout<<"dest ip: "<<denormalized_ip<<endl;
    perror("inet_aton fail!");
  }

    // Connect to the server
    if (connect(sock, (struct sockaddr *)&remote_addr, sizeof(remote_addr)) < 0)
    {
        cerr << "Error: Connection failed" << endl;
        close(sock);
        return false;
    }

    // Prepare the message
    std::string message = "GET " + key + "\n";
    const char *message_cstr = message.c_str();

    // Send the message
    ssize_t bytes_sent = write(sock, message_cstr, message.size());
    //cout << "send bytes: " << bytes_sent << endl;
    if (bytes_sent < 0)
    {
        cerr << "Error: Failed to send data" << endl;
        close(sock);
        return false;
    }
    close(sock);

    // 等待proxy发送的信息
    if (listen(client_socket_connect, 100) < 0)
    {
        close(client_socket_connect);
        throw std::runtime_error("listen failed: " + std::string(strerror(errno)));
    };
    struct sockaddr_in remote_addr_p;
    
    remote_addr_p.sin_addr.s_addr = inet_addr(PROXY_IP);
    //remote_addr.sin_port = htons(PROXY_PORT);
    
    socklen_t length = sizeof(remote_addr);
    int client_sock = accept(client_socket_connect, (struct sockaddr*)&remote_addr_p, &length);
    if (client_sock < 0)
    {
        perror("accept failed");
        // Handle error
    }
    else
    {
         // 接收文件长度头
       // 设置socket超时
       struct timeval timeout;
       timeout.tv_sec = 10;  // 10秒超时
       timeout.tv_usec = 0;
       setsockopt(client_sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
       
       // 接收文件长度头
       uint32_t data_length_net;
       ssize_t header_received = recv(client_sock, &data_length_net, sizeof(data_length_net), MSG_WAITALL);
       
       if (header_received != sizeof(data_length_net)) {
           cerr << "Failed to receive data length header, received: " << header_received << endl;
           close(client_sock);
           return false;
       }
       
       size_t expected_size = ntohl(data_length_net);  // 转换回主机字节序
       //cout << "Expected to receive: " << expected_size << " bytes" << endl;
       
       // 验证数据长度合理性
       if (expected_size == 0 || expected_size > 1024 * 1024 * 100) {  // 限制最大100MB
           cerr << "Invalid data size: " << expected_size << endl;
           close(client_sock);
           return false;
       }
    
    //  获取实际value数据
        //size_t block_size = 1024 * 1024;
        char *buffer = new char[expected_size+1];
        memset(buffer, 0, expected_size + 1);
        std::string received_data;
        ssize_t total_received = 0;
        ssize_t remaining = expected_size;
        while (total_received < expected_size)
        {
            ssize_t received = read(client_sock, buffer+total_received, remaining);
            if (received == 0)
            {
                std::cerr << " received " << total_received << "/" << expected_size << " bytes)" << std::endl;
                break;
            }
            else if (received < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    // 非阻塞模式下无数据，可重试（需确保套接字是非阻塞的）
                    continue;
                }
                std::cerr << "read() error: " << strerror(errno) << std::endl;
                break;
            }
            total_received += received;
            remaining -= received;
            received_data.append(buffer, received);
        }
        cout<<"GET command received :"<<total_received<<endl;
    }
    close(client_sock);
    
    // Close the socket

    return true;
}

bool Client::repair(string failed_node_str)
{
    // cout << "repair  : " << failed_node_str<<endl;
    //  解析失败节点
    //  Split the failed_node_str by spaces
    std::istringstream iss(failed_node_str);
    std::string token;
    std::map<std::string, int> block_count;

    // Count occurrences of each block type
    while (iss >> token)
    {
        block_count[token]++;
    }

    // Print the total count of all blocks
    int total_count = 0;
    for (const auto &entry : block_count)
    {
        total_count += entry.second;
    }

    // cout << "Total count of blocks: " << total_count << endl;
    string fail_num = "fail_number_" + to_string(total_count);
    string value ="REPAIR client "+ fail_num + " " + failed_node_str;
    // cout<<"massage : "<<value<<endl;
    SendRequestToCoordinator(value);
}

void Client::SendRequestToCoordinator(string value)
{
    // client-side socket
    int client_socket = InitClient();

    size_t ret;

    // set server-side sockaddr_in
    struct sockaddr_in remote_addr;
    bzero(&remote_addr, sizeof(remote_addr));
    remote_addr.sin_family = AF_INET;
    remote_addr.sin_port = htons(COORDINATOR_PORT);
    // char* denormalized_ip = denormalizeIP(des_ip);
    const char *denormalized_ip = COORDINATOR_IP;
    if (inet_aton(denormalized_ip, &remote_addr.sin_addr) == 0)
    {
        cout << "dest ip: " << denormalized_ip << endl;
        perror("inet_aton fail!");
    }

    // connect, connect server
    if (connect(client_socket, (struct sockaddr *)&remote_addr, sizeof(remote_addr)) < 0)
    {
        cerr << "Error: Connection failed" << endl;
        close(client_socket);
        return;
    }
    // send data
    std::string buf = value + "\n";
    const char *buf_cstr = buf.c_str();
    // cout<<"massage : "<<buf<<endl;
    size_t chunk_size = buf.size();
    size_t send_len = write(client_socket, buf_cstr, chunk_size);
    //cout << "send bytes: " << send_len << endl;
    if (send_len < 0)
    {
        cerr << "Error: Failed to send data" << endl;
        close(client_socket);
        return;
    }
    // close client-side socket
    if ((close(client_socket)) == -1)
    {
        cout << "close client_socket error!" << endl;
        exit(1);
    }
    cout << "finish send data !" << endl;
    close(client_socket);
}

int Client::InitClient()
{
    int client_socket;
    int opt;
    int ret;
    if ((client_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        perror("create client socket error!");
    }
    opt = 1;
    if ((ret = setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt))) != 0)
    {
        perror("set client socket error!");
    }
    return client_socket;
}

int main()
{
    Client client;
    client.StartListen();
    // 输入请求
    cout << "Please input the command : " << " set, get, repair "
         << " for example: set file_path " << " get file " << " repair (stripe_0_block_0) \n"
         << endl;
    string command;
    // 解析command
    getline(cin, command);
    std::string key;
    std::string value;
    if (command.find("set") == 0)
    {
        string file_path = command.substr(4);
        if (client.FileToKeyValue(file_path, key, value) == true)
        {
            if(client.set(key, value) == false){
                cout << "SET : "<<key<<" fail!!!!"<<endl;
            }
        }
        else
        {
            cout << "FileToKeyValue error" << endl;
        }
    }
    else if (command.find("get") == 0)
    {
        string file_name = command.substr(4);
        string value;
        if(client.get(file_name, value) == false){
            cout << "GET : "<<key<<" fail!!!!"<<endl;
        }
        
    }
    else if (command.find("repair") == 0)
    {
        string failed_node_str = command.substr(7);
        // 解析失败节点列表
        client.repair(failed_node_str);
    }
    else
    {
        cout << "Invalid command" << endl;
    }
}