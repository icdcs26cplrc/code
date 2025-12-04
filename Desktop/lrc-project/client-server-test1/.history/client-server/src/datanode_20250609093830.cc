
#include "../include/datanode.hh"


using namespace ClientServer;

std::mutex ClientServer::DataNode::io_mutex_;  
DataNode::DataNode()
{
    // 初始化数据节点
    listen_fd_data = -1;
}

void DataNode::Start(int port)
{

    // 创建套接字
    listen_fd_data = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_data < 0)
        throw std::runtime_error("socket() failed");

    // 绑定指定端口
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    const char *des_ip = "127.0.0.1";
    const char* datanode_ips[] = {
        DATANODE_IP_0, DATANODE_IP_1, DATANODE_IP_2, DATANODE_IP_3,
        DATANODE_IP_4, DATANODE_IP_5, DATANODE_IP_6, DATANODE_IP_7,
        DATANODE_IP_8, DATANODE_IP_9, DATANODE_IP_10, DATANODE_IP_11,
        DATANODE_IP_12, DATANODE_IP_13, DATANODE_IP_14
    };

    if ((port - DATANODE_PORT_BASE) >= 0 && (port - DATANODE_PORT_BASE) < sizeof(datanode_ips) / sizeof(datanode_ips[0])) {
        addr.sin_addr.s_addr =inet_addr(datanode_ips[(port - DATANODE_PORT_BASE)]) ;
    } else {
        std::cerr << "Invalid node_id: " << (port - DATANODE_PORT_BASE) << std::endl;
        return;
    }
    //inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr); // 修复IP地址

    if (::bind(listen_fd_data, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        close(listen_fd_data);
        throw std::runtime_error("bind failed: " + std::string(strerror(errno)));
    }

    if (listen(listen_fd_data, 100) < 0)
    {
        close(listen_fd_data);
        throw std::runtime_error("listen failed");
    }

    // 启动独立线程运行监听循环
    // server_thread_ = std::thread([this, port]
    //                              { RunServer_data(port); });
     // 定义一个互斥锁

    
       
        RunServer_data(port);
    
}

void DataNode::RunServer_data(int port)
{
    while (true)
    {
        // sockaddr_in client_addr{};
        // socklen_t addr_len = sizeof(client_addr);
        //std::lock_guard<std::mutex> lock(mtx); // 使用锁保护访问
        int data_fd = accept(listen_fd_data, nullptr, nullptr);

        if (data_fd < 0)
        {
            cout << "datanode accept failed" << endl;
            break; // 发生严重错误时退出
        }

        //为新连接创建处理线程
            // std::thread([this, data_fd, port]
            //             { HandleConnection_data(data_fd); })
            //     .detach();
        
        HandleConnection_data(data_fd);
    }
}

void DataNode::HandleConnection_data(int client_fd)
{
        // 接收文件长度头
       // 设置socket超时
       struct timeval timeout;
       timeout.tv_sec = 10;  // 10秒超时
       timeout.tv_usec = 0;
       setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
       
       // 接收文件长度头
       uint32_t data_length_net;
       ssize_t header_received = recv(client_fd, &data_length_net, sizeof(data_length_net), MSG_WAITALL);
       
       if (header_received != sizeof(data_length_net)) {
           cerr << "Failed to receive data length header, received: " << header_received << endl;
           close(client_fd);
           return;
       }
       
       size_t expected_size = ntohl(data_length_net);  // 转换回主机字节序
       //cout << "Expected to receive: " << expected_size << " bytes" << endl;
       
       // 验证数据长度合理性
       if (expected_size == 0 || expected_size > 1024 * 1024 * 100) {  // 限制最大100MB
           cerr << "Invalid data size: " << expected_size << endl;
           close(client_fd);
           return;
       }
    
    //  获取实际value数据
     
    char * cmd_buf = new char[expected_size+1];
    memset(cmd_buf, 0, expected_size + 1);
    
    //ssize_t received = RevData(client_fd, cmd_buf, expected_size);
    //cout << "Received: " << received << " bytes" << endl;
    //ssize_t received = RevData(client_fd, cmd_buf, chunk_size, packet_size);
    //cout<<"received : "<<received<<endl;
    Socket Datanode;
        
    if (Datanode.ReceiveDataReliably(client_fd, cmd_buf, expected_size) == false) {
            cerr << "Failed to receive data from client" << endl;
            delete[] cmd_buf;
            close(client_fd);
            return;
    }
    // 向proxy发送ack
    string ack = "datanode_ack";
    int ack_sent_times = 0;
    const int max_ack_sent_times = 3;
    while (ack_sent_times < max_ack_sent_times) {
        if (write(client_fd, ack.c_str(), ack.size()) < 0) {
            cerr << "Failed to send ACK to client" << endl;
            ack_sent_times++;
            continue; // 重试发送ACK
        }
        break; // 成功发送ACK后退出循环
    }
    
    std::string cmd(cmd_buf, expected_size);  // 使用完整的预期大小
    //std::string cmd(cmd_buf, received);
    //std::cout << "Received command: " << cmd << std::endl;
    //  解析命令
    std::istringstream iss(cmd);
    std::string operation, node_id, value;
    iss >> operation >> node_id;
    // 根据操作类型处理数据
    if (operation == "SET")
    {
        value.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        //std::lock_guard<std::mutex> lock(io_mutex_);
        //lock_guard<std::mutex> log_lock(log_mutex_);
        //cout << "Received SET command: " << value.size() << std::endl;
        //log_lock.unlock(); 
        ProcessSet_data(node_id, value);
        close(client_fd);
        // 发送ACK响应
       
    }
    else if (operation == "GET")
    {
        //cout<<"Received GET command: " << cmd << endl;
        value.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        ProcessGet_data(node_id, value);
        close(client_fd);
    }
    else if (operation == "REPAIR")
    {
        value.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        //std::cout << "Received REPAIR command: " << node_id << std::endl;
        ProcessRepair_data(node_id,value);
        close(client_fd);
    }
    else
    {
        std::cerr << "Unknown operation: " << operation << std::endl;
    }

    close(client_fd);
}
ssize_t DataNode::RevData(int client_fd, char *buf, size_t chunk_size)//, size_t packet_size)
{
    ssize_t total_received = 0;
    total_received = read(client_fd,buf,chunk_size);
    //cout<<"chunk_size : "<<chunk_size<<endl;
    // while (total_received < chunk_size)
    // {
    //     //cout<<"before recv : "<<total_received<<endl;
    //     //std::lock_guard<std::mutex> lock_2(mtx); // 使用锁保护访问
    //     ssize_t received = read(client_fd, buf + total_received, packet_size);
    //     //cout<<"received : "<<received<<endl;
    //     // if (received <= 0)
    //     // {
    //     //     //std::cerr << "recv() failed" << std::endl;
    //     //     break; // 发生错误
    //     // }
    //     if (received == 0) {
    //         // 连接正常关闭，但数据未接收完
    //         //std::cerr << "Connection closed by peer (received " << total_received << "/" << chunk_size << " bytes)" << std::endl;
    //         break;
    //     } else if (received < 0) {
    //         if (errno == EAGAIN || errno == EWOULDBLOCK) {
    //             // 非阻塞模式下无数据，可重试（需确保套接字是非阻塞的）
    //             continue;
    //         }
    //         std::cerr << "read() error: " << strerror(errno) << std::endl;
    //         break;
    //     }
        
    //     total_received += received;
    //     //cout<<"after recv : "<<total_received<<endl;
    // }
    //cout << "datanode total_received : " << total_received << std::endl;
    return total_received;
}

void DataNode::SetNodeList(int node_id_number)
{
    // 创建节点存储结构
    //node_storage_list_.clear();
    // node_storage_list_.resize(node_id_number);
    // for (int i = 0; i < node_id_number; i++)
    // {
    //     NodeStorage node_storage;
    //     node_storage.node_id = "node_id_" + std::to_string(i);
    //     node_storage.value = "";
    //     node_storage_list_[i] = node_storage;
    //     //cout << "node_storage_list_[" << i << "]: " << node_storage.node_id << endl;
    //     //cout << "node_storage_list_[" << i << "]: " << node_storage.value << endl;
    // }
    node_storage_list_.clear();
    node_storage_list_.reserve(node_id_number);  // 预分配空间，避免多次扩容
    for (int i = 0; i < node_id_number; i++) {
        node_storage_list_.emplace_back(        // 直接在容器中构造对象
            std::make_unique<NodeStorage>("node_id_" + std::to_string(i), "")                                 // value
        );
    }
}


const std::vector<std::unique_ptr<DataNode::NodeStorage>>& DataNode::GetNodeList() const {

    std::shared_lock<std::shared_mutex> lock(node_list_mutex_);
    return node_storage_list_;  // 返回引用，避免拷贝
}

void DataNode::ProcessSet_data(std::string node_id, const std::string value)
{
    // 处理SET命令
    //std::cout << "Received SET command: node_id : " << node_id << " value : " << value.size() << std::endl;
    // 存储数据到对应的节点
    //cout<<"node_list_to_set size: "<<node_list_to_set.size()<<endl;
     // 写锁
    // 遍历节点列表，找到对应的节点并存储数据
    for (const auto &node : node_storage_list_)
    {
        //cout<<"node_list_to_set node_id: "<<node.node_id<<endl;
        if (node->node_id == node_id)
        {
            //std::unique_lock<std::shared_mutex> lock(node_list_mutex_); 
            // std::unique_lock<std::shared_mutex> lock(node->mutex);
            // node->value.append(value);
            std::unique_lock<std::shared_mutex> lock(node->mutex);
            // 使用更安全的字符串操作
            size_t old_size = node->value.size();
            std::string temp_value = node->value;
            temp_value.append(value);
            node->value = std::move(temp_value);
            size_t new_size = node->value.size();
            //cout << "Before Stored value in " << node->node_id << ": " << node->value.size() <<endl;
            //log_lock.unlock();

            // 验证数据完整性
            if (new_size != old_size + value.size()) {
                std::cerr << "Data corruption detected in " << node_id 
                         << ": expected " << (old_size + value.size()) 
                         << ", got " << new_size << std::endl;
            }
            
            std::lock_guard<std::mutex> log_lock(io_mutex_);
            cout << "After Stored value in " << node->node_id << ": " << node->value.size() <<endl;//" value : "<<node.value<< endl;
            
            break;
        }
    }
    // 这里可以将数据存储到文件或其他存储介质中
    
}

void DataNode::ProcessGet_data(std::string node_id, const std::string value)
{
    // 处理GET命令
    //cout << "Received GET command: " << node_id <<" : "<<value<<endl;
    // 读取数据并返回给coordinator
    int start_pos;
    size_t value_len;
    string value_;
    // 解析value
    size_t offset_pos = value.find("offset_");
    size_t value_len_pos = value.find("value_len_");
    if (offset_pos != std::string::npos && value_len_pos != std::string::npos) {
        start_pos = std::stoi(value.substr(offset_pos + 7, 1)); // 获取offset_的最后一位并转换为int
        value_len = std::stoi(value.substr(value_len_pos + 10)); // 获取value_len_的最后一位并转换为int
    } else {
        std::cerr << "Invalid value format: missing offset_ or value_len_" << std::endl;
        return;
    }
    //cout << "start_pos: " << start_pos << " value_len : " << value_len << endl;
    const auto& node_list_to_get = GetNodeList();
    std::shared_lock<std::shared_mutex> lock(node_list_mutex_);  // 读锁
    // 遍历节点列表，找到对应的节点并读取数据
    for (const auto &node : node_list_to_get)
    {
        //cout<<"node_list_to_get node_id: "<<node.node_id<<endl;
        if (node->node_id == node_id)
        {
            value_.assign(node->value, start_pos, value_len);
             // 发送数据给coordinator
            SendValueToProxy(node_id,value_);
            break;
    }
}

}

void DataNode::SendValueToProxy(string node_id,string value)
{
    // 发送数据给coordinator
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
    {
        std::cerr << "Error: Unable to create socket" << std::endl;
        return;
    }
    // 定义coordinator的地址
    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(PROXY_PORT_DATA);
    const char* denormalized_ip= PROXY_IP;
    if(inet_aton(denormalized_ip, &server_address.sin_addr) <= 0){
        cout<<"dest ip: "<<denormalized_ip<<endl;
        perror("inet_aton fail!");
      }
      int retry_count = 0;
      const int max_retries = 3;
      while (retry_count < max_retries) {
          if (connect(sock, (struct sockaddr *)&server_address, sizeof(server_address)) == 0) {
              break;  // 连接成功
          }
          retry_count++;
          if (retry_count >= max_retries) {
              perror("Failed to connect after retries");
              close(sock);
              return;
          }
          usleep(100000);  // 等待100ms后重试
      }
    string massaga = node_id+"\n"+ value+'\n';
    uint32_t data_length_net = htonl(massaga.size());
    if (write(sock, &data_length_net, sizeof(data_length_net)) != sizeof(data_length_net)) {
        cerr << "Failed to send data length header" << endl;
        close(sock);
        return;
    }
    
    cout << "Sending " << massaga.size() << " bytes to datanode" << endl;
    Socket send_value_to_proxy;
    if (send_value_to_proxy.SendDataReliably(sock,massaga.c_str(),massaga.size()) == false)
    {
        std::cerr << "Failed to send data to proxy" << std::endl;
        close(sock);
        return;
    }

    // ssize_t bytes_sent = send(sock, value.c_str(), value.size(), 0);
    // shutdown(sock, SHUT_WR);
    // std::cout << "Sent value to proxy: " << value.size() <<" from : "<<node_id<< std::endl;
    // if (bytes_sent < 0)
    // {
    //     std::cerr << "Error: Failed to send data" << std::endl;
    //     close(sock);
    //     return;
    // }
    
    //std::cout << "Data sent to coordinator successfully" << std::endl;
    close(sock);
}

void DataNode::ProcessRepair_data(std::string node_id, const std::string offset)
{
    //cout << "Received REPAIR command: " << node_id << " offset : " << offset << endl;
    // 解析节点ID和偏移量
    size_t node_id_pos = node_id.find("node_id_");
    size_t offset_pos = offset.find("offset_");
    int node_id_, offset_;
    if (node_id_pos != std::string::npos && offset_pos != std::string::npos)
    {
        node_id_ = std::stoi(node_id.substr(node_id_pos + 8)); // 获取node_id_的最后一位并转换为int
        offset_ = std::stoi(offset.substr(offset_pos + 7));     // 获取offset_的最后一位并转换为int
        cout << "node_id : " << node_id_ << " offset : " << offset_ << endl;
    }
    else
    {
        std::cerr << "Invalid REPAIR command format" << std::endl;
    }
    const auto& node_list_to_get = GetNodeList();
    std::shared_lock<std::shared_mutex> lock(node_list_mutex_);  // 读锁
    // 遍历节点列表，找到对应的节点并读取数据
    for (const auto &node : node_list_to_get)
    {
        //cout<<"node_list_to_get node_id: "<<node.node_id<<endl;
        if (node->node_id == node_id)
        {

            string value = node->value.substr(offset_, block_size);
            //cout << "value: " << value.size() << endl;
            SendValueToProxy(node_id,value);
            break;
        }
    }

}

int main()
{
    
    cout<<"Please input the number of datanodes,the number of global parity,the number of local parity "<<endl;
    int k, r, p, datanode_number;
    size_t block_size;
    cin >> k;
    cin >> r;
    cin >> p;
    //cin >> block_size;
    datanode_number = k + r + p;
    //cout << "datanode_number: " << datanode_number << endl;
    // 启动多个数据节点
    //std::vector<DataNode> datanodes(datanode_number);
    std::vector<std::unique_ptr<DataNode>> datanodes;
    for (int i = 0; i < datanode_number; i++) {
        datanodes.emplace_back(std::make_unique<DataNode>());
    }
    // 启动数据节点监听
    std::vector<std::thread> threads;
    for (int i = 0; i < datanode_number; i++)
    {
        threads.emplace_back([&, i]() {
            int dataport = DATANODE_PORT_BASE + i;
            datanodes[i]->block_size = block_size;
            datanodes[i]->SetNodeList(datanode_number);
            datanodes[i]->Start(dataport);
        });
    }

    // 等待所有线程完成
    for (auto &thread : threads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }
}
