
#include "../include/datanode.hh"

using namespace ClientServer;

std::mutex ClientServer::DataNode::io_mutex_;

static std::vector<pid_t> g_child_pids;

void DataNode::signal_handler(int sig)
{
    std::cout << "Received signal " << sig << ", terminating all DataNode processes..." << std::endl;
    for (pid_t child_pid : g_child_pids)
    {
        kill(child_pid, SIGTERM);
    }
    exit(0);
}

// void DataNode::graceful_shutdown_handler(int sig)
// {
//     std::cout << "DataNode received signal " << sig << ", shutting down gracefully..." << std::endl;
//     exit(0);
// }

DataNode::DataNode()
{

    listen_fd_data = -1;
}

void DataNode::Start(int port, const std::string &bind_ip)
{

    listen_fd_data = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_data < 0)
        throw std::runtime_error("socket() failed");

    int opt = 1;
    if (setsockopt(listen_fd_data, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        close(listen_fd_data);
        throw std::runtime_error("setsockopt SO_REUSEADDR failed");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    std::string target_ip;
    if (!bind_ip.empty())
    {
        target_ip = bind_ip;
    }
    else
    {
        target_ip = getDatanodeIPByNodeId(port - DATANODE_PORT_BASE);
    }

    if (inet_aton(target_ip.c_str(), &addr.sin_addr) <= 0)
    {
        close(listen_fd_data);
        throw std::runtime_error("Invalid IP address: " + target_ip);
    }

    if (::bind(listen_fd_data, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        close(listen_fd_data);
        throw std::runtime_error("bind failed on " + target_ip + ":" + std::to_string(port) + " - " + std::string(strerror(errno)));
    }

    if (listen(listen_fd_data, 100) < 0)
    {
        close(listen_fd_data);
        throw std::runtime_error("listen failed");
    }

    std::cout << "DataNode listening on " << target_ip << ":" << port << std::endl;
    RunServer_data(port);
}

void DataNode::RunServer_data(int port)
{
    while (true)
    {
        // sockaddr_in client_addr{};
        // socklen_t addr_len = sizeof(client_addr);
        // std::lock_guard<std::mutex> lock(mtx); // 使用锁保护访问
        int data_fd = accept(listen_fd_data, nullptr, nullptr);

        if (data_fd < 0)
        {
            cout << "datanode accept failed" << endl;
            break; // 发生严重错误时退出
        }

        // 为新连接创建处理线程
        //  std::thread([this, data_fd, port]
        //              { HandleConnection_data(data_fd); })
        //      .detach();

        HandleConnection_data(data_fd);
    }
}

void DataNode::HandleConnection_data(int client_fd)
{

    struct timeval timeout;
    timeout.tv_sec = 10; // 10s timeout
    timeout.tv_usec = 0;
    setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    uint32_t data_length_net;
    ssize_t header_received = recv(client_fd, &data_length_net, sizeof(data_length_net), MSG_WAITALL);

    if (header_received != sizeof(data_length_net))
    {
        cerr << "Failed to receive data length header, received: " << header_received << endl;
        close(client_fd);
        return;
    }

    size_t expected_size = ntohl(data_length_net);
    // cout << "Expected to receive: " << expected_size << " bytes" << endl;

    if (expected_size == 0 || expected_size > 1024 * 1024 * 100)
    {
        cerr << "Invalid data size: " << expected_size << endl;
        close(client_fd);
        return;
    }

    char *cmd_buf = new char[expected_size + 1];
    memset(cmd_buf, 0, expected_size + 1);

    // ssize_t received = RevData(client_fd, cmd_buf, expected_size);
    // cout << "Received: " << received << " bytes" << endl;
    // ssize_t received = RevData(client_fd, cmd_buf, chunk_size, packet_size);
    // cout<<"received : "<<received<<endl;
    Socket Datanode;

    if (Datanode.ReceiveDataReliably(client_fd, cmd_buf, expected_size) == false)
    {
        cerr << "Failed to receive data from client" << endl;
        delete[] cmd_buf;
        close(client_fd);
        return;
    }

    string ack = "datanode_ack";
    int ack_sent_times = 0;
    const int max_ack_sent_times = 3;
    while (ack_sent_times < max_ack_sent_times)
    {
        if (write(client_fd, ack.c_str(), ack.size()) < 0)
        {
            cerr << "Failed to send ACK to client" << endl;
            ack_sent_times++;
            continue;
        }
        break;
    }

    std::string cmd(cmd_buf, expected_size);
    // std::string cmd(cmd_buf, received);
    // std::cout << "Received command: " << cmd << std::endl;

    std::istringstream iss(cmd);
    std::string operation, node_id, value;
    iss >> operation >> node_id;

    if (operation == "SET")
    {
        value.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        // std::lock_guard<std::mutex> lock(io_mutex_);
        // lock_guard<std::mutex> log_lock(log_mutex_);
        // cout << "Received SET command: " << value.size() << std::endl;
        // log_lock.unlock();
        ProcessSet_data(node_id, value);
        close(client_fd);
    }
    else if (operation == "GET")
    {
        // cout<<"Received GET command: " << cmd << endl;
        value.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        ProcessGet_data(node_id, value);
        close(client_fd);
    }
    else if (operation == "REPAIR")
    {
        value.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        // std::cout << "Received REPAIR command: " << node_id << std::endl;
        ProcessRepair_data(node_id, value);
        close(client_fd);
    }
    else
    {
        std::cerr << "Unknown operation: " << operation << std::endl;
    }

    close(client_fd);
}
ssize_t DataNode::RevData(int client_fd, char *buf, size_t chunk_size) //, size_t packet_size)
{
    ssize_t total_received = 0;
    total_received = read(client_fd, buf, chunk_size);
    return total_received;
}

void DataNode::SetNodeList(int node_id_number)
{

    node_storage_list_.clear();
    node_storage_list_.reserve(1);
    for (int i = 0; i < 1; i++)
    {
        node_storage_list_.emplace_back(                                                   // 直接在容器中构造对象
            std::make_unique<NodeStorage>("node_id_" + std::to_string(node_id_number), "") // value
        );
    }
}

const std::vector<std::unique_ptr<DataNode::NodeStorage>> &DataNode::GetNodeList() const
{

    std::shared_lock<std::shared_mutex> lock(node_list_mutex_);
    return node_storage_list_; // 返回引用，避免拷贝
}

void DataNode::ProcessSet_data(std::string node_id, const std::string value)
{

    for (const auto &node : node_storage_list_)
    {
        // cout<<"node_list_to_set node_id: "<<node.node_id<<endl;
        if (node->node_id == node_id)
        {
            // std::unique_lock<std::shared_mutex> lock(node_list_mutex_);
            //  std::unique_lock<std::shared_mutex> lock(node->mutex);
            //  node->value.append(value);
            std::unique_lock<std::shared_mutex> lock(node->mutex);

            size_t old_size = node->value.size();
            std::string temp_value = node->value;
            temp_value.append(value);
            node->value = std::move(temp_value);
            size_t new_size = node->value.size();
            // cout << "Before Stored value in " << node->node_id << ": " << node->value.size() <<endl;
            // log_lock.unlock();

            if (new_size != old_size + value.size())
            {
                std::cerr << "Data corruption detected in " << node_id
                          << ": expected " << (old_size + value.size())
                          << ", got " << new_size << std::endl;
            }

            std::lock_guard<std::mutex> log_lock(io_mutex_);
            cout << "After Stored value in " << node->node_id << ": " << node->value.size() << endl; //" value : "<<node.value<< endl;

            break;
        }
    }
}

void DataNode::ProcessGet_data(std::string node_id, const std::string value)
{

    int start_pos;
    size_t value_len;
    string value_;

    size_t offset_pos = value.find("offset_");
    size_t value_len_pos = value.find("value_len_");
    if (offset_pos != std::string::npos && value_len_pos != std::string::npos)
    {
        start_pos = std::stoi(value.substr(offset_pos + 7, 1));  // 获取offset_的最后一位并转换为int
        value_len = std::stoi(value.substr(value_len_pos + 10)); // 获取value_len_的最后一位并转换为int
    }
    else
    {
        std::cerr << "Invalid value format: missing offset_ or value_len_" << std::endl;
        return;
    }
    // cout << "start_pos: " << start_pos << " value_len : " << value_len << endl;
    const auto &node_list_to_get = GetNodeList();
    std::shared_lock<std::shared_mutex> lock(node_list_mutex_); // 读锁

    for (const auto &node : node_list_to_get)
    {
        // cout<<"node_list_to_get node_id: "<<node.node_id<<endl;
        if (node->node_id == node_id)
        {
            value_.assign(node->value, start_pos, value_len);

            SendValueToProxy(node_id, value_);
            break;
        }
    }
}

void DataNode::SendValueToProxy(string node_id, string value)
{

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
    {
        std::cerr << "Error: Unable to create socket" << std::endl;
        return;
    }

    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(PROXY_PORT_DATA);
    const char *denormalized_ip = PROXY_IP;
    if (inet_aton(denormalized_ip, &server_address.sin_addr) <= 0)
    {
        cout << "dest ip: " << denormalized_ip << endl;
        perror("inet_aton fail!");
    }
    int retry_count = 0;
    const int max_retries = 3;
    while (retry_count < max_retries)
    {
        if (connect(sock, (struct sockaddr *)&server_address, sizeof(server_address)) == 0)
        {
            break;
        }
        retry_count++;
        if (retry_count >= max_retries)
        {
            perror("Failed to connect after retries");
            close(sock);
            return;
        }
        usleep(100000);
    }
    string massaga = node_id + "\n" + value + '\n';
    cout << "Sending data to proxy , from " << node_id << endl;
    uint32_t data_length_net = htonl(massaga.size());
    if (write(sock, &data_length_net, sizeof(data_length_net)) != sizeof(data_length_net))
    {
        cerr << "Failed to send data length header" << endl;
        close(sock);
        return;
    }

    cout << "Sending " << massaga.size() << " bytes to datanode" << endl;
    Socket send_value_to_proxy;
    if (send_value_to_proxy.SendDataReliably(sock, massaga.c_str(), massaga.size()) == false)
    {
        std::cerr << "Failed to send data to proxy" << std::endl;
        close(sock);
        return;
    }

    // std::cout << "Data sent to coordinator successfully" << std::endl;
    close(sock);
}

void DataNode::ProcessRepair_data(std::string node_id, const std::string offset)
{
    // cout << "Received REPAIR command: " << node_id << " offset : " << offset << endl;

    size_t node_id_pos = node_id.find("node_id_");
    size_t offset_pos = offset.find("offset_");
    int node_id_, offset_;
    if (node_id_pos != std::string::npos && offset_pos != std::string::npos)
    {
        node_id_ = std::stoi(node_id.substr(node_id_pos + 8)); // 获取node_id_的最后一位并转换为int
        offset_ = std::stoi(offset.substr(offset_pos + 7));    // 获取offset_的最后一位并转换为int
        cout << "node_id : " << node_id_ << " offset : " << offset_ << endl;
    }
    else
    {
        std::cerr << "Invalid REPAIR command format" << std::endl;
    }
    const auto &node_list_to_get = GetNodeList();
    std::shared_lock<std::shared_mutex> lock(node_list_mutex_); // 读锁

    for (const auto &node : node_list_to_get)
    {
        // cout<<"node_list_to_get node_id: "<<node.node_id<<endl;
        if (node->node_id == node_id)
        {

            string value = node->value.substr(offset_, block_size);
            // cout << "value: " << value.size() << endl;
            SendValueToProxy(node_id, value);
            break;
        }
    }
}

bool DataNode::IsProcessAlive(pid_t pid)
{
    return kill(pid, 0) == 0;
}

void DataNode::CleanupResources()
{
    if (listen_fd_data >= 0)
    {
        close(listen_fd_data);
        listen_fd_data = -1;
    }

    node_storage_list_.clear();
}

void DataNode::graceful_shutdown_handler(int sig)
{
    std::cout << "DataNode received signal " << sig << ", shutting down gracefully..." << std::endl;

    exit(0);
}

bool DataNode::HealthCheck()
{

    if (listen_fd_data < 0)
    {
        return false;
    }

    if (node_storage_list_.empty())
    {
        return false;
    }

    return true;
}
