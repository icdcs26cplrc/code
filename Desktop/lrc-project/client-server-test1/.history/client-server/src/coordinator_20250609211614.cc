

#include "../include/coordinator.hh"

using namespace ClientServer;

Coordinator::Coordinator()
{
    // Initialize the Coordinator
    listen_fd_ = -1;
}

void Coordinator::SetMaxStripeBoundary(ECSchema ecschema)
{
    max_stripe_boundary_ = ecschema.k_datablock * block_size;
}

int Coordinator::GetMaxStripeBoundary()
{
    return max_stripe_boundary_;
}

void Coordinator::StartListen()
{
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0)
        throw std::runtime_error("socket() failed");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(COORDINATOR_PORT);         // Coordinator端口
    addr.sin_addr.s_addr = inet_addr(COORDINATOR_IP);
    
    if ((listen_fd_ = socket(PF_INET, SOCK_STREAM, 0)) < 0)
    {
        perror("create server socket error!");
    }
     

    if (::bind(listen_fd_, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        close(listen_fd_);
        throw std::runtime_error("bind failed: " + std::string(strerror(errno)));
    }

    if (listen(listen_fd_, 100) < 0)
    {
        close(listen_fd_);
        throw std::runtime_error("listen failed: " + std::string(strerror(errno)));
    };
    RunServer();
}
ssize_t Coordinator::RecvCommand(int client_fd, char *buf, size_t max_len, size_t packet_size)
{
    ssize_t total_received = 0;
    size_t pos;
    std::string buffer_str;
    while (total_received < max_len)
    {
        ssize_t received = packet_size;

        // std::cout<<"rev_len before read: "<<total_received<<endl;
        received = read(client_fd, buf + total_received, packet_size);

        // // 检查缓冲区中是否有结束符 '\n'
        buffer_str.append(buf + total_received, received);
        // cout<<"buffer_str: "<<strlen(buffer_str.c_str())<<endl;
        //  if ((pos = buffer_str.find('\n')) != std::string::npos) {
        //      std::cout << "收到完整消息: " << strlen(buf) << std::endl;
        //      break;
        //  }
        if (received <= 0)
        {
            // std::cout << "done \n";
            // close(client_fd);
            break;
        }
        total_received += received;
        // std::cout<<"rev_len after read: "<<total_received<<endl;
    }
    //cout << "total_received: " << total_received << endl;
    return total_received;
}
void Coordinator::RunServer()
{
    while (true)
    {
        // sockaddr_in client_addr{};
        // socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(listen_fd_, nullptr, nullptr);
        if (client_fd < 0)
        {
            if (errno == EINTR)
                continue;
            throw std::runtime_error("accept() failed");
        }
        // std::cout<<"Coordinator is waiting for connection..."<<endl;

        HandleConnection(client_fd);
        //     // 使用线程池处理连接（简化为新线程）
        //     std::thread([this, client_fd] {
        //         int fd = client_fd;
        //         std::cout << "Thread started, client_fd: " << fd << std::endl; // 检查线程是否启动
        //         try {
        //             HandleConnection(fd);
        //         } catch (const std::exception& e) {
        //             close(client_fd);
        //             std::cerr << "Error handling connection: " << e.what() << std::endl;
        //         }
        //     }).detach();
    }
}

void Coordinator::HandleConnection(int client_fd)
{

    //std::cout << "Start revData" << endl;
    char cmd_buf[max_stripe_boundary_]; // 1MB buffer
    size_t packet_size = 1024;          // 1KB
    ssize_t received = RecvCommand(client_fd, cmd_buf, sizeof(cmd_buf), packet_size);
    if (received <= 0)
    {
        std::cerr << "recv() failed" << std::endl;
        close(client_fd);
        return;
    }
    std::string cmd(cmd_buf, received);
    // std::cout << "Received command: " << cmd << std::endl;

    // Parse the command
    std::istringstream iss(cmd);
    std::string operation, key, value;
    iss >> operation >> key;
    if (operation == "SET")
    {
        value.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        // Process SET command
        std::cout << "Received SET command: " << key.size() << "  " << value.size() << std::endl;
        ProcessSet(key, value);
        close(client_fd);
    }
    else if (operation == "GET")
    {
        std::cout << "Received GET command: " << key<< std::endl;
        ProcessGet(key);
        close(client_fd); 
    }
    else if (operation == "REPAIR")
    {
        value.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        std::istringstream iss_fail_message(value);
        string fail_num, value_;
        iss_fail_message >> fail_num;
        value_.assign(std::istreambuf_iterator<char>(iss_fail_message >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        ProcessRepair(key, fail_num, value_);
        close(client_fd);
    }
    else
    {
        std::cerr << "Unknown command: " << operation << std::endl;
        close(client_fd);
    }
}

void Coordinator::SetEcschema(ECSchema ecschema)
{
    ecschema_cordinate_ = ecschema;
}

ECSchema Coordinator::GetEcschema()
{
    return ecschema_cordinate_;
}

void Coordinator::SetObjectList()
{
    object_list_.clear();
}

vector<ObjectItem> Coordinator::GetObjectList()
{
    return object_list_;
}

void Coordinator::SetBlockList()
{
    block_list_.clear();
}

vector<BlockItem> Coordinator::GetBlockList()
{
    return block_list_;
}

void Coordinator::SetStripeList()
{
    stripe_list_.clear();
    StripeItem stripe;
    stripe = buffer_node_.stripe;
    stripe_list_.push_back(stripe);
}

vector<StripeItem> Coordinator::GetStripeList()
{
    return stripe_list_;
}

void Coordinator::SetNodeList(int k, int r, int p)
{
    int n = k + r + p;
    node_list_.resize(n);
    for (int i = 0; i < n; i++)
    {
        NodeItem node;
        node.Node_id = i;
        node.alive = 1;
        node.Node_ip = "127.0.0.1";
        node.Node_port = DATANODE_PORT_BASE + i;
        node_list_[i] = node;
        // cout << "Node_ " << i << "_id: "<<node.Node_id<<" _ip: " << node.Node_ip << " _port:" << node.Node_port <<"\n"<< endl;
    }
}

vector<NodeItem> Coordinator::GetNodeList()
{
    return node_list_;
}

void Coordinator::StoreDataInBuffer(const std::string &key, const std::string &value, int k, int r, int p, size_t value_len)
{
    // Store the value in buffer_node
    StripeItem stripe;
    // 修改object元数据
    ObjectItem object;
    object.key = key;
    object.stripe_id = buffer_node_.stripe.Stripe_id;
    // cout<<"stripe_id: "<<object.stripe_id<<endl;
    int *block_indx = (int *)malloc(sizeof(int));
    block_indx[0] = buffer_node_.current_size / block_size;
    // cout<<"stored in stripe_ : "<<object.stripe_id<<" block_ : "<<block_indx[0]<<endl;
    size_t offset_0 = buffer_node_.current_size % block_size;
    int value_remain_size = value_len - (block_size - (buffer_node_.current_size % block_size));
    int block_num_ = 1;
    // cout<<"value_remain_size : "<<value_remain_size<<endl;
    if (value_remain_size > 0)
    {
        // 需要分割数据
        object.block_idx.push_back(block_indx[0]);
        object.offset.push_back(offset_0);
        block_num_ = value_remain_size / block_size + 1 + 1;
        //cout << "need more block to store data :" << block_num_ << endl;
        for (int i = 1; i < block_num_; i++)
        {
            block_indx[i] = block_indx[0] + i;
            object.block_idx.push_back(block_indx[i]);
            object.offset.push_back(0);
        }
    }
    else
    {
        // 不需要分割数据
        //cout << "one block is enough" << endl;
        object.block_idx.push_back(block_indx[0]);
        object.offset.push_back(offset_0);
    }

    object.status = 1;
    object.object_size = value_len;
    object_list_.push_back(object);
    //cout << "object_list_.size : " << object_list_.size() << endl;
    // 修改stripe元数据
    //  stripe.Stripe_id = buffer_node_.stripe.Stripe_id;
    //  stripe.block_size = buffer_node_.stripe.block_size;
    //  stripe.k = buffer_node_.stripe.k;
    //  stripe.r = buffer_node_.stripe.r;
    //  stripe.p = buffer_node_.stripe.p;
    //  stripe.encodetype = buffer_node_.stripe.encodetype;
    //  stripe.nodes = buffer_node_.stripe.nodes;
    //  stripe_list_.push_back(stripe);
    // 修改block元数据
    BlockItem blocks[block_num_];
    for (int i = 0; i < block_num_; i++)
    {
        blocks[i].block_id = "stripe_" + std::to_string(buffer_node_.stripe.Stripe_id) + " block_" + std::to_string(block_indx[i]);
        //cout << "block_id : " << blocks[i].block_id << endl;
        blocks[i].objects.push_back(object);
        block_list_.push_back(blocks[i]);
    }

    // 修改buffer_node
    // std::cout << "before:buffer_node_.current_size: " << buffer_node_.current_size << endl;
    buffer_node_.current_size += value_len;
    // buffer_node_.data.append(value);
    std::cout << "after:buffer_node_.current_size: " << buffer_node_.current_size << endl;

    // Send acknowledgment to the client
}

void Coordinator::SendBlockToDatanode(int datanode_port, int node_id, const char *data)
{
    // cout<<"data size : "<<std::string(data,block_size).size()<<endl;
    Socket socket_datanode;
    std::string node_id_str = "node_id_" + std::to_string(node_id);
    std::string data_str(block_size, '\0');

    memcpy(data_str.data(), data, block_size);
    std::string buf_str = "SET " + node_id_str + " " + data_str;
    size_t chunk_size = buf_str.size();
    char *buf = new char[chunk_size];
    memcpy(buf, buf_str.data(), chunk_size);
    // cout<<"data port : "<<datanode_port<<" chunk_size: "<<chunk_size<<endl;
    size_t packet_size = chunk_size / 2;
    const char *des_ip = "127.0.0.1";
    const char* datanode_ips[] = {
        DATANODE_IP_0, DATANODE_IP_1, DATANODE_IP_2, DATANODE_IP_3,
        DATANODE_IP_4, DATANODE_IP_5, DATANODE_IP_6, DATANODE_IP_7,
        DATANODE_IP_8, DATANODE_IP_9, DATANODE_IP_10, DATANODE_IP_11,
        DATANODE_IP_12, DATANODE_IP_13, DATANODE_IP_14
    };

    if (node_id >= 0 && node_id < sizeof(datanode_ips) / sizeof(datanode_ips[0])) {
        des_ip = datanode_ips[node_id];
    } else {
        std::cerr << "Invalid node_id: " << node_id << std::endl;
        return;
    }
    int des_port = datanode_port;
    socket_datanode.sendData(buf, chunk_size, packet_size, des_ip, des_port);
}

void Coordinator::SendCommandToProxy(int port, std::string value)
{
    // client-side socket
    int client_socket = InitClient_Cor();

    size_t ret;

    // set server-side sockaddr_in
    struct sockaddr_in remote_addr;
    bzero(&remote_addr, sizeof(remote_addr));
    remote_addr.sin_family = AF_INET;
    remote_addr.sin_port = htons(port);
    // char* denormalized_ip = denormalizeIP(des_ip);
    const char *denormalized_ip = PROXY_IP;
    if (inet_aton(denormalized_ip, &remote_addr.sin_addr) <= 0)
    {
        // cout << "dest ip: " << denormalized_ip << endl;
        perror("inet_aton fail!");
    }
    // connect, connect server
    while (connect(client_socket, (struct sockaddr *)&remote_addr, sizeof(remote_addr)) < 0)
        ;
    // send data

    ret = write(client_socket, value.c_str(), value.size());

    if (ret < 0)
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
    cout << "Data sent to proxy successfully (Cor)" << endl;
    // 发送完数据后，关闭连接
    close(client_socket);
    // cout << "send value to proxy" << endl;
}

bool Coordinator::ProcessSet(const std::string &key, const std::string &value)
{
    // size_t value_len = value.size();
    size_t max_boundery = GetMaxStripeBoundary();

    int k = buffer_node_.stripe.k;
    int r = buffer_node_.stripe.r;
    int p = buffer_node_.stripe.p;
    int num = k + r + p;
    // Check if the value fits in the current buffer node
    size_t length = 0;
    size_t pos = value.find('_');
    if (pos != std::string::npos)
    {
        std::string length_str = value.substr(pos + 1);
        try
        {
            length = std::stoi(length_str);
            //std::cout << "Extracted data length: " << length << std::endl;
            // Use the extracted length as needed
        }
        catch (const std::out_of_range &e)
        {
            std::cerr << "Data length out of range: " << value << std::endl;
        }
    }
    else
    {
        std::cerr << "Invalid data length format: " << value << std::endl;
    }
    size_t value_len = length;

    if (value_len + buffer_node_.current_size <= max_stripe_boundary_)
    {

        // Store the value in buffer_node
        StoreDataInBuffer(key, value, k, r, p, value_len);
        string set_command = "SET Store_in_buffer " + value;
        SendCommandToProxy(PROXY_PORT, set_command);
    }
    else
    {
        string encode_info = to_string(k) + "_" + to_string(r) + "_" + to_string(p) + "_" + to_string(block_size);
        string set_comm = "SET Encode " + value + " " + ecschema_cordinate_.encodetype + " " + encode_info;
        SendCommandToProxy(PROXY_PORT, set_comm);
        // Buffer is full, need to encode and create a new stripe
        // Check if buffer_node_.current_size is less than max_stripe_boundary_
        // if (buffer_node_.current_size < max_stripe_boundary_)
        // {
        //     size_t padding_size = max_stripe_boundary_ - buffer_node_.current_size;
        //     buffer_node_.data.append(padding_size, '\0'); // Pad with zeros
        // }
        // // cout<<"buffer_node_.data.size() before encoding: "<<buffer_node_.data.size()<<endl;
        // // cout<<"buffer_node_.data: "<<buffer_node_.data<<endl;
        // std::vector<char *> data_ptrs(k, nullptr);
        // std::vector<char *> global_coding_ptrs(r, nullptr);
        // std::vector<char *> local_coding_ptrs(p, nullptr); // Divide buffer_node_.data into k blocks and assign to data_ptrs

        // for (int i = 0; i < k; i++)
        // {
        //     data_ptrs[i] = new char[block_size];
        //     size_t start_pos = i * block_size;
        //     size_t copy_len = block_size;
        //     std::string data = buffer_node_.data.substr(start_pos, copy_len);
        //     // cout<<"before encoding data_ptrs ["<<i<<"]"<<data.size()<<endl;
        //     std::memcpy(data_ptrs[i], data.data(), copy_len);
        //     //cout<<" encoding data_ptrs ["<<i<<"]"<<std::string(data_ptrs[i],block_size).size()<<endl;
        // }

        // for (int i = 0; i < (r + p); i++)
        // {
        //     if (i < r)
        //     {
        //         global_coding_ptrs[i] = new char[block_size];
        //     }
        //     else
        //     {
        //         local_coding_ptrs[i - r] = new char[block_size];
        //     }
        // }

        // if (ecschema_cordinate_.encodetype == "new_lrc")
        // {

        //     NewLRC new_lrc;
        //     new_lrc.new_encode(k, r, p, data_ptrs.data(), global_coding_ptrs.data(), local_coding_ptrs.data(), block_size);
        //     cout << "new_lrc encoding done" << endl;
        // }

        // // debug
        // //  for (int i = 0; i < (k +r+p); i++)
        // //  {
        // //     if (i<k)
        // //     {
        // //         cout<<"after encoding data_ptrs["<<i<<"] size: "<<std::string(data_ptrs[i],block_size).size()<<endl;
        // //     }else if(i>=k && i<k+r)
        // //     {
        // //         cout<<"after encoding global_coding_ptrs["<<i-k<<"] size: "<<std::string(global_coding_ptrs[i-k],block_size).size()<<endl;
        // //     }else
        // //     {
        // //         cout<<"after encoding local_coding_ptrs["<<i-k-r<<"] size: "<<std::string(local_coding_ptrs[i-k-r],block_size).size()<<endl;
        // //     }

        // //  }

        // // 2. send data to datanode
        // // Send data to datanodes
        // std::vector<std::thread> threads;
        // for (int i = 0; i < num; i++)
        // {
        //     if (i < k)
        //     {
        //         threads.emplace_back([this, i, data_ptrs]()
        //                              {
        //             int dataport_ = DATANODE_PORT_BASE + i;
        //             int node_id = i;
        //             SendBlockToDatanode(dataport_, node_id, data_ptrs[i]); });
        //     }
        //     else if (i >= k && i < k + r)
        //     {
        //         threads.emplace_back([this, i, global_coding_ptrs, k]()
        //                              {
        //             int dataport_ = DATANODE_PORT_BASE + i;
        //             int node_id = i;
        //             SendBlockToDatanode(dataport_, node_id, global_coding_ptrs[i-k]); });
        //     }
        //     else
        //     {
        //         threads.emplace_back([this, i, local_coding_ptrs, k, r]()
        //                              {
        //             int dataport_ = DATANODE_PORT_BASE + i;
        //             int node_id = i;
        //             SendBlockToDatanode(dataport_, node_id, local_coding_ptrs[i-k-r]); });
        //     }
        // }

        // // Wait for all threads to complete
        // for (auto &thread : threads)
        // {
        //     if (thread.joinable())
        //     {
        //         thread.join();
        //     }
        // }

        // 3.Reset buffer_node and create a new stripe
        // 通过stripe_id将条带中object.status变为0
        int stripe_id_ = buffer_node_.stripe.Stripe_id;
        for (auto &object : object_list_)
        {
            if (object.stripe_id == stripe_id_)
            {
                object.status = 0;
            }
        }
        buffer_node_.stripe.Stripe_id++;
        buffer_node_.data.clear();
        buffer_node_.current_size = 0;
        StripeItem stripe;
        stripe = buffer_node_.stripe;
        stripe_list_.push_back(stripe);
        StoreDataInBuffer(key, value, k, r, p, value_len);
    }
}

void Coordinator::ProcessGet(const std::string &key)
{
    //std::cout << "Received GET command: " << key << std::endl;
    ObjectItem object_get;
    // Search for the object in the object list
    bool found = false;
    for (const auto &object : object_list_)
    {
        if (object.key == key)
        {
            object_get = object;
            found = true;
            //cout << "objct_get.size: " << object_get.object_size << endl;
            break;
        }
    }

    if (!found)
    {
        std::cerr << "Error: Object with key '" << key << "' not found." << std::endl;
        return;
    }
    string get_command;
    size_t value_len = object_get.object_size;
    string valuelen = "valuelen_" + to_string(value_len);
    if (object_get.status == 1)
    {
        // Object is still in the buffer node, send it directly
        std::string value;
        //cout << "object_get.block_idx[0]: " << object_get.block_idx[0] << endl;
        //cout << "object_get.offset[0]: " << object_get.offset[0] << endl;
        size_t start_pos = object_get.block_idx[0] * block_size + object_get.offset[0];
        string startpos = "start_" + to_string(start_pos);
        size_t copy_len = value_len;
        //cout << "start_pos: " << start_pos << " copy_len : " << copy_len << endl;
        // value = buffer_node_.data.substr(start_pos, copy_len) + '\n';
        // cout << "value.size: " << value.size() << endl;

        get_command = "GET proxy " + valuelen + " " + startpos;
        SendCommandToProxy(PROXY_PORT, get_command);
        // Send the value to the client
        // int sock = socket(AF_INET, SOCK_STREAM, 0);
        // if (sock < 0)
        //     cerr << "Error: Unable to create socket" << endl;

        // sockaddr_in server_address;
        // server_address.sin_family = AF_INET;
        // server_address.sin_port = htons(CLIENT_PORT);
        // if (inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr) <= 0)
        // {
        //     cerr << "Error: Invalid address or address not supported" << endl;
        //     close(sock);
        // }

        // if (connect(sock, (struct sockaddr *)&server_address, sizeof(server_address)) < 0)
        // {
        //     cerr << "Error: Connection failed" << endl;
        //     close(sock);
        // }

        // ssize_t bytes_sent = send(sock, value.c_str(), value.size(), 0);
        // cout << "GET:send bytes: " << bytes_sent << endl;
        // if (bytes_sent < 0)
        // {
        //     cerr << "Error: Failed to send data" << endl;
        //     close(sock);
        // }
        // close(sock);
    }
    else if (object_get.status == 0)
    {
        // Object is not in the buffer node, need to retrieve it from the datanode
        //std::cout << "Object with key '" << key << "' is not in the buffer node." << std::endl;
        // Retrieve the object from the datanode using its block ID
        int num = object_get.block_idx.size();
        vector<std::string> node_id(num);
        int *offset = new int[num];
        size_t *value_len = new size_t[num];
        int *dataport = new int[num];
        int stripe_id = object_get.stripe_id;
        string get_info;
        size_t get_info_pos = 0;
        for (int i = 0; i < num; i++)
        {
            node_id[i] = "node_id_" + std::to_string(object_get.block_idx[i]);
            offset[i] = stripe_id * block_size + object_get.offset[i];
            if (i != num - 1)
            {
                value_len[i] = block_size - offset[i];
            }
            else
            {
                // Last block, calculate the remaining size
                size_t sum_before_len = 0;
                for (int i = 0; i < (num - 1); i++)
                {
                    sum_before_len += value_len[i];
                }
                // cout<<"sum_before_len: "<<sum_before_len<<endl;
                value_len[i] = object_get.object_size - sum_before_len;
            }
            dataport[i] = DATANODE_PORT_BASE + object_get.block_idx[i];
            //cout << "get data from " << node_id[i] << " offset : " << offset[i] << " value_len : " << value_len[i] << " datanode port: " << dataport[i] << endl;
            std::string temp = node_id[i] + " offset_" + to_string(offset[i]) + " split_value_len_" + to_string(value_len[i]);
            get_info.append(temp + " ");
        }
        string get_number = "num_" + to_string(num);
        get_command = "GET datanode " + valuelen + " " + get_number + " stripe_" + to_string(stripe_id) + " " + get_info;
        SendCommandToProxy(PROXY_PORT, get_command);
        // Send a request to the datanode to retrieve the object
        // Send the request to the datanode
        //     vector<std::thread> threads;
        //     for (int i = 0; i < num; i++)
        //     {
        //         threads.emplace_back([this, i, dataport, node_id, offset, value_len]()
        //                              { SendGetRequest(dataport[i], node_id[i], offset[i], value_len[i]); });
        //     }
        //     // Wait for all threads to complete
        //     for (auto &thread : threads)
        //     {
        //         if (thread.joinable())
        //         {
        //             thread.join();
        //         }
        //     }
        //     // Receive the data from the datanode
        //     char **data = new char *[num];
        //     // cout<<"data size: "<<sizeof(data)<<endl;
        //     for (int i = 0; i < num; i++)
        //     {
        //         data[i] = new char[block_size];
        //         // cout<<"data["<<i<<"] size: "<<sizeof(data[i])<<endl;
        //         // std::cout << "data[" << i << "] size: " << sizeof(data[i]) << std::endl;
        //     }
        //     GetRevData(data, num, value_len);
        //     cout << "GetRevData done" << endl;
        //     // Combine the received data into a single value
        //     std::string value;
        //     for (int i = 0; i < num; i++)
        //     {
        //         // cout << "data[" << i << "] size: " << strlen(data[i])<< endl;
        //         value.append(data[i], value_len[i]);
        //         if (i == num - 1)
        //         {
        //             if (value.size() != object_get.object_size)
        //             {
        //                 std::cerr << "Error: Object size mismatch." << std::endl;
        //                 // Handle error
        //             }
        //         }
        //     }
        //     // Send the value to the client
        //     SendValueToClient(value);
    }
}

void Coordinator::SendValueToClient(std::string value)
{
    // Send the value to the client
    // cout<<"SendValueToClient"<<endl;
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
    {
        std::cerr << "Error: Unable to create socket" << std::endl;
        return;
    }
    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(CLIENT_PORT);
    const char* denormalized_ip = CLIENT_IP;
    if (inet_aton(denormalized_ip, &server_address.sin_addr) <= 0)
    {
        std::cerr << "Error: Invalid address or address not supported" << std::endl;
        close(sock);
        return;
    }
    if (connect(sock, (struct sockaddr *)&server_address, sizeof(server_address)) < 0)
    {
        std::cerr << "Error: Connection failed" << std::endl;
        close(sock);
        return;
    }
    ssize_t bytes_sent = write(sock, value.c_str(), value.size());
    std::cout << "Sent value to client: " << value.size() << std::endl;
    if (bytes_sent < 0)
    {
        std::cerr << "Error: Failed to send data" << std::endl;
        close(sock);
    }
    close(sock);
}

void Coordinator::SendGetRequest(int datanode_port, std::string node_id, int offset, size_t value_len)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
    {
        std::cerr << "Error: Unable to create socket" << std::endl;
        return;
    }
    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(datanode_port);
    if (inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr) < 0)
    {
        std::cerr << "Error: Invalid address or address not supported" << std::endl;
        close(sock);
        return;
    }
    if (connect(sock, (struct sockaddr *)&server_address, sizeof(server_address)) < 0)
    {
        std::cerr << "Error: Connection failed" << std::endl;
        close(sock);
        return;
    }
    // Prepare the message
    std::string message = "GET " + node_id + " offset_" + std::to_string(offset) + " value_len_" + std::to_string(value_len) + "\n";
    // Send the message
    if (send(sock, message.c_str(), message.size(), 0) < 0)
    {
        std::cerr << "Error: Failed to send data" << std::endl;
        close(sock);
        return;
    }
    std::cout << "Sent GET request to datanode: " << node_id << std::endl;
    close(sock);
}

void Coordinator::StartProxy()
{
    cout << "GetRevData" << endl;
    sock_proxy = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_proxy < 0)
    {
        std::cerr << "Error: Unable to create socket" << std::endl;
        return;
    }
    // 设置 SO_REUSEADDR 以允许端口重用
    int opt = 1;
    if (setsockopt(sock_proxy, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        close(sock_proxy);
        throw std::runtime_error("setsockopt() failed: " + std::string(strerror(errno)));
    }

    sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PROXY_PORT); // 绑定到目标端口
    if (::bind(sock_proxy, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        close(sock_proxy);
        throw std::runtime_error("bind failed: " + std::string(strerror(errno)));
        return;
    }
    if (listen(sock_proxy, 100) < 0)
    {
        close(sock_proxy);
        throw std::runtime_error("listen failed");
    }
}

void Coordinator::GetRevData(char **data, int num, size_t *value_len)
{
    int *connfd = new int[num];
    vector<std::thread> recv_thrds;
    int retry_count = 0;
    int max_retry = 3; // 最大重试次数
    int index = 0;
    while (true)
    {

        connfd[index] = accept(sock_proxy, nullptr, nullptr);
        if (connfd[index] < 0)
        {
            if (errno == EBADF && retry_count < max_retry)
            {
                // 重建监听套接字
                close(sock_proxy);
                StartProxy(); // 重新初始化 sock_proxy
                retry_count++;
                continue;
            }
            // std::cerr << "accept failed: " << strerror(errno) << std::endl;
            // close(connfd[index]);
            // continue;
        }
        int conffd_accept = connfd[index];
        recv_thrds.emplace_back([this, index, conffd_accept, &data, &value_len]()
                                { HandleGetRev(data[index], conffd_accept, value_len[index]); });
        index++;
        if (index == num)
        {
            break;
        }
    }

    for (int i = 0; i < num; i++)
    {
        recv_thrds[i].join();
    }

    for (int i = 0; i < num; i++)
    {
        close(connfd[i]);
    }
}

void Coordinator::HandleGetRev(char *&data, int connfd, size_t value_len)
{
    // std::cout << "Received data from datanode" << std::endl;
    ssize_t total_received = 0;
    data = new char[value_len];
    struct timeval timeout{.tv_sec = 5, .tv_usec = 0}; // 5秒超时

    // 设置套接字超时
    setsockopt(connfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    while (total_received < value_len)
    {
        ssize_t received = read(connfd, data, value_len);
        if (received == 0)
        {
            std::cerr << "Connection closed by peer (received " << total_received << "/" << value_len << " bytes)" << std::endl;
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
    }
    //cout << "Received data size: " << total_received << endl;
    close(connfd);
}

int Coordinator::ParseFailMassage(string failed_block_number, string failed_block, int *&stripe_id, int *&node_id)
{
    // Extract the number of failed blocks from failed_block_number
    int fail_number = std::stoi(failed_block_number.substr(failed_block_number.find_last_of('_') + 1));

    // Resize stripe_id and node_id arrays to hold the extracted values
    stripe_id = new int[fail_number];
    node_id = new int[fail_number];

    // Split the failed_block string into individual blocks
    std::istringstream iss(failed_block);
    std::string block;
    int index = 0;

    while (iss >> block && index < fail_number)
    {
        // Extract stripe_id and node_id from each block
        size_t stripe_pos = block.find("stripe_");
        size_t block_pos = block.find("_block_");

        if (stripe_pos != std::string::npos && block_pos != std::string::npos)
        {
            stripe_id[index] = std::stoi(block.substr(stripe_pos + 7, block_pos - (stripe_pos + 7)));
            node_id[index] = std::stoi(block.substr(block_pos + 7));
            index++;
        }
        else
        {
            std::cerr << "Invalid block format: " << block << std::endl;
        }
    }
    // cout << "fail_number: " << fail_number << endl;
    // cout << "stripe_id: ";
    // for (int i = 0; i < fail_number; i++)
    // {
    //     cout << stripe_id[i] << " ";
    // }
    // cout << endl;
    // cout << "node_id: ";
    // for (int i = 0; i < fail_number; i++)
    // {
    //     cout << node_id[i] << " ";
    // }
    // cout << endl;
    return fail_number;
}

void Coordinator::ProcessRepair(string des, string failed_node_number, string failed_node_block)
{

    int *failed_node_id;
    int *failed_node_stripe;
    int failed_number = ParseFailMassage(failed_node_number, failed_node_block, failed_node_stripe, failed_node_id);
    string repair_command, block_info;
    int *block_need;
    int *dataport;
    int num, k, r, p;
    k = ecschema_cordinate_.k_datablock;
    r = ecschema_cordinate_.r_gobalparityblock;
    p = ecschema_cordinate_.p_localgroup;
    string encode_info = ecschema_cordinate_.encodetype+" "+to_string(k) + "_" + to_string(r) + "_" + to_string(p) + "_" + to_string(block_size) + " ";
    string fail_node_messg = "fail_"+to_string(failed_number)+" ";
    cout<<"encode_info: "<<encode_info<<endl;
    for (int i = 0; i < failed_number; i++)
    {
        fail_node_messg += to_string(failed_node_id[i])+"_";
        if (i == (failed_number - 1))
        {
            fail_node_messg += to_string(failed_node_id[i]);
        }
        
    }  
    set<int> unique_stripes(failed_node_stripe, failed_node_stripe + failed_number);
    int unique_count = unique_stripes.size();
    //cout << "Number of unique stripe IDs: " << unique_count << endl;
    size_t offset = failed_node_stripe[0] * block_size;
    
    if (ecschema_cordinate_.encodetype == "new_lrc")
    {

        // if (failed_number == 1)
        // {
        //     int failed_node_id_ = failed_node_id[0];
        //     int failed_node_stripe_ = failed_node_stripe[0];
            // 计算修复需要的数据块索引
            NewLRC new_lrc(k, r, p,block_size);
            num = new_lrc.muti_decode_node_need( failed_number,failed_node_id, block_need);
            // cout << "block_need num: " << num << endl;
            size_t value_len = block_size;
            dataport = new int[num];
            for (int i = 0; i < num; i++)
            {
                dataport[i] = DATANODE_PORT_BASE + block_need[i];
                string temp = "blockneed_" + to_string(block_need[i]) + " ";
                block_info.append(temp);
            }
            // // 向datanode发送请求
            // vector<std::thread> threads;
            // for (int i = 0; i < num; i++)
            // {
            //     threads.emplace_back([this, i, dataport, block_need, offset]()
            //                          { SendRequestToDatanode(dataport[i], offset); });
            // }
            // // Wait for all threads to complete
            // for (auto &thread : threads)
            // {
            //     if (thread.joinable())
            //     {
            //         thread.join();
            //     }
            // }
            // // 接收数据
            // char **data = new char *[num];
            // size_t *value_len_ = new size_t[num];
            // for (int i = 0; i < num; i++)
            // {
            //     data[i] = new char[block_size];
            //     value_len_[i] = block_size;
            // }

            // GetRevData(data, num, value_len_);
            // // 将接收数据划分为data_ptr和code_ptr
            // char **data_ptr_ = new char *[num + 1];
            // for (int i = 0; i < num + 1; i++)
            // {
            //     data_ptr_[i] = new char[block_size];
            // }

            // int pos = -1;
            // for (int i = 0; i < num; i++)
            // {
            //     if (block_need[i] == failed_node_id_ + 1)
            //     {
            //         pos = i;
            //         break;
            //     }
            //     else if (failed_node_id_ == block_need[num - 2] + 1)
            //     {
            //         pos = num - 1;
            //         break;
            //     }
            //     else if (failed_node_id_ > (ecschema_cordinate_.k_datablock + ecschema_cordinate_.r_gobalparityblock - 1))
            //     {
            //         pos = num;
            //         break;
            //     }
            // }

            // for (int i = 0; i < num + 1; i++)
            // {
            //     if (i < pos)
            //     {
            //         data_ptr_[i] = data[i];
            //     }
            //     else if (i == pos)
            //     {
            //         data_ptr_[i] = new char[block_size];
            //     }
            //     else
            //     {
            //         data_ptr_[i] = data[i - 1];
            //     }
            // }
            // char **code_ptr = new char *[1];
            // code_ptr[0] = data_ptr_[num];
            // char **data_ptr = new char *[num];
            // for (int i = 0; i < num; i++)
            // {
            //     data_ptr[i] = new char[block_size];
            //     data_ptr[i] = data_ptr_[i];
            // }

            // // 修复数据
            // bool repair_ok = false;
            // repair_ok = new_lrc.new_single_decode(ecschema_cordinate_.k_datablock, ecschema_cordinate_.r_gobalparityblock, ecschema_cordinate_.p_localgroup, failed_node_id_, data_ptr, code_ptr, block_size, num);
            // if (repair_ok)
            // {
            //     cout << "sucessfully repair" << endl;
            // }
            // else
            // {
            //     cout << "repair fail" << endl;
            // }
        //}
        // else
        // {
            // 多个节点故障
            // 计算failed_node_stripe数组中数值相异的次数
            // 计算修复需要的数据块索引
                // NewLRC new_lrc(k, r, p,);
                // num = new_lrc.new_decode_node_need(k, r, p, failed_number, failed_node_id, block_need);
                // // cout << "block_need num: " << num << endl;
                // size_t value_len = block_size;
                // int *dataport = new int[num];

                // for (int i = 0; i < num; i++)
                // {
                //     dataport[i] = DATANODE_PORT_BASE + block_need[i];
                //     string temp = "blockneed_" + to_string(block_need[i]) + " ";
                //     block_info.append(temp);
                // }

                //         // 向datanode发送请求
                //         vector<std::thread> threads;
                //         for (int i = 0; i < num; i++)
                //         {
                //             threads.emplace_back([this, i, dataport, block_need, offset]()
                //                                  { SendRequestToDatanode(dataport[i], offset); });
                //         }
                //         // Wait for all threads to complete
                //         for (auto &thread : threads)
                //         {
                //             if (thread.joinable())
                //             {
                //                 thread.join();
                //             }
                //         }
                //         // 接收数据
                //         char **data = new char *[num];
                //         size_t *value_len_ = new size_t[num];
                //         for (int i = 0; i < num; i++)
                //         {
                //             data[i] = new char[block_size];
                //             value_len_[i] = block_size;
                //         }

                //         GetRevData(data, num, value_len_);
                //         char **data_ptrs = new char *[ecschema_cordinate_.k_datablock];
                //         char **global_parity = new char *[ecschema_cordinate_.r_gobalparityblock];
                //         char **local_parity = new char *[ecschema_cordinate_.p_localgroup];
                //         int sum = ecschema_cordinate_.k_datablock + ecschema_cordinate_.r_gobalparityblock + ecschema_cordinate_.p_localgroup;
                //         int pos = 0;
                //         for (int i = 0; i < sum; i++)
                //         {
                //             if (i != failed_node_id[i] && pos < num)
                //             {
                //                 if (i < ecschema_cordinate_.k_datablock)
                //                 {
                //                     data_ptrs[i] = new char[block_size];
                //                     data_ptrs[i] = data[pos];
                //                     pos++;
                //                 }
                //                 else if (i >= ecschema_cordinate_.k_datablock && i < ecschema_cordinate_.k_datablock + ecschema_cordinate_.r_gobalparityblock)
                //                 {
                //                     global_parity[i - ecschema_cordinate_.k_datablock] = new char[block_size];
                //                     global_parity[i - ecschema_cordinate_.k_datablock] = data[pos];
                //                     pos++;
                //                 }
                //                 else
                //                 {
                //                     local_parity[i - ecschema_cordinate_.k_datablock - ecschema_cordinate_.r_gobalparityblock] = new char[block_size];
                //                     local_parity[i - ecschema_cordinate_.k_datablock - ecschema_cordinate_.r_gobalparityblock] = data[pos];
                //                     pos++;
                //                 }
                //             }
                //             else
                //             {
                //                 if (i < ecschema_cordinate_.k_datablock)
                //                 {
                //                     data_ptrs[i] = new char[block_size];
                //                 }
                //                 else if (i >= ecschema_cordinate_.k_datablock && i < ecschema_cordinate_.k_datablock + ecschema_cordinate_.r_gobalparityblock)
                //                 {
                //                     global_parity[i - ecschema_cordinate_.k_datablock] = new char[block_size];
                //                 }
                //                 else
                //                 {
                //                     local_parity[i - ecschema_cordinate_.k_datablock - ecschema_cordinate_.r_gobalparityblock] = new char[block_size];
                //                 }
                //             }
                //         }
                //         int repair_ok = -1;
                //         repair_ok = new_lrc.new_decode(ecschema_cordinate_.k_datablock, ecschema_cordinate_.r_gobalparityblock, ecschema_cordinate_.p_localgroup, failed_number, failed_node_id, data_ptrs, global_parity, local_parity, block_size);
                //         if (repair_ok)
                //         {
                //             cout << "sucessfully repair" << endl;
                //         }
                //         else
                //         {
                //             cout << "repair fail" << endl;
                //         }
                //     }
            
        // }
    }
    else if (ecschema_cordinate_.encodetype == "azure")
    {
        AZURE_LRC azure(k,r,p,block_size);
            num = azure.muti_decode_node_need(failed_number, failed_node_id, block_need);
            for (int i = 0; i < num; i++)
            {
                string temp = "blockneed_" + to_string(block_need[i]) + " ";
                block_info.append(temp);
            }
    }
    else if (ecschema_cordinate_.encodetype == "azure_1")
    {
        AZURE_LRC_1 azure_1(k,r,p,block_size);
        num = azure_1.muti_decode_node_need(failed_number, failed_node_id, block_need);
        for (int i = 0; i < num; i++)
        {
            string temp = "blockneed_" + to_string(block_need[i]) + " ";
            block_info.append(temp);
        }
    }
    else if (ecschema_cordinate_.encodetype == "optimal")
    {
        OPTIMAL_LRC optimal(k,r,p,block_size);
        num = optimal.muti_decode_node_need(failed_number, failed_node_id, block_need);
        for (int i = 0; i < num; i++)
        {
            string temp = "blockneed_" + to_string(block_need[i]) + " ";
            block_info.append(temp);
        }
    }
    
    else if (ecschema_cordinate_.encodetype == "uniform")
    {
        UNIFORM_LRC uniform(k,r,p,block_size);
        num = uniform.muti_decode_node_need(failed_number, failed_node_id, block_need);
        for (int i = 0; i < num; i++)
        {
            string temp = "blockneed_" + to_string(block_need[i]) + " ";
            block_info.append(temp);
        }
    }
    else if (ecschema_cordinate_.encodetype == "sep-uni")
    {
        UNBALANCED_LRC unbalanced(k,r,p,block_size);
        num = unbalanced.muti_decode_node_need(failed_number, failed_node_id, block_need);
        for (int i = 0; i < num; i++)
        {
            string temp = "blockneed_" + to_string(block_need[i]) + " ";
            block_info.append(temp);
        }
    }
    else if (ecschema_cordinate_.encodetype == "sep-azure")
    {
        SEP_AZURE sep_azure(k,r,p,block_size);
        num = sep_azure.muti_decode_node_need(failed_number, failed_node_id, block_need);
        for (int i = 0; i < num; i++)
        {
            string temp = "blockneed_" + to_string(block_need[i]) + " ";
            block_info.append(temp);
        }
    }

    repair_command = "REPAIR num_" + to_string(num) +" "+ encode_info + "offset_"+to_string(offset)+" "+fail_node_messg+" "+block_info;
    
    if (des == "proxy")
    {
        SendCommandToProxy(PROXY_PORT_DATA, repair_command);
    }
    else if (des == "client")
    {
        SendCommandToProxy(PROXY_PORT, repair_command);
    }
}

void Coordinator::SendRequestToDatanode(int dataport, size_t offset)
{
    int sock = InitClient_Cor();

    size_t ret;

    // set server-side sockaddr_in
    struct sockaddr_in remote_addr;
    bzero(&remote_addr, sizeof(remote_addr));
    remote_addr.sin_family = AF_INET;
    remote_addr.sin_port = htons(dataport);
    // char* denormalized_ip = denormalizeIP(des_ip);
    const char *denormalized_ip = "127.0.0.1";
    if (inet_aton(denormalized_ip, &remote_addr.sin_addr) == 0)
    {
        cout << "dest ip: " << denormalized_ip << endl;
        perror("inet_aton fail!");
    }

    // connect, connect server
    if (connect(sock, (struct sockaddr *)&remote_addr, sizeof(remote_addr)) < 0)
    {
        cerr << "Error: Connection failed" << endl;
        close(sock);
        return;
    }
    // send data
    string node_id_str = "node_id_" + to_string(dataport - DATANODE_PORT_BASE);
    string offset_str = "offset_" + to_string(offset);
    std::string buf = "REPAIR " + node_id_str + " " + offset_str + "\n";
    const char *buf_cstr = buf.c_str();
    size_t chunk_size = buf.size();
    size_t send_len = write(sock, buf_cstr, chunk_size);
    cout << "send bytes: " << send_len << endl;
    if (send_len < 0)
    {
        cerr << "Error: Failed to send data" << endl;
        close(sock);
        return;
    }
    // close client-side socket
    if ((close(sock)) == -1)
    {
        cout << "close client_socket error!" << endl;
        exit(1);
    }
    close(sock);
}

int Coordinator::InitClient_Cor()
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

void Coordinator::SetBufferNode(int block_size)
{
    buffer_node_.stripe.Stripe_id = 0;
    buffer_node_.stripe.block_size = block_size;
    buffer_node_.stripe.k = ecschema_cordinate_.k_datablock;
    buffer_node_.stripe.r = ecschema_cordinate_.r_gobalparityblock;
    buffer_node_.stripe.p = ecschema_cordinate_.p_localgroup;
    buffer_node_.stripe.encodetype = ecschema_cordinate_.encodetype;
    // Set node list
    vector<NodeItem> tempnode_list = GetNodeList();
    for (int i = 0; i < (ecschema_cordinate_.k_datablock + ecschema_cordinate_.r_gobalparityblock + ecschema_cordinate_.p_localgroup); i++)
    {
        buffer_node_.stripe.nodes.push_back(tempnode_list[i].Node_id);
        // cout << "buffer_Node_ " << i << "_id: "<<tempnode_list[i].Node_id<<" _ip: " << tempnode_list[i].Node_ip << " _port:" << tempnode_list[i].Node_port<<"\n" << endl;
    }

    buffer_node_.current_size = 0;
}

int main()
{
    // Initialize Coordinator
    ECSchema ecschema_input;
    ClientServer::Coordinator coordinator;
    // 输入编码方案
    std::cout << "Please input the encoding scheme: " << "including EncodeType(azure_lrc,azure_lrc_1,optimal,uniform,new_lrc),k_datablock ,p_localgroup, r_gobalparityblock,blocksize\n"
              << "for example: new_lrc 4 2 2 1048576\n"
              << endl;
    cin >> ecschema_input.encodetype;
    cin >> ecschema_input.k_datablock;
    cin >> ecschema_input.p_localgroup;
    cin >> ecschema_input.r_gobalparityblock;
    cin >> ecschema_input.block_size;

    coordinator.block_size = ecschema_input.block_size;
    // Set encoding scheme
    coordinator.SetEcschema(ecschema_input);
    // Set max stripe boundary
    coordinator.SetMaxStripeBoundary(ecschema_input);
    // Set object, block, stripe, and node lists
    coordinator.SetObjectList();
    coordinator.SetBlockList();
    // 为了后续stripe.nodes的初始化
    //  Set node list需在setbuffernode之前
    coordinator.SetNodeList(ecschema_input.k_datablock, ecschema_input.r_gobalparityblock, ecschema_input.p_localgroup);
    // Set buffer node
    coordinator.SetBufferNode(coordinator.block_size);
    coordinator.SetStripeList();

    // Print max stripe boundary
    std::cout << "Max stripe boundary: " << coordinator.GetMaxStripeBoundary() << " bytes" << std::endl;
    // Start listening for connections
    coordinator.StartListen();
    // coordinator.StartProxy();

    return 0;
}
