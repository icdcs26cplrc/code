

#include "../include/coordinator.hh"

using namespace ClientServer;

void Coordinator::SetMaxStripeBoundary(ECSchema ecschema) {
    max_stripe_boundary_ = (ecschema.k_datablock + ecschema.r_gobalparityblock + ecschema.p_localgroup)*1024*1024;
}

int Coordinator::GetMaxStripeBoundary() {
    return max_stripe_boundary_;
}

void Coordinator::StartListen() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) throw std::runtime_error("socket() failed");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(COORDINATOR_PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr); // 设置为本地地址 127.0.0.1
    
    if (::bind(listen_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(listen_fd_);
        throw std::runtime_error("bind failed: " + std::string(strerror(errno)));
    }

    if(listen(listen_fd_, 100)<0) {
        close(listen_fd_);
        throw std::runtime_error("listen failed: " + std::string(strerror(errno)));
    };
}


ssize_t Coordinator::RecvCommand(int client_fd, char* buf, size_t max_len) {
    auto& buffer = recv_buffers_[client_fd];
    
    // 尝试从缓冲区提取完整命令
    size_t pos = buffer.find(CMD_DELIMITER);
    if (pos != std::string::npos) {
        size_t copy_len = std::min(pos, max_len-1);
        buffer.copy(buf, copy_len);
        buffer.erase(0, pos+1); // 移除已处理部分
        buf[copy_len] = '\0';
        return copy_len;
    }

    // 接收新数据
    char temp[1024*1024]; // 1MB buffer
    // 这里使用循环接收数据，直到找到CMD_DELIMITER或连接关闭
    while (true) {
        ssize_t n = recv(client_fd, temp, sizeof(temp), 0);
        if (n <= 0) {
            if (n == 0 && !buffer.empty()) { // 连接关闭但还有数据
                size_t copy_len = std::min(buffer.size(), max_len-1);
                buffer.copy(buf, copy_len);
                buf[copy_len] = '\0';
                buffer.clear();
                return copy_len;
            }
            return n; // 错误或正常关闭
        }
        
        buffer.append(temp, n);
        pos = buffer.find(CMD_DELIMITER);
        if (pos != std::string::npos) {
            return RecvCommand(client_fd, buf, max_len); // 递归处理
        }
    }
}
void Coordinator::RunServer() {
    while (true) {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        
        int client_fd = accept(listen_fd_, 
                             (sockaddr*)&client_addr,
                             &addr_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error("accept() failed");
        }

        // 使用线程池处理连接（简化为新线程）
        std::thread([this, client_fd] {
            try {
                HandleConnection(client_fd);
            } catch (const std::exception& e) {
                std::cerr << "Error handling connection: " << e.what() << std::endl;
            }
        }).detach();
    }
}

void Coordinator::HandleConnection(int client_fd) {
    
    char cmd_buf[1024*1024]; // 1MB buffer
    // Receive command from client
    // client_fd是客户端连接的文件描述符
    // 使用socket_对象来接收数据
    int received = RecvCommand(client_fd, cmd_buf, sizeof(cmd_buf));

    std::string cmd(cmd_buf, received);
    
    // Parse the command
    std::istringstream iss(cmd);
    std::string operation, key, value;
    iss >> operation >> key;
    if (operation == "SET") {
        std::getline(iss >> std::ws, value); // 读取剩余的值
        if (value.empty()) {
            std::cerr << "Value is empty for SET operation" << std::endl;
            close(client_fd);
            return;
        }
        // Process SET command
        std::cout << "Received SET command: " << key << " = " << value << std::endl;
        ProcessSet(client_fd, key, value);
    } else if (operation == "GET") {
        ProcessGet(client_fd, key);
    } else if (operation == "REPAIR"){
        std::vector<int> key_vector(key.begin(), key.end());
        ProcessRepair(client_fd, key_vector);
    } else{
        std::cerr << "Unknown command: " << operation << std::endl;
        
    }

}

void Coordinator::SetEcschema(ECSchema ecschema) {
    ecschema_ = ecschema;
}

ECSchema Coordinator::GetEcschema() {
    return ecschema_;
}

void Coordinator::SetObjectList() {
    object_list_.clear();
}

vector<ObjectItem> Coordinator::GetObjectList() {
    return object_list_;
}

void Coordinator::SetBlockList() {
    block_list_.clear();
}

vector<BlockItem> Coordinator::GetBlockList() {
    return block_list_;
}

void Coordinator::SetStripeList() {
    stripe_list_.clear();
    for (int  i = 0; i < (ecschema_.k_datablock+ecschema_.r_gobalparityblock+ecschema_.p_localgroup); i++)
    {
        /* code */
    }
    
}

vector<StripeItem> Coordinator::GetStripeList() {
    return stripe_list_;
}

void Coordinator::SetNodeList(int k,int r,int p) {
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
        cout << "Node_ " << i << "_id: "<<node.Node_id<<" _ip: " << node.Node_ip << " _port:" << node.Node_port <<"\n"<< endl;
    }
    
}

vector<NodeItem> Coordinator::GetNodeList() {
    return node_list_;
}

bool Coordinator::ProcessSet(int fd, const std::string& key, const std::string& value) {
    size_t value_len = value.size();
    size_t max_boundery = GetMaxStripeBoundary();
    ObjectItem object;
    StripeItem stripe;
    BlockItem block;
    // Check if the value fits in the current buffer node
    
    if (value_len+buffer_node_.current_size <= max_stripe_boundary_) {
        
        // Store the value in buffer_node
       
        // 修改object元数据
        object.key = key;
        object.stripe_id = buffer_node_.stripe.Stripe_id;
        object.block_idx.push_back(buffer_node_.current_size/block_size) ;
        object.offset.push_back(buffer_node_.current_size % block_size);
        object.status = 1;
        object.object_size = value_len;
        object_list_.push_back(object);
        //修改stripe元数据
        stripe.Stripe_id = buffer_node_.stripe.Stripe_id;
        stripe.block_size = buffer_node_.stripe.block_size;
        stripe.k = buffer_node_.stripe.k;
        stripe.r = buffer_node_.stripe.r;
        stripe.p = buffer_node_.stripe.p;
        stripe.encodetype = buffer_node_.stripe.encodetype;
        stripe.nodes = buffer_node_.stripe.nodes;
        stripe_list_.push_back(stripe);
        //修改block元数据
        block.block_id = buffer_node_.stripe.Stripe_id+buffer_node_.current_size/block_size;
        block.objects.push_back(object);
        block_list_.push_back(block);
        //修改buffer_node
        buffer_node_.current_size= buffer_node_.current_size + value_len;
        buffer_node_.data.append(value);
        cout << "buffer_node_.current_size: " << buffer_node_.current_size << endl;
    }else{
        // Buffer is full, need to encode and create a new stripe
        // Check if buffer_node_.current_size is less than max_stripe_boundary_
        if (buffer_node_.current_size < max_stripe_boundary_) {
            size_t padding_size = max_stripe_boundary_ - buffer_node_.current_size;
            buffer_node_.data.append(padding_size, '\0'); // Pad with zeros
        }
         
        std::vector<char*> data_ptrs;
        std::vector<char*> global_coding_ptrs;
        std::vector<char*> local_coding_ptrs;
        // Divide buffer_node_.data into k blocks and assign to data_ptrs
        int k = buffer_node_.stripe.k;
        data_ptrs.resize(k);
        for (int  i = 0; i < k; ++i) {
            data_ptrs[i] = new char[block_size];
            size_t start_pos = i * block_size;
            size_t copy_len = block_size;
            std::memcpy(data_ptrs[i], buffer_node_.data.data() + start_pos, copy_len);
        } 
        // Encode the data using the encoding scheme
        if (ecschema_.encodetype == "new_lrc")
        {
            NewLRC new_lrc;
            new_lrc.new_encode(ecschema_.k_datablock, ecschema_.r_gobalparityblock, ecschema_.p_localgroup, data_ptrs.data(), global_coding_ptrs.data(), local_coding_ptrs.data(), block_size); 
        }
        
        // 2. send data to datanode
        // Send data to datanodes
        for (int i = 0; i < k; ++i) {
            int datanode_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (datanode_fd < 0) {
            throw std::runtime_error("Failed to create socket for datanode");
            }

            sockaddr_in datanode_addr{};
            datanode_addr.sin_family = AF_INET;
            datanode_addr.sin_port = htons(DATANODE_PORT_BASE + i);
            inet_pton(AF_INET, "127.0.0.1", &datanode_addr.sin_addr);

            if (connect(datanode_fd, (sockaddr*)&datanode_addr, sizeof(datanode_addr)) < 0) {
            close(datanode_fd);
            throw std::runtime_error("Failed to connect to datanode");
            }

            // Send data block to datanode
            if (send(datanode_fd, data_ptrs[i], block_size, 0) < 0) {
            close(datanode_fd);
            throw std::runtime_error("Failed to send data to datanode");
            }

            close(datanode_fd);
        }

        // Send global parity blocks to datanodes
        for (int i = 0; i < ecschema_.r_gobalparityblock; ++i) {
            int datanode_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (datanode_fd < 0) {
            throw std::runtime_error("Failed to create socket for datanode");
            }

            sockaddr_in datanode_addr{};
            datanode_addr.sin_family = AF_INET;
            datanode_addr.sin_port = htons(DATANODE_PORT_BASE + k + i);
            inet_pton(AF_INET, "127.0.0.1", &datanode_addr.sin_addr);

            if (connect(datanode_fd, (sockaddr*)&datanode_addr, sizeof(datanode_addr)) < 0) {
            close(datanode_fd);
            throw std::runtime_error("Failed to connect to datanode");
            }

            // Send global parity block to datanode
            if (send(datanode_fd, global_coding_ptrs[i], block_size, 0) < 0) {
            close(datanode_fd);
            throw std::runtime_error("Failed to send global parity block to datanode");
            }

            close(datanode_fd);
        }

        // Send local parity blocks to datanodes
        for (int i = 0; i < ecschema_.p_localgroup; ++i) {
            int datanode_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (datanode_fd < 0) {
            throw std::runtime_error("Failed to create socket for datanode");
            }

            sockaddr_in datanode_addr{};
            datanode_addr.sin_family = AF_INET;
            datanode_addr.sin_port = htons(DATANODE_PORT_BASE + k + ecschema_.r_gobalparityblock + i);
            inet_pton(AF_INET, "127.0.0.1", &datanode_addr.sin_addr);

            if (connect(datanode_fd, (sockaddr*)&datanode_addr, sizeof(datanode_addr)) < 0) {
            close(datanode_fd);
            throw std::runtime_error("Failed to connect to datanode");
            }

            // Send local parity block to datanode
            if (send(datanode_fd, local_coding_ptrs[i], block_size, 0) < 0) {
            close(datanode_fd);
            throw std::runtime_error("Failed to send local parity block to datanode");
            }

            close(datanode_fd);
        }

        // 3.Reset buffer_node and create a new stripe
        buffer_node_.stripe.Stripe_id++;
        buffer_node_.data.clear();
        buffer_node_.current_size = 0; 
        
    }
   

}
void Coordinator::SetBufferNode(int block_size) {
    buffer_node_.stripe.Stripe_id = 0;
    buffer_node_.stripe.block_size = block_size;
    buffer_node_.stripe.k = ecschema_.k_datablock;
    buffer_node_.stripe.r = ecschema_.r_gobalparityblock;
    buffer_node_.stripe.p = ecschema_.p_localgroup;
    buffer_node_.stripe.encodetype = ecschema_.encodetype;
    // Set node list
    vector<NodeItem>tempnode_list = GetNodeList();
    for (int i = 0; i < (ecschema_.k_datablock+ecschema_.r_gobalparityblock+ecschema_.p_localgroup); i++)
    {
        buffer_node_.stripe.nodes.push_back(tempnode_list[i].Node_id);
        cout << "buffer_Node_ " << i << "_id: "<<tempnode_list[i].Node_id<<" _ip: " << tempnode_list[i].Node_ip << " _port:" << tempnode_list[i].Node_port<<"\n" << endl;
    }
    
    buffer_node_.current_size = 0;
}

int main() {
    // Initialize Coordinator
    ECSchema ecschema ;
    ClientServer::Coordinator coordinator;
    //输入编码方案
    cout<<"Please input the encoding scheme: "<<"including EncodeType(azure_lrc,azure_lrc_1,optimal,uniform,new_lrc),k_datablock ,p_localgroup, r_gobalparityblock,blocksize\n"
        <<"for example: azure_lrc 4 2 2 1048576\n"<<endl;
    cin >> ecschema.encodetype;
    cin >> ecschema.k_datablock;
    cin >> ecschema.p_localgroup;
    cin >> ecschema.r_gobalparityblock;
    cin >> coordinator.block_size;
    // Set encoding scheme
    coordinator.SetEcschema(ecschema);
    // Set max stripe boundary
    coordinator.SetMaxStripeBoundary(ecschema);
    // Set object, block, stripe, and node lists
    coordinator.SetObjectList();
    coordinator.SetBlockList(); 
    //为了后续stripe.nodes的初始化
    // Set node list需在setbuffernode之前
    coordinator.SetNodeList(ecschema.k_datablock,ecschema.r_gobalparityblock,ecschema.p_localgroup);
    // Set buffer node
    coordinator.SetBufferNode(coordinator.block_size);
    coordinator.SetStripeList();

    // Print max stripe boundary
    std::cout << "Max stripe boundary: " << coordinator.GetMaxStripeBoundary() << " bytes" << std::endl;
    // Start listening for connections
    coordinator.StartListen();
    std::cout << "Coordinator started, waiting for connections..." << std::endl;
    // Run the server
    coordinator.RunServer();
    return 0;
}










