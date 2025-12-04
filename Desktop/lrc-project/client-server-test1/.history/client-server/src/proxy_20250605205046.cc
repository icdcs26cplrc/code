#include "../include/proxy.hh"
using namespace ClientServer;

Proxy::Proxy()
{
    // Initialize the Proxy
    sock_proxy = -1;
}
void Proxy::StartProxy()
{
    cout << "Start Proxy Listen" << endl;
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
    server_addr.sin_addr.s_addr = inet_addr(PROXY_IP);
    server_addr.sin_port = htons(PROXY_PORT); // 绑定到目标端口
    if (::bind(sock_proxy, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        close(sock_proxy);
        throw std::runtime_error("bind failed: " + std::string(strerror(errno)));
        return;
    }

    RunServer();
}

void Proxy::StartProxy_Data()
{
    cout << "Start Proxy_DATA Listen" << endl;
    sock_proxy_data = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_proxy_data < 0)
    {
        std::cerr << "Error: Unable to create socket" << std::endl;
        return;
    }
    // 设置 SO_REUSEADDR 以允许端口重用
    int opt = 1;
    if (setsockopt(sock_proxy_data, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        close(sock_proxy_data);
        throw std::runtime_error("setsockopt() failed: " + std::string(strerror(errno)));
    }

    sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(PROXY_IP);
    server_addr.sin_port = htons(PROXY_PORT_DATA); // 绑定到目标端口
    if (::bind(sock_proxy_data, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        close(sock_proxy_data);
        throw std::runtime_error("bind failed: " + std::string(strerror(errno)));
        return;
    }
    if (listen(sock_proxy_data, 100) < 0)
    {
        close(sock_proxy_data);
        throw std::runtime_error("listen failed: " + std::string(strerror(errno)));
    }
}

ssize_t Proxy::RecvCommand(int client_fd, char *buf, size_t max_len)
{
    ssize_t total_received = 0;
    total_received = read(client_fd, buf, max_len);
    // cout << "total_received: " << total_received << endl;
    return total_received;
}
void Proxy::RunServer()
{
    if (listen(sock_proxy, 100) < 0)
    {
        close(sock_proxy);
        throw std::runtime_error("listen failed");
    }
    while (true)
    {
        // sockaddr_in client_addr{};
        // socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(sock_proxy, nullptr, nullptr);
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

void Proxy::HandleConnection(int client_fd)
{

    // std::cout << "Start revData" << endl;
    char cmd_buf[200];

    ssize_t received = RecvCommand(client_fd, cmd_buf, sizeof(cmd_buf));
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
    std::string operation, value;
    iss >> operation;
    if (operation == "SET")
    {
        value.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        // Process SET command
        // std::cout << "Received SET command: " << value.size() << std::endl;
        close(client_fd);
        ProcessSet_Proxy(value);
    }
    else if (operation == "GET")
    {
        value.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        close(client_fd);
        ProcessGet_Proxy(value);
    }
    else if (operation == "REPAIR")
    {
        value.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        ProcessRepair_Client(value);
    }
    else
    {
        std::cerr << "Unknown command: " << operation << std::endl;
        close(client_fd);
    }
}

void Proxy::ProcessSet_Proxy(std::string value)
{
    // parse the value
    std::istringstream iss(value);
    std::string command, data_length, encode_type;
    iss >> command >> data_length;
    int length = get_int_from_string(data_length);
    // size_t recv;
    char **data = new char *[1];
    data[0] = new char[length];
    size_t *value_len_ = new size_t[1];
    value_len_[0] = length;
    if (command == "Store_in_buffer")
    {
        // send ack to client
        string massage = "SET_ACK";
        SendMassgeToD("client", massage);
        // Extract the numeric part from data_length
        // recv = RecvClientData(data, length);
        size_t *recv_len;
        GetRevData(data, 1, value_len_, recv_len);
        // cout << "before recv buffer_data size : " << buffer_data.size() << endl;
        buffer_data.append(data[0] + buffer_data.size(), value_len_[0]);
        cout << "after recv buffer_data size : " << buffer_data.size() << endl;
    }
    else if (command == "Encode")
    {
        encode_type.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>()); // 读取剩余的值
        std::istringstream iss(encode_type);
        string encode_type_str, encode_info;
        iss >> encode_type_str >> encode_info;
        // std::cout << "encode_type_str: " << encode_type_str << std::endl;
        // std::cout << "encode_info: " << encode_info << std::endl;

        int k = 0, r = 0, p = 0;
        size_t blocksize = 0;
        size_t pos1 = encode_info.find('_');
        size_t pos2 = encode_info.find('_', pos1 + 1);
        size_t pos3 = encode_info.find('_', pos2 + 1);
        if (pos1 != std::string::npos && pos2 != std::string::npos && pos3 != std::string::npos)
        {
            try
            {
                k = std::stoi(encode_info.substr(0, pos1));
                r = std::stoi(encode_info.substr(pos1 + 1, pos2 - pos1 - 1));
                p = std::stoi(encode_info.substr(pos2 + 1, pos3 - pos2 - 1));
                blocksize = std::stoul(encode_info.substr(pos3 + 1));
            }
            catch (const std::exception &e)
            {
                std::cerr << "Error parsing encode_info: " << e.what() << std::endl;
            }
        }
        else
        {
            std::cerr << "Invalid encode_info format: " << encode_info << std::endl;
        }
        if (buffer_data.size() < k * blocksize)
        {
            size_t padding_size = k * blocksize - buffer_data.size();
            buffer_data.append(padding_size, '\0'); // Pad with zeros
        }
        char **global_ptr = new char *[r];
        char **local_ptr = new char *[p];
        char **data_ptr = new char *[k];
        int num = k + r + p;
        for (int i = 0; i < num; i++)
        {
            if (i < k)
            {
                data_ptr[i] = new char[blocksize];
                size_t start_pos = i * blocksize;
                size_t copy_len = blocksize;
                std::string data = buffer_data.substr(start_pos, copy_len);
                // cout<<"before encoding data_ptrs ["<<i<<"]"<<data.size()<<endl;
                std::memcpy(data_ptr[i], data.data(), copy_len);
                // cout<<" encoding data_ptrs ["<<i<<"]"<<std::string(data_ptrs[i],block_size).size()<<endl;
            }
            else if (i >= k && i < (k + r))
            {
                global_ptr[i - k] = new char[blocksize];
            }
            else if (i >= (k + r) && i < (k + r + p))
            {
                local_ptr[i - k - r] = new char[blocksize];
            }
        }

        if (encode_type_str == "new_lrc")
        {
            NewLRC newlrc(k, r, p, blocksize);
            newlrc.encode(data_ptr, global_ptr, local_ptr);
        }
        else if (encode_type_str == "azure")
        {
            AZURE_LRC azure(k, r, p, blocksize);
            azure.encode(data_ptr, global_ptr, local_ptr);
        }
        else if (encode_type_str == "azure_1")
        {
            AZURE_LRC_1 azure_1(k, r, p, blocksize);
            azure_1.encode(data_ptr, global_ptr, local_ptr);
        }
        else if (encode_type_str == "optimal")
        {
            OPTIMAL_LRC optimal(k, r, p, blocksize);
            optimal.encode(data_ptr, global_ptr, local_ptr);
        }
        else if (encode_type_str == "uniform")
        {
            UNIFORM_LRC uniform(k, r, p, blocksize);
            uniform.encode(data_ptr, global_ptr, local_ptr);
        }
        else if (encode_type_str == "unbalanced")
        {
            cout << "unbalanced encoding" << endl;
            UNBALANCED_LRC unbalanced(k, r, p, blocksize);
            unbalanced.encode(data_ptr, global_ptr, local_ptr);
        }

        std::vector<std::thread> threads;
        for (int i = 0; i < num; i++)
        {
            if (i < k)
            {
                threads.emplace_back([this, i, data_ptr, blocksize]()
                                     {
                    int dataport_ = DATANODE_PORT_BASE + i;
                    int node_id = i;
                    SendBlockToDatanode(dataport_, node_id, data_ptr[i],blocksize); });
            }
            else if (i >= k && i < k + r)
            {
                threads.emplace_back([this, i, global_ptr, k, blocksize]()
                                     {
                    int dataport_ = DATANODE_PORT_BASE + i;
                    int node_id = i;
                    SendBlockToDatanode(dataport_, node_id, global_ptr[i-k],blocksize); });
            }
            else
            {
                threads.emplace_back([this, i, local_ptr, k, r, blocksize]()
                                     {
                    int dataport_ = DATANODE_PORT_BASE + i;
                    int node_id = i;
                    SendBlockToDatanode(dataport_, node_id, local_ptr[i-k-r],blocksize); });
            }
        }

        // Wait for all threads to complete
        for (auto &thread : threads)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
        // send ack to client
        string massage = "SET_ACK";
        buffer_data.clear();
        SendMassgeToD("client", massage);
        size_t *recv_len;
        GetRevData(data, 1, value_len_, recv_len);
        buffer_data.append(data[0] + buffer_data.size(), value_len_[0]);
        cout << "buffer_data size : " << buffer_data.size() << endl;
        // delete[] data_ptr;
        // delete[] global_ptr;
        // delete[] local_ptr;
    }
    else
    {
        cout << "INVALID COMMAND FROM COR" << endl;
    }
}

void Proxy::ProcessGet_Proxy(std::string value)
{
    // parse the value
    std::istringstream iss(value);
    std::string command, data_length, get_message, get_info;
    iss >> command >> data_length >> get_message;
    size_t value_len = get_int_from_string(data_length);
    if (command == "proxy")
    {
        size_t start_pos = get_int_from_string(get_message);
        string value = buffer_data.substr(start_pos, value_len);
        SendMassgeToD("client", value);
    }
    else if (command == "datanode")
    {
        get_info.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>());
        int num = get_int_from_string(get_message);
        int *node_id = new int[num];
        int *offset = new int[num];
        size_t *value_len_ = new size_t[num];

        std::istringstream get_info_stream(get_info);
        string stripe_id_, get_info_stream_;
        get_info_stream >> stripe_id_;
        int stripe_id = get_int_from_string(stripe_id_);
        get_info_stream_.assign(std::istreambuf_iterator<char>(get_info_stream >> std::ws), std::istreambuf_iterator<char>());
        std::istringstream get_info_(get_info_stream_);
        for (int i = 0; i < num; ++i)
        {
            std::string node_id_str, offset_str, valuelen_str;
            get_info_ >> node_id_str >> offset_str >> valuelen_str;

            if (node_id_str.find("node_id_") == 0)
            {
                node_id[i] = std::stoi(node_id_str.substr(8));
            }
            if (offset_str.find("offset_") == 0)
            {
                offset[i] = std::stoi(offset_str.substr(7));
            }
            if (valuelen_str.find("split_value_len_") == 0)
            {
                value_len_[i] = std::stoi(valuelen_str.substr(16));
            }
            // cout << "get data from " << node_id[i] << " offset : " << offset[i] << " value_len : " << value_len_[i] << endl;
        }
        vector<std::thread> threads;
        int *get_requst_return = new int[num];
        for (int i = 0; i < num; i++)
        {
            threads.emplace_back([this, i, node_id, offset, value_len_, get_requst_return]()
                                 { get_requst_return[i] = SendGetRequest(node_id[i], offset[i], value_len_[i]); });
        }
        // Wait for all threads to complete
        for (auto &thread : threads)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
        // Receive the data from the datanode
        char **data_get = new char *[num];
        size_t *recv_len;
        GetRevData(data_get, num, value_len_, recv_len);
        // cout << "GetRevData done" << endl;
        // Extract and sort node_ids
        std::vector<std::pair<int, int>> node_id_index_pairs; // Pair of node_id and its index
        for (int i = 0; i < num; ++i)
        {
            std::string data_str(data_get[i]);
            size_t pos = data_str.find("node_id_");
            if (pos != std::string::npos)
            {
                try
                {
                    int node_id = std::stoi(data_str.substr(pos + 8)); // Extract the integer after "node_id_"
                    node_id_index_pairs.emplace_back(node_id, i);      // Store node_id and its index
                }
                catch (const std::exception &e)
                {
                    std::cerr << "Error parsing node_id: " << e.what() << std::endl;
                }
            }
            else
            {
                std::cerr << "node_id_ not found in data: " << data_str << std::endl;
            }
        }

        // Sort by node_id
        std::sort(node_id_index_pairs.begin(), node_id_index_pairs.end());

        // Rearrange data_get based on sorted node_ids
        char **sorted_data_get = new char *[num];
        for (int i = 0; i < num; ++i)
        {
            int original_index = node_id_index_pairs[i].second;
            sorted_data_get[i] = data_get[original_index];
        }

        // Remove "node_id_" from sorted data_get
        for (int i = 0; i < num; ++i)
        {
            std::string data_str(sorted_data_get[i]);
            size_t pos = data_str.find("node_id_");
            if (pos != std::string::npos)
            {
                size_t end_pos = pos + 8 + std::to_string(node_id_index_pairs[i].first).size();                  // Calculate the end position of "node_id_<int>"
                memmove(sorted_data_get[i], sorted_data_get[i] + end_pos, strlen(sorted_data_get[i]) - end_pos); // Shift the remaining data
                sorted_data_get[i][strlen(sorted_data_get[i]) - end_pos] = '\0';                                 // Null-terminate the string
            }
        }

        // Update original data_get with sorted and modified values
        for (int i = 0; i < num; ++i)
        {
            data_get[i] = sorted_data_get[i];
        }

        delete[] sorted_data_get;
        // 检查get value是否完整
        int *error_id;
        int fail_num = check_error(num, recv_len, value_len_, node_id, error_id);
        // Combine the received data into a single value
        string value_;
        if (fail_num == 0)
        {
            for (int i = 0; i < num; i++)
            {
                // cout << "data[" << i << "] size: " << strlen(data[i])<< endl;
                value_.append(data_get[i], value_len_[i]);
            }
        }
        else
        {
            // degrade read
            // send fail message to cor
            string fail_info;
            for (int i = 0; i < fail_num; i++)
            {
                string fail_mesg = "stripe_" + to_string(stripe_id) + "_block_" + to_string(error_id[i]) + " ";
                fail_info.append(fail_mesg);
            }
            string fail_value = "RAPAIR proxy fail_number_" + to_string(fail_num) + " " + fail_info;
            SendMassgeToD("coordinator", fail_value);
            // 接收来自cor的信息
            char *repair_message = nullptr;
            size_t recv_length = 200;
            size_t *actual_length;
            GetRevData(&repair_message, 1, &recv_length, actual_length);
            char **repair_get_value;
            Repair_degrade(repair_message, num, node_id, offset, value_len_, value_);
            // size_t start_repair = node_id[0]*offset[0];
            // size_t repair_valuelen = 0;
            // for (int i = 0; i < num; i++)
            // {
            //     repair_valuelen =repair_valuelen + value_len_[i];
            // }
            // std::string repair_get_value_str(repair_get_value[0]);
            // std::string value_ = repair_get_value_str.substr(start_repair, repair_valuelen);
        }

        // Send the value to the client
        SendMassgeToD("client", value_);
    }
    else
    {
        cout << "INVALID GET COMMAND" << endl;
    }
}

void Proxy::Repair_degrade(string message, int value_get_num, int *node_id, int *offset_value, size_t *repair_value_len, string &value)
{
    // parse value
    istringstream iss(message);
    string command, get_num, decode_type, decode_info, offset_str, fail_num, fail_id, node_need;
    iss >> command >> get_num >> decode_type >> decode_info >> offset_str >> fail_num >> fail_id;
    node_need.assign(istreambuf_iterator<char>(iss >> std::ws), istreambuf_iterator<char>());

    int num, fail_number, k, r, p, blocksize;
    num = get_int_from_string(get_num);
    fail_number = get_int_from_string(fail_num);
    int *node_need_id = new int[num];
    int *decode_info_ = new int[4];
    int *fail_node = new int[fail_number];

    get_int_from_muti_(decode_info, decode_info_, 4);
    get_int_from_muti_(fail_id, fail_node, fail_number);

    k = decode_info_[0];
    r = decode_info_[1];
    p = decode_info_[2];
    blocksize = decode_info_[3];
    size_t offset_ = get_int_from_string(offset_str);

    istringstream iss_node(node_need);
    for (int i = 0; i < num; i++)
    {
        string node_id_str;
        iss_node >> node_id_str;

        if (node_id_str.find("blockneed_") == 0)
        {
            node_need_id[i] = std::stoi(node_id_str.substr(10));
        }
        cout << "node_need_id[" << i << "]" << node_need_id[i] << endl;
    }
    // send get request
    vector<std::thread> threads;
    for (int i = 0; i < num; i++)
    {
        threads.emplace_back([this, i, node_need_id, offset_, blocksize]()
                             { SendGetRequest(node_need_id[i], offset_, blocksize); });
    }
    for (auto &thread : threads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }
    // recv data
    char **recv_data = new char *[num];
    std::vector<size_t> value_len(num, blocksize);
    size_t *recv_len;
    GetRevData(recv_data, num, value_len.data(), recv_len);
    // 对recv_data 重新排序
    //  Extract and sort node_ids
    std::vector<std::pair<int, int>> node_id_index_pairs; // Pair of node_id and its index
    for (int i = 0; i < num; ++i)
    {
        std::string data_str(recv_data[i]);
        size_t pos = data_str.find("node_id_");
        if (pos != std::string::npos)
        {
            try
            {
                int node_id = std::stoi(data_str.substr(pos + 8)); // Extract the integer after "node_id_"
                node_id_index_pairs.emplace_back(node_id, i);      // Store node_id and its index
            }
            catch (const std::exception &e)
            {
                std::cerr << "Error parsing node_id: " << e.what() << std::endl;
            }
        }
        else
        {
            std::cerr << "node_id_ not found in data: " << data_str << std::endl;
        }
    }

    // Sort by node_id
    std::sort(node_id_index_pairs.begin(), node_id_index_pairs.end());

    // Rearrange data_get based on sorted node_ids
    char **sorted_data_get = new char *[num];
    for (int i = 0; i < num; ++i)
    {
        int original_index = node_id_index_pairs[i].second;
        sorted_data_get[i] = recv_data[original_index];
    }

    // Remove "node_id_" from sorted data_get
    for (int i = 0; i < num; ++i)
    {
        std::string data_str(sorted_data_get[i]);
        size_t pos = data_str.find("node_id_");
        if (pos != std::string::npos)
        {
            size_t end_pos = pos + 8 + std::to_string(node_id_index_pairs[i].first).size();                  // Calculate the end position of "node_id_<int>"
            memmove(sorted_data_get[i], sorted_data_get[i] + end_pos, strlen(sorted_data_get[i]) - end_pos); // Shift the remaining data
            sorted_data_get[i][strlen(sorted_data_get[i]) - end_pos] = '\0';                                 // Null-terminate the string
        }
    }

    // Update original data_get with sorted and modified values
    for (int i = 0; i < num; ++i)
    {
        recv_data[i] = sorted_data_get[i];
    }

    delete[] sorted_data_get;

    char **data_ptrs = new char *[k];
    char **global_parity = new char *[r];
    char **local_parity = new char *[p];
    int sum = k + r + p;
    int pos = 0;
    for (int i = 0; i < sum; i++)
    {
        if (i != fail_node[i] && pos < num)
        {
            if (i < k)
            {
                data_ptrs[i] = new char[blocksize];
                data_ptrs[i] = recv_data[pos];
                pos++;
            }
            else if (i >= k && i < k + r)
            {
                global_parity[i - k] = new char[blocksize];
                global_parity[i - k] = recv_data[pos];
                pos++;
            }
            else
            {
                local_parity[i - k - r] = new char[blocksize];
                local_parity[i - k - r] = recv_data[pos];
                pos++;
            }
        }
        else
        {
            if (i < k)
            {
                data_ptrs[i] = new char[blocksize];
            }
            else if (i >= k && i < k + r)
            {
                global_parity[i - k] = new char[blocksize];
            }
            else
            {
                local_parity[i - k - r] = new char[blocksize];
            }
        }
    }
    bool repair_ok = false;
    if (decode_type == "new_lrc")
    {
        NewLRC newlrc(k, r, p, blocksize);
        repair_ok = newlrc.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }
    else if (decode_type == "azure")
    {
        AZURE_LRC azure(k, r, p, blocksize);
        repair_ok = azure.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }
    else if (decode_type == "azure_1")
    {
        AZURE_LRC_1 azure_1(k, r, p, blocksize);
        repair_ok = azure_1.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }
    else if (decode_type == "optimal")
    {
        OPTIMAL_LRC optimal(k, r, p, blocksize);
        repair_ok = optimal.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }
    else if (decode_type == "uniform")
    {
        UNIFORM_LRC uniform(k, r, p, blocksize);
        repair_ok = uniform.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }
    else if (decode_type == "unbalanced")
    {
        UNBALANCED_LRC unbalanced(k, r, p, blocksize);
        repair_ok = unbalanced.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }
    // print
    if (repair_ok)
    {
        cout << "sucessfully repair" << endl;
    }
    else
    {
        cout << "repair fail" << endl;
    }
    // 赋值
    for (int i = 0; i < value_get_num; i++)
    {
        char *temp = data_ptrs[node_id[i]];
        value.append(temp + offset_value[i], repair_value_len[i]);
    }
}

void Proxy::ProcessRepair_Client(string value)
{
    // parse value
    istringstream iss(value);
    string command, get_num, decode_type, decode_info, offset_str, fail_num, fail_id, node_need;
    iss >> get_num >> decode_type >> decode_info >> offset_str >> fail_num >> fail_id;
    node_need.assign(istreambuf_iterator<char>(iss >> std::ws), istreambuf_iterator<char>());

    int num, fail_number, k, r, p, blocksize;
    num = get_int_from_string(get_num);
    fail_number = get_int_from_string(fail_num);
    int *node_need_id = new int[num];
    int *decode_info_ = new int[4];
    int *fail_node = new int[fail_number];

    get_int_from_muti_(decode_info, decode_info_, 4);
    get_int_from_muti_(fail_id, fail_node, fail_number);

    k = decode_info_[0];
    r = decode_info_[1];
    p = decode_info_[2];
    blocksize = decode_info_[3];
    size_t offset_ = get_int_from_string(offset_str);
    // cout<<"fail : "<<fail_node[0]<<endl;
    istringstream iss_node(node_need);
    for (int i = 0; i < num; i++)
    {
        string node_id_str;
        iss_node >> node_id_str;

        if (node_id_str.find("blockneed_") == 0)
        {
            node_need_id[i] = std::stoi(node_id_str.substr(10));
        }
        cout << "node_need_id[" << i << "]" << node_need_id[i] << endl;
    }
    // send get request
    vector<std::thread> threads;
    int *get_request_return = new int[num];
    for (int i = 0; i < num; i++)
    {
        threads.emplace_back([this, i, node_need_id, offset_, blocksize, get_request_return]()
                             { get_request_return[i] = SendGetRequest(node_need_id[i], offset_, blocksize); });
    }
    for (auto &thread : threads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }
    // recv data
    char **recv_data = new char *[num];
    std::vector<size_t> value_len(num, blocksize);
    size_t *recv_len;
    GetRevData(recv_data, num, value_len.data(), recv_len);

    //     //对recv_data 重新排序
    // // Extract and sort node_ids
    // std::vector<std::pair<int, int>> node_id_index_pairs; // Pair of node_id and its index
    // for (int i = 0; i < num; ++i) {
    //     cout<<"before get data size:"<<strlen(recv_data[i])<<endl;
    //     std::string data_str(recv_data[i]);
    //     size_t pos = data_str.find("node_id_");
    //     if (pos != std::string::npos) {
    //     try {
    //         int node_id = std::stoi(data_str.substr(pos + 8)); // Extract the integer after "node_id_"
    //         node_id_index_pairs.emplace_back(node_id, i); // Store node_id and its index
    //     } catch (const std::exception &e) {
    //         std::cerr << "Error parsing node_id: " << e.what() << std::endl;
    //     }
    //     } else {
    //     std::cerr << "node_id_ not found in data: " << data_str << std::endl;
    //     }
    // }

    // // Sort by node_id
    // std::sort(node_id_index_pairs.begin(), node_id_index_pairs.end());

    // // Rearrange data_get based on sorted node_ids
    // char **sorted_data_get = new char *[num];
    // for (int i = 0; i < num; ++i) {
    //     int original_index = node_id_index_pairs[i].second;
    //     sorted_data_get[i] = recv_data[original_index];
    // }

    // // Remove "node_id_" from sorted data_get
    // for (int i = 0; i < num; ++i) {
    //     std::string data_str(sorted_data_get[i]);
    //     size_t pos = data_str.find("node_id_");
    //     if (pos != std::string::npos) {
    //     size_t end_pos = pos + 8 + std::to_string(node_id_index_pairs[i].first).size(); // Calculate the end position of "node_id_<int>"
    //     memmove(sorted_data_get[i], sorted_data_get[i] + end_pos, strlen(sorted_data_get[i]) - end_pos); // Shift the remaining data
    //     sorted_data_get[i][strlen(sorted_data_get[i]) - end_pos] = '\0'; // Null-terminate the string
    //     }
    // }

    // // Update original data_get with sorted and modified values
    // for (int i = 0; i < num; ++i) {
    //     recv_data[i] = sorted_data_get[i];
    //     cout<<"size of recv_data["<<i<<"] : "<<strlen(recv_data[i])<<endl;
    // }

    // delete[] sorted_data_get;

    // 对recv_data重新排序 - 修复版本
    std::vector<std::pair<int, int>> node_id_index_pairs;
    std::vector<size_t> original_lengths(num); // 保存原始长度

    for (int i = 0; i < num; ++i)
    {
        original_lengths[i] = recv_len[i]; // 保存实际接收长度

        // 查找node_id，但只在前面一小部分搜索（避免在二进制数据中误匹配）
        std::string header_str(recv_data[i], std::min((size_t)100, recv_len[i])); // 只搜索前100字节
        size_t pos = header_str.find("node_id_");

        if (pos != std::string::npos)
        {
            try
            {
                // 提取node_id
                size_t id_start = pos + 8;
                size_t id_end = header_str.find_first_not_of("0123456789", id_start);
                if (id_end == std::string::npos)
                    id_end = header_str.length();

                int node_id = std::stoi(header_str.substr(id_start, id_end - id_start));
                node_id_index_pairs.emplace_back(node_id, i);
            }
            catch (const std::exception &e)
            {
                std::cerr << "Error parsing node_id: " << e.what() << std::endl;
            }
        }
        else
        {
            std::cerr << "node_id_ not found in data header" << std::endl;
        }
    }

    // 按node_id排序
    std::sort(node_id_index_pairs.begin(), node_id_index_pairs.end());

    // 重新排列数据
    char **sorted_data_get = new char *[num];
    std::vector<size_t> sorted_lengths(num);

    for (int i = 0; i < num; ++i)
    {
        int original_index = node_id_index_pairs[i].second;
        sorted_lengths[i] = original_lengths[original_index];

        // 分配新内存并复制数据
        sorted_data_get[i] = new char[sorted_lengths[i]];
        memcpy(sorted_data_get[i], recv_data[original_index], sorted_lengths[i]);
    }

    // 从排序后的数据中移除node_id前缀
    for (int i = 0; i < num; ++i)
    {
        // 重新查找node_id位置
        std::string header_str(sorted_data_get[i], std::min((size_t)100, sorted_lengths[i]));
        size_t pos = header_str.find("node_id_");

        if (pos != std::string::npos)
        {
            // 计算要移除的前缀长度
            size_t id_start = pos + 8;
            size_t id_end = header_str.find_first_not_of("0123456789", id_start);
            if (id_end == std::string::npos)
                id_end = std::min((size_t)100, sorted_lengths[i]);

            size_t prefix_len = id_end;
            size_t remaining_len = sorted_lengths[i] - prefix_len;

            // 创建新的缓冲区存储去除前缀后的数据
            char *clean_data = new char[remaining_len];
            memcpy(clean_data, sorted_data_get[i] + prefix_len, remaining_len);

            // 释放旧内存，更新指针和长度
            delete[] sorted_data_get[i];
            sorted_data_get[i] = clean_data;
            sorted_lengths[i] = remaining_len;
        }

        cout << "size of cleaned data[" << i << "]: " << sorted_lengths[i] << endl;
    }

    // 更新原始数据
    for (int i = 0; i < num; ++i)
    {
        // 释放原始数据内存
        delete[] recv_data[i];
        recv_data[i] = sorted_data_get[i];
        recv_len[i] = sorted_lengths[i]; // 更新长度信息
    }

    delete[] sorted_data_get; // 注意：不要delete内容，因为指针已经转移

    char **data_ptrs = new char *[k];
    char **global_parity = new char *[r];
    char **local_parity = new char *[p];
    int sum = k + r + p;
    int pos = 0;
    for (int i = 0; i < sum; i++)
    {
        bool is_failed = false;

        // 检查是否是失效节点
        for (int j = 0; j < fail_number; j++)
        {
            if (i == fail_node[j])
            {
                is_failed = true;
                break;
            }
        }

        if (!is_failed && pos < num&& i == node_need_id[i])
        {
            if (i < k && i >= 0)
            {
                data_ptrs[i] = new char[blocksize];
                data_ptrs[i] = recv_data[pos];
                pos++;
                cout << "size of data_ptrs[" << i << "] : " << strlen(data_ptrs[i]) << endl;
            }
            else if (i >= k && i < k + r)
            {
                global_parity[i - k] = new char[blocksize];
                global_parity[i - k] = recv_data[pos];
                pos++;
                cout << "size of global[" << i - k << "] : " << strlen(global_parity[i - k]) << endl;
            }
            else if (i >= (k + r) && i < (k + r + p))
            {
                local_parity[i - k - r] = new char[blocksize];
                local_parity[i - k - r] = recv_data[pos];
                pos++;
                cout << "size of local_parity[" << i - k - r << "] : " << strlen(local_parity[i - k - r]) << endl;
            }
        }
        else if (is_failed)
        {
            if (i < k)
            {
                data_ptrs[i] = new char[blocksize];
            }
            else if (i >= k && i < k + r)
            {
                global_parity[i - k] = new char[blocksize];
            }
            else
            {
                local_parity[i - k - r] = new char[blocksize];
            }
        }
    }
    bool repair_ok = false;
    if (decode_type == "new_lrc")
    {
        NewLRC newlrc(k, r, p, blocksize);
        repair_ok = newlrc.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }
    else if (decode_type == "azure")
    {
        AZURE_LRC azure(k, r, p, blocksize);
        repair_ok = azure.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }
    else if (decode_type == "azure_1")
    {
        AZURE_LRC_1 azure_1(k, r, p, blocksize);
        repair_ok = azure_1.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }
    else if (decode_type == "optimal")
    {
        OPTIMAL_LRC optimal(k, r, p, blocksize);
        repair_ok = optimal.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }
    else if (decode_type == "uniform")
    {
        UNIFORM_LRC uniform(k, r, p, blocksize);
        repair_ok = uniform.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }
    else if (decode_type == "unbalanced")
    {
        UNBALANCED_LRC unbalanced(k, r, p, blocksize);
        repair_ok = unbalanced.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }

    // print
    if (repair_ok)
    {
        cout << "sucessfully repair" << endl;
    }
    else
    {
        cout << "repair fail" << endl;
    }
}

void Proxy::get_int_from_muti_(string data, int *&get, int sum)
{
    std::istringstream iss(data);
    for (int i = 0; i < sum; ++i)
    {
        std::string token;
        if (i < (sum - 1))
        {
            if (std::getline(iss, token, '_'))
            {
                try
                {
                    get[i] = std::stoi(token);
                }
                catch (const std::exception &e)
                {
                    std::cerr << "Error parsing integer from string: " << e.what() << std::endl;
                    get[i] = 0; // Assign a default value in case of error
                }
            }
            else
            {
                std::cerr << "Error: Insufficient data to parse" << std::endl;
                get[i] = 0; // Assign a default value in case of error
            }
        }
        else if (i == (sum - 1))
        {
            if (std::getline(iss, token))
            {
                try
                {
                    get[sum - 1] = std::stoi(token);
                }
                catch (const std::exception &e)
                {
                    std::cerr << "Error parsing last integer from string: " << e.what() << std::endl;
                    get[sum - 1] = 0; // Assign a default value in case of error
                }
            }
        }
    }
}

int Proxy::check_error(int num, size_t *value_len, size_t *recv_len, int *node_id, int *&error_id)
{
    int fail_num = 0;
    error_id = new int[num];
    for (int i = 0; i < num; i++)
    {
        if (recv_len[i] != value_len[i])
        {
            error_id[i] = node_id[i];
            fail_num++;
        }
    }
    return fail_num;
}

int Proxy::SendGetRequest(int node_id, int offset, size_t value_len)
{
    int datanode_port = DATANODE_PORT_BASE + node_id;
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
    {
        std::cerr << "Error: Unable to create socket" << std::endl;
        return -1;
    }
    struct sockaddr_in server_address;
    bzero(&server_address, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(datanode_port);
    const char *des_ip = "127.0.0.1";
    const char *datanode_ips[] = {
        DATANODE_IP_0, DATANODE_IP_1, DATANODE_IP_2, DATANODE_IP_3,
        DATANODE_IP_4, DATANODE_IP_5, DATANODE_IP_6, DATANODE_IP_7,
        DATANODE_IP_8, DATANODE_IP_9, DATANODE_IP_10, DATANODE_IP_11,
        DATANODE_IP_12, DATANODE_IP_13, DATANODE_IP_14};

    if (node_id >= 0 && node_id < sizeof(datanode_ips) / sizeof(datanode_ips[0]))
    {
        des_ip = datanode_ips[node_id];
    }
    else
    {
        std::cerr << "Invalid node_id: " << node_id << std::endl;
        return -1;
    }
    if (inet_aton(des_ip, &server_address.sin_addr) == 0)
    {
        cout << "dest ip: " << des_ip << endl;
        perror("inet_aton fail!");
    }
    // 建立连接 - 添加重试机制和错误处理
    int retry_count = 0;
    const int max_retries = 3;
    while (retry_count < max_retries)
    {
        if (connect(sock, (struct sockaddr *)&server_address, sizeof(server_address)) == 0)
        {
            break; // 连接成功
        }
        retry_count++;
        if (retry_count >= max_retries)
        {
            perror("Failed to connect after retries");
            close(sock);
            return -1;
        }
        usleep(100000); // 等待100ms后重试
    }

    // Prepare the message
    string message = "GET node_id_" + to_string(node_id) + " offset_" + to_string(offset) + " value_len_" + to_string(value_len) + "\n";
    // Send the message

    uint32_t data_length_net = htonl(message.size());
    if (write(sock, &data_length_net, sizeof(data_length_net)) != sizeof(data_length_net))
    {
        cerr << "Failed to send data length header" << endl;
        close(sock);
        return -1;
    }

    if (send(sock, message.c_str(), message.size(), 0) < 0)
    {
        std::cerr << "Error: Failed to send data" << std::endl;
        close(sock);
        return -1;
    }
    std::cout << "Sent GET request to datanode: " << node_id << std::endl;
    return 0;
    close(sock);
}

int Proxy::get_int_from_string(string data)
{
    int length = 0;
    size_t pos = data.find('_');
    if (pos != std::string::npos)
    {
        std::string length_str = data.substr(pos + 1);
        try
        {
            length = std::stoi(length_str);
            // std::cout << "Extracted data length: " << length << std::endl;
            //  Use the extracted length as needed
        }
        catch (const std::invalid_argument &e)
        {
            std::cerr << "Invalid data length format: " << data << std::endl;
        }
        catch (const std::out_of_range &e)
        {
            std::cerr << "Data length out of range: " << data << std::endl;
        }
    }
    else
    {
        std::cerr << "Invalid data length format: " << data << std::endl;
    }
    return length;
}

void Proxy::SendBlockToDatanode(int datanode_port, int node_id, const char *data, size_t block_size)
{
    Socket socket_datanode;
    std::string node_id_str = "node_id_" + std::to_string(node_id);
    std::string data_str(data, block_size); // 直接使用 data 和 block_size
    std::string buf_str = "SET " + node_id_str + " " + data_str;

    size_t chunk_size = buf_str.size();

    // 节点IP选择逻辑
    const char *datanode_ips[] = {
        DATANODE_IP_0, DATANODE_IP_1, DATANODE_IP_2, DATANODE_IP_3,
        DATANODE_IP_4, DATANODE_IP_5, DATANODE_IP_6, DATANODE_IP_7,
        DATANODE_IP_8, DATANODE_IP_9, DATANODE_IP_10, DATANODE_IP_11,
        DATANODE_IP_12, DATANODE_IP_13, DATANODE_IP_14};

    const char *des_ip = "127.0.0.1";
    if (node_id >= 0 && node_id < sizeof(datanode_ips) / sizeof(datanode_ips[0]))
    {
        des_ip = datanode_ips[node_id];
    }
    else
    {
        std::cerr << "Invalid node_id: " << node_id << std::endl;
        return;
    }

    // 创建socket
    int sock_data = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_data < 0)
    {
        cerr << "Error: Unable to create socket" << endl;
        return;
    }

    // 设置socket超时
    struct timeval timeout;
    timeout.tv_sec = 5; // 5秒超时
    timeout.tv_usec = 0;
    setsockopt(sock_data, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock_data, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    // 设置远程地址
    struct sockaddr_in remote_addr;
    bzero(&remote_addr, sizeof(remote_addr));
    remote_addr.sin_family = AF_INET;
    remote_addr.sin_port = htons(datanode_port);

    if (inet_aton(des_ip, &remote_addr.sin_addr) == 0)
    {
        cout << "dest ip: " << des_ip << endl;
        perror("inet_aton fail!");
        close(sock_data);
        return;
    }

    // 建立连接 - 添加重试机制和错误处理
    int retry_count = 0;
    const int max_retries = 3;
    while (retry_count < max_retries)
    {
        if (connect(sock_data, (struct sockaddr *)&remote_addr, sizeof(remote_addr)) == 0)
        {
            break; // 连接成功
        }
        retry_count++;
        if (retry_count >= max_retries)
        {
            perror("Failed to connect after retries");
            close(sock_data);
            return;
        }
        usleep(100000); // 等待100ms后重试
    }

    // 发送长度头 - 转换为网络字节序
    uint32_t data_length_net = htonl(chunk_size);
    if (write(sock_data, &data_length_net, sizeof(data_length_net)) != sizeof(data_length_net))
    {
        cerr << "Failed to send data length header" << endl;
        close(sock_data);
        return;
    }

    // cout << "Sending " << chunk_size << " bytes to datanode" << endl;

    // 发送实际数据
    // ssize_t bytes_sent = write(sock_data, buf_str.c_str(), buf_str.size());
    Socket socket_send_datanode;
    if (socket_send_datanode.SendDataReliably(sock_data, buf_str.c_str(), chunk_size) == false)
    {
        std::cerr << "Failed to send data to datanode" << std::endl;
        close(sock_data);
        return;
    }
    // 添加发送确认机制
    char ack_buffer[16];
    if (recv(sock_data, ack_buffer, sizeof(ack_buffer), 0) > 0)
    {
        // 处理确认消息
    }
    // cout << "Successfully sent " << bytes_sent << " bytes to datanode" << endl;
    close(sock_data);
}

void Proxy::GetRevData(char **data, int num, size_t *value_len, size_t *&recv_len)
{
    int *connfd = new int[num];
    recv_len = new size_t[num];
    vector<std::thread> recv_thrds;
    int retry_count = 0;
    int max_retry = 3; // 最大重试次数
    int index = 0;
    while (true)
    {
        connfd[index] = accept(sock_proxy_data, nullptr, nullptr);
        if (connfd[index] < 0)
        {
            if (errno == EBADF && retry_count < max_retry)
            {
                // 重建监听套接字
                close(sock_proxy_data);
                StartProxy_Data(); // 重新初始化 sock_proxy
                retry_count++;
                continue;
            }
            std::cerr << "accept failed: " << strerror(errno) << std::endl;
            close(connfd[index]);
            continue;
        }
        int conffd_accept = connfd[index];

        recv_thrds.emplace_back([this, recv_len, index, conffd_accept, &data, &value_len]()
                                { recv_len[index] = HandleGetRev(data[index], conffd_accept, value_len[index]); });
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

size_t Proxy::HandleGetRev(char *&data, int connfd, size_t value_len)
{
    // std::cout << "Received data from datanode" << std::endl;
    if (connfd < 0)
    {
        std::cerr << "Invalid file descriptor: " << connfd << std::endl;
        return -1;
    }
    // 接收文件长度头
    // 设置socket超时
    struct timeval timeout;
    timeout.tv_sec = 10; // 10秒超时
    timeout.tv_usec = 0;
    setsockopt(connfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    // 接收文件长度头
    uint32_t data_length_net;
    ssize_t header_received = recv(connfd, &data_length_net, sizeof(data_length_net), MSG_WAITALL);

    if (header_received != sizeof(data_length_net))
    {
        cerr << "Failed to receive data length header, received: " << header_received << endl;
        close(connfd);
        return -1;
    }

    size_t expected_size = ntohl(data_length_net); // 转换回主机字节序
    cout << "Expected to receive: " << expected_size << " bytes" << endl;

    // 验证数据长度合理性
    if (expected_size == 0 || expected_size > 1024 * 1024 * 100)
    { // 限制最大100MB
        cerr << "Invalid data size: " << expected_size << endl;
        close(connfd);
        return -1;
    }

    ssize_t total_received = 0;
    ssize_t remaining = expected_size;
    data = new char[expected_size + 1];
    memset(data, 0, expected_size + 1);
    // struct timeval timeout{.tv_sec = 5, .tv_usec = 0}; // 5秒超时

    // 设置套接字超时
    // setsockopt(connfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    while (total_received < (ssize_t)expected_size)
    {
        ssize_t received = read(connfd, data + total_received, remaining);
        if (received == 0)
        {
            // 连接正常关闭
            cout << "Connection closed by peer (received " << total_received << "/" << expected_size << " bytes)" << endl;
            break;
        }
        else if (received < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                // 超时或者暂时无数据
                cout << "Timeout or no data available, retrying..." << endl;
                continue;
            }
            std::cerr << "read() error: " << strerror(errno) << std::endl;
            break;
        }

        total_received += received;
        remaining -= received;
    }

    cout << "Received data size: " << total_received << endl;
    // cout<< " data size :"<<strlen(data)<<endl;
    close(connfd);
    return total_received;
}

void Proxy::SendMassgeToD(string destination, string value)
{
    // send ack to client
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
    {
        cerr << "Error: Unable to create socket" << endl;
        return;
    }

    struct sockaddr_in remote_addr;
    bzero(&remote_addr, sizeof(remote_addr));
    const char *des_ip = "127.0.0.1";
    if (destination == "client")
    {
        remote_addr.sin_family = AF_INET;
        remote_addr.sin_port = htons(CLIENT_PORT);
        des_ip = CLIENT_IP;
    }
    else if (destination == "coordinator")
    {
        remote_addr.sin_family = AF_INET;
        remote_addr.sin_port = htons(COORDINATOR_PORT);
        des_ip = COORDINATOR_IP;
    }

    if (inet_aton(des_ip, &remote_addr.sin_addr) == 0)
    {
        cout << "dest ip: " << des_ip << endl;
        perror("inet_aton fail!");
    }

    int retry_count = 0;
    const int max_retries = 3;
    while (retry_count < max_retries)
    {
        if (connect(sock, (struct sockaddr *)&remote_addr, sizeof(remote_addr)) == 0)
        {
            break; // 连接成功
        }
        retry_count++;
        if (retry_count >= max_retries)
        {
            perror("Failed to connect after retries");
            close(sock);
            return;
        }
        usleep(100000); // 等待100ms后重试
    }
    size_t chunk_size = value.size();

    // 发送长度头 - 转换为网络字节序
    // uint32_t data_length_net = htonl(chunk_size);
    // if (write(sock, &data_length_net, sizeof(data_length_net)) != sizeof(data_length_net)) {
    //     cerr << "Failed to send data length header" << endl;
    //     close(sock);
    //     return;
    // }

    // cout << "Sending " << chunk_size << " bytes to " << destination<<endl;

    // Socket socket_send_messg;
    // if (socket_send_messg.SendDataReliably(sock, value.c_str(), chunk_size) == false) {
    //     std::cerr << "Failed to send data to datanode" << std::endl;
    //     close(sock);
    //     return;
    // }
    if (send(sock, value.c_str(), value.size(), 0) < 0)
    {
        std::cerr << "Error: Failed to send data" << std::endl;
        close(sock);
        return;
    }
    // cout << "Sent ack to client " << endl;
    close(sock);
}

int main()
{
    ClientServer::Proxy proxy;
    proxy.StartProxy_Data();
    proxy.StartProxy();
}