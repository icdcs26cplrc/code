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
        buffer_data.append(data[0], value_len_[0]);
        // string ack = "SET_DONE_ACK";
        // SendMassgeToD("client", ack);
        cout << "after recv buffer_data size : " << buffer_data.size() << endl;
    }
    else if (command == "Encode")
    {
        // cout << " ProcessSet_Proxy Encode buffer size : " << buffer_data.size() << endl;
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
                // cout<<"copy len"<<i<<"]"<<copy_len<<endl;
                std::memcpy(data_ptr[i], data.data(), copy_len);
                // cout<<" encoding data_ptrs ["<<i<<"]"<<strlen(data_ptr[i])<<endl;
                // print_data_info(data_ptr[i], blocksize, "data_ptr");
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
        else if (encode_type_str == "spe-uni")
        {
            cout << "unbalanced encoding" << endl;
            UNBALANCED_LRC unbalanced(k, r, p, blocksize);
            unbalanced.encode(data_ptr, global_ptr, local_ptr);
        }
        else if (encode_type_str == "sep-azure")
        {
            SEP_AZURE spe_azure(k, r, p, blocksize);
            spe_azure.encode(data_ptr, global_ptr, local_ptr);
        }

        std::vector<std::thread> threads;
        for (int i = 0; i < num; i++)
        {
            if (i < k)
            {
                threads.emplace_back([this, i, data_ptr, blocksize]()
                                     {
                    
                    int node_id = i;
                    SendBlockToDatanode( node_id, data_ptr[i],blocksize); });
            }
            else if (i >= k && i < k + r)
            {
                threads.emplace_back([this, i, global_ptr, k, blocksize]()
                                     {
                    //int dataport_ = DATANODE_PORT_BASE + i;
                    int node_id = i;
                    SendBlockToDatanode(node_id, global_ptr[i-k],blocksize); });
            }
            else if (i >= k + r && i < k + r + p)
            {
                threads.emplace_back([this, i, local_ptr, k, r, blocksize]()
                                     {
                    //int dataport_ = DATANODE_PORT_BASE + i;
                    int node_id = i;
                    SendBlockToDatanode( node_id, local_ptr[i-k-r],blocksize); });
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
        // string ack = "SET_DONE_ACK";
        // SendMassgeToD("client", ack);
        cout << "after recv buffer_data size : " << buffer_data.size() << endl;
        // delete[] data_ptr;
        // delete[] global_ptr;
        // delete[] local_ptr;
    }
    else
    {
        cout << "INVALID COMMAND FROM COR" << endl;
    }
}

void Proxy::print_data_info(char *data, int size, const char *name)
{
    cout << name << " size: " << size << ", first 10 bytes: ";
    for (int i = 0; i < min(10, size); i++)
    {
        cout << (int)(unsigned char)data[i] << " ";
    }
    cout << endl;
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
        int num = get_int_from_string(get_message);
        get_info.assign(std::istreambuf_iterator<char>(iss >> std::ws), std::istreambuf_iterator<char>());

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
            try
            {
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
            }
            catch (const std::invalid_argument &e)
            {
                std::cerr << "Invalid input: " << node_id_str << " is not a valid integer." << std::endl;
                // 或其他适当的错误处理
            }
            catch (const std::out_of_range &e)
            {
                std::cerr << "Input out of range: " << node_id_str << " is too large or too small." << std::endl;
                // 或其他适当的错误处理
            }
            // cout << "get data from " << node_id[i] << " offset : " << offset[i] << " value_len : " << value_len_[i] << endl;
        }

        // 构建请求参数
        std::vector<string> node_ids(num);
        for (int i = 0; i < num; i++)
        {
            string temp = "node_id_" + std::to_string(node_id[i]);
            node_ids[i] = temp;
        }
        std::vector<int> offset_vec(offset, offset + num);
        std::vector<size_t> value_len_vec(value_len_, value_len_ + num);

        // 发送GET请求并接收数据
        char **data_get = new char *[num];
        size_t *recv_len = nullptr;
        std::vector<bool> request_success(num, false); // 记录每个请求是否成功

        // 初始化data_get数组
        for (int i = 0; i < num; i++)
        {
            data_get[i] = nullptr;
        }

        // 处理GET请求
        bool all_requests_successful = ProcessGetRequestsWithErrorHandling(
            data_get, recv_len, node_ids, offset_vec, value_len_vec, request_success);
        resort_recvdata(data_get, recv_len, num);
        // 检查接收到的数据是否完整 - 修复后的错误检测逻辑
        int *error_id = nullptr;
        int fail_num = check_error_fixed(num, recv_len, value_len_, node_id, error_id, request_success);

        // 组合数据或触发降级读
        string value_get_to_client;
        if (fail_num == 0)
        {
            // 所有数据都正确接收，组合数据
            for (int i = 0; i < num; i++)
            {
                value_get_to_client.append(data_get[i], value_len_[i]);
            }
            cout << "Successfully combined data from all nodes" << endl;
        }
        else
        {
            // cout << "Detected " << fail_num << " failed nodes, triggering degraded read" << endl;

            // 触发降级读
            string fail_info;
            for (int i = 0; i < fail_num; i++)
            {
                string fail_mesg = "stripe_" + to_string(stripe_id) + "_block_" + to_string(error_id[i]) + " ";
                fail_info.append(fail_mesg);
            }
            string fail_value = "REPAIR proxy fail_number_" + to_string(fail_num) + " " + fail_info;

            // cout << "Sending repair request: " << fail_value << endl;
            SendMassgeToD("coordinator", fail_value);

            // 接收来自coordinator的修复信息
            char *repair_message = nullptr;
            size_t *recv_length = new size_t[1]; // 增加缓冲区大小
            size_t *actual_length = new size_t[1];
            actual_length[0] = 1024;

            // cout << "Waiting for repair message from coordinator..." << endl;
            GetRevData(&repair_message, 1, actual_length, recv_length, true);

            // fail_offset

            if (repair_message != nullptr && *actual_length > 0)
            {
                cout << "Received repair message, starting degraded read process" << endl;
                // 执行降级读修复
                Repair_degrade(repair_message, num, node_id, offset, value_len_, value_get_to_client, data_get);
                delete[] repair_message;
            }
            else
            {
                cout << "Failed to receive repair message from coordinator" << endl;
                value_get_to_client = "ERROR: Failed to perform degraded read";
            }

            delete actual_length;
        }

        // 发送结果给客户端
        SendMassgeToD("client", value_get_to_client);

        // 清理内存
        for (int i = 0; i < num; i++)
        {
            if (data_get[i] != nullptr)
            {
                delete[] data_get[i];
            }
        }
        delete[] data_get;
        delete[] recv_len;
        delete[] node_id;
        delete[] offset;
        delete[] value_len_;
        if (error_id != nullptr)
        {
            delete[] error_id;
        }
    }
    else
    {
        cout << "INVALID GET COMMAND" << endl;
    }
}

void Proxy::resort_recvdata(char **recv_data, size_t *recv_len, int num)
{
    // 对recv_data 重新排序
    // Extract and sort node_ids
    std::vector<std::pair<int, int>> node_id_index_pairs;
    std::vector<size_t> original_lengths(num);
    std::vector<bool> valid_data(num, false); // 标记哪些数据是有效的

    for (int i = 0; i < num; ++i)
    {
        original_lengths[i] = recv_len[i];

        // 如果接收长度为0，说明这个节点没有数据（可能是请求失败）
        if (recv_len[i] == 0 || recv_data[i] == nullptr)
        {
            std::cerr << "No data received from index " << i << std::endl;
            node_id_index_pairs.emplace_back(i, i); // 使用索引作为node_id占位符
            continue;
        }

        // 将接收到的数据转换为字符串进行解析
        std::string data_str(recv_data[i], recv_len[i]);

        // 查找第一个换行符，node_id应该在第一行
        size_t first_newline = data_str.find('\n');

        if (first_newline != std::string::npos)
        {
            try
            {
                // 提取第一行作为node_id
                std::string node_id_str = data_str.substr(0, first_newline);

                // 检查是否以"node_id_"开头
                if (node_id_str.find("node_id_") == 0)
                {
                    // 提取node_id数值
                    std::string id_part = node_id_str.substr(8); // 跳过"node_id_"
                    int node_id = std::stoi(id_part);
                    node_id_index_pairs.emplace_back(node_id, i);
                    valid_data[i] = true;

                    // cout << "Parsed node_id: " << node_id << " from data[" << i << "]" << endl;
                }
                else
                {
                    std::cerr << "Invalid node_id format in data[" << i << "]: " << node_id_str << std::endl;
                    node_id_index_pairs.emplace_back(i, i); // 使用索引作为占位符
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "Error parsing node_id from data[" << i << "]: " << e.what() << std::endl;
                node_id_index_pairs.emplace_back(i, i); // 使用索引作为占位符
            }
        }
        else
        {
            std::cerr << "No newline found in data[" << i << "], invalid format" << std::endl;
            node_id_index_pairs.emplace_back(i, i); // 使用索引作为占位符
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

        // 如果原始数据无效或为空
        if (!valid_data[original_index] || recv_len[original_index] == 0)
        {
            // 失败节点置为空数据
            sorted_lengths[i] = 0;
            sorted_data_get[i] = nullptr;
        }
        else
        {
            sorted_lengths[i] = original_lengths[original_index];

            // 分配新内存并复制数据
            sorted_data_get[i] = new char[sorted_lengths[i]];
            memcpy(sorted_data_get[i], recv_data[original_index], sorted_lengths[i]);
        }
    }

    // 从排序后的数据中提取实际数据内容（去掉node_id行和最后的换行符）
    for (int i = 0; i < num; ++i)
    {
        if (sorted_lengths[i] == 0 || sorted_data_get[i] == nullptr)
        {
            continue; // 跳过空数据
        }

        std::string data_str(sorted_data_get[i], sorted_lengths[i]);

        // 查找第一个换行符（node_id行结束）
        size_t first_newline = data_str.find('\n');

        if (first_newline != std::string::npos)
        {
            // 提取实际数据部分（跳过node_id行）
            size_t data_start = first_newline + 1;
            size_t data_length = sorted_lengths[i] - data_start;

            // 检查是否以换行符结尾，如果是则去掉
            if (data_length > 0 && sorted_data_get[i][sorted_lengths[i] - 1] == '\n')
            {
                data_length--;
            }

            if (data_length > 0)
            {
                // 创建新的缓冲区存储纯数据
                char *clean_data = new char[data_length];
                memcpy(clean_data, sorted_data_get[i] + data_start, data_length);

                // 释放旧内存，更新指针和长度
                delete[] sorted_data_get[i];
                sorted_data_get[i] = clean_data;
                sorted_lengths[i] = data_length;

                cout << "Node " << node_id_index_pairs[i].first
                     << " - size of cleaned data[" << i << "]: " << sorted_lengths[i] << endl;
            }
            else
            {
                // 数据为空
                delete[] sorted_data_get[i];
                sorted_data_get[i] = nullptr;
                sorted_lengths[i] = 0;
            }
        }
        else
        {
            std::cerr << "Error: No newline found in sorted data[" << i << "]" << std::endl;
            // 保持原始数据不变，但这种情况下数据可能有问题
        }
    }

    // 更新原始数据
    for (int i = 0; i < num; ++i)
    {
        // 释放原始数据内存
        if (recv_data[i] != nullptr)
        {
            delete[] recv_data[i];
        }
        recv_data[i] = sorted_data_get[i];
        recv_len[i] = sorted_lengths[i]; // 更新长度信息
    }

    // 释放临时数组
    delete[] sorted_data_get;
}

void Proxy::overlap_resort_recvdata(char **recv_data, size_t *recv_len, int num, const size_t *value_len)
{
    // recv_data 数组大小为 2*num，每个node_id重复2次
    // 根据数据大小与value_len匹配来决定拼接顺序
    int total_data_count = 2 * num;

    // Extract and sort node_ids
    std::map<int, std::vector<int>> node_id_to_indices; // node_id -> 原始索引列表
    std::vector<size_t> original_lengths(total_data_count);
    std::vector<bool> valid_data(total_data_count, false); // 标记哪些数据是有效的

    for (int i = 0; i < total_data_count; ++i)
    {
        original_lengths[i] = recv_len[i];

        // 如果接收长度为0，说明这个节点没有数据（可能是请求失败）
        if (recv_len[i] == 0 || recv_data[i] == nullptr)
        {
            std::cerr << "No data received from index " << i << std::endl;
            continue;
        }

        // 将接收到的数据转换为字符串进行解析
        std::string data_str(recv_data[i], recv_len[i]);

        // 查找第一个换行符，node_id应该在第一行
        size_t first_newline = data_str.find('\n');

        if (first_newline != std::string::npos)
        {
            try
            {
                // 提取第一行作为node_id
                std::string node_id_str = data_str.substr(0, first_newline);

                // 检查是否以"node_id_"开头
                if (node_id_str.find("node_id_") == 0)
                {
                    // 提取node_id数值
                    std::string id_part = node_id_str.substr(8); // 跳过"node_id_"
                    int node_id = std::stoi(id_part);
                    node_id_to_indices[node_id].push_back(i);
                    valid_data[i] = true;

                    // cout << "Parsed node_id: " << node_id << " from data[" << i << "]" << endl;
                }
                else
                {
                    std::cerr << "Invalid node_id format in data[" << i << "]: " << node_id_str << std::endl;
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "Error parsing node_id from data[" << i << "]: " << e.what() << std::endl;
            }
        }
        else
        {
            std::cerr << "No newline found in data[" << i << "], invalid format" << std::endl;
        }
    }

    // 提取纯数据内容（去掉node_id行）并记录实际大小
    std::vector<std::vector<char>> clean_data_list(total_data_count);
    std::vector<size_t> clean_data_lengths(total_data_count);

    for (int i = 0; i < total_data_count; ++i)
    {
        if (!valid_data[i] || recv_len[i] == 0 || recv_data[i] == nullptr)
        {
            clean_data_lengths[i] = 0;
            continue;
        }

        std::string data_str(recv_data[i], recv_len[i]);
        size_t first_newline = data_str.find('\n');

        if (first_newline != std::string::npos)
        {
            size_t data_start = first_newline + 1;
            size_t data_length = recv_len[i] - data_start;

            // 检查是否以换行符结尾，如果是则去掉
            if (data_length > 0 && recv_data[i][recv_len[i] - 1] == '\n')
            {
                data_length--;
            }

            if (data_length > 0)
            {
                clean_data_list[i].resize(data_length);
                memcpy(clean_data_list[i].data(), recv_data[i] + data_start, data_length);
                clean_data_lengths[i] = data_length;
            }
            else
            {
                clean_data_lengths[i] = 0;
            }
        }
        else
        {
            clean_data_lengths[i] = 0;
        }
    }

    // 创建排序后的node_id列表
    std::vector<int> sorted_node_ids;
    for (const auto &pair : node_id_to_indices)
    {
        sorted_node_ids.push_back(pair.first);
    }
    std::sort(sorted_node_ids.begin(), sorted_node_ids.end());

    // 合并重复node_id的数据，根据数据大小与value_len匹配来决定拼接顺序
    std::vector<std::vector<char>> final_merged_data(num);
    std::vector<size_t> final_merged_lengths(num);

    for (int output_idx = 0; output_idx < num && output_idx < sorted_node_ids.size(); ++output_idx)
    {
        int node_id = sorted_node_ids[output_idx];
        const std::vector<int> &indices = node_id_to_indices[node_id];

        std::cout << "Processing node_id " << node_id << " with " << indices.size()
                  << " duplicate entries for output position " << output_idx << std::endl;

        if (indices.empty())
        {
            // 无有效数据
            final_merged_lengths[output_idx] = 0;
            continue;
        }

        // 创建数据大小与索引的配对，用于匹配value_len
        std::vector<std::pair<size_t, int>> size_index_pairs; // <实际数据大小, 原始索引>

        for (int idx : indices)
        {
            if (clean_data_lengths[idx] > 0)
            {
                size_index_pairs.emplace_back(clean_data_lengths[idx], idx);
                std::cout << "  - Index " << idx << " has clean data size: "
                          << clean_data_lengths[idx] << std::endl;
            }
        }

        if (size_index_pairs.empty())
        {
            final_merged_lengths[output_idx] = 0;
            continue;
        }

        // 根据value_len的顺序来排列数据
        // 创建value_len的排序顺序
        std::vector<std::pair<size_t, int>> value_len_order; // <value_len值, value_len索引>
        for (int i = 0; i < num; ++i)
        {
            value_len_order.emplace_back(value_len[i], i);
        }
        std::sort(value_len_order.begin(), value_len_order.end());

        // 按照value_len的顺序匹配数据大小
        std::vector<char> concatenated_buffer;
        std::vector<bool> used_data(size_index_pairs.size(), false);

        std::cout << "  Value_len order for matching:" << std::endl;
        for (const auto &vl_pair : value_len_order)
        {
            std::cout << "    Looking for data size " << vl_pair.first << std::endl;

            // 寻找匹配的数据大小
            for (size_t j = 0; j < size_index_pairs.size(); ++j)
            {
                if (!used_data[j] && size_index_pairs[j].first == vl_pair.first)
                {
                    int data_idx = size_index_pairs[j].second;

                    // 添加这个数据到拼接缓冲区
                    concatenated_buffer.insert(concatenated_buffer.end(),
                                               clean_data_list[data_idx].begin(),
                                               clean_data_list[data_idx].end());

                    used_data[j] = true;

                    std::cout << "    Matched and added data from index " << data_idx
                              << " (size: " << size_index_pairs[j].first << ")" << std::endl;
                    break;
                }
            }
        }

        // 如果还有未使用的数据（没有匹配的value_len），按原顺序添加
        for (size_t j = 0; j < size_index_pairs.size(); ++j)
        {
            if (!used_data[j])
            {
                int data_idx = size_index_pairs[j].second;
                concatenated_buffer.insert(concatenated_buffer.end(),
                                           clean_data_list[data_idx].begin(),
                                           clean_data_list[data_idx].end());

                std::cout << "    Added unmatched data from index " << data_idx
                          << " (size: " << size_index_pairs[j].first << ")" << std::endl;
            }
        }

        final_merged_data[output_idx] = concatenated_buffer;
        final_merged_lengths[output_idx] = concatenated_buffer.size();

        std::cout << "Final merged data for node_id " << node_id
                  << " at position " << output_idx << ", total length: "
                  << concatenated_buffer.size() << std::endl;
    }

    // 释放原始数据内存
    for (int i = 0; i < total_data_count; ++i)
    {
        if (recv_data[i] != nullptr)
        {
            delete[] recv_data[i];
            recv_data[i] = nullptr;
        }
    }

    // 重新分配最终结果到大小为num的数组
    for (int i = 0; i < num; ++i)
    {
        if (final_merged_lengths[i] > 0)
        {
            recv_data[i] = new char[final_merged_lengths[i]];
            memcpy(recv_data[i], final_merged_data[i].data(), final_merged_lengths[i]);
            recv_len[i] = final_merged_lengths[i];

            std::cout << "Final result[" << i << "] - length: " << recv_len[i] << std::endl;
        }
        else
        {
            recv_data[i] = nullptr;
            recv_len[i] = 0;
        }
    }

    // 将数组中num到2*num-1的位置置空（因为现在数组大小变为num）
    for (int i = num; i < total_data_count; ++i)
    {
        recv_data[i] = nullptr;
        recv_len[i] = 0;
    }
}

void Proxy::Repair_degrade(string message, int value_get_num, const int *node_id, const int *offset_value, const size_t *repair_value_len, string &value, char **data_alredy_get)
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
        // cout << "node_need_id[" << i << "]" << node_need_id[i] << endl;
    }

    // recv_data
    char **recv_data = new char *[num];
    // std::vector<size_t> value_len(num, blocksize);
    size_t *recv_len;
    // GetRevData(recv_data, num, value_len.data(), recv_len);
    vector<string> node_ids(num);
    vector<int> offset_value_vec(num);
    vector<size_t> repair_value_len_vec(num);
    vector<int> degrade_node_ids(num);
    for (int i = 0; i < num; i++)
    {
        degrade_node_ids[i] = node_need_id[i];
    }

    // 优化 ---- >计算offset和value_len
    int symbols = compute_degrade_offsets_and_lens(
        fail_number, blocksize,
        std::vector<int>(node_id, node_id + value_get_num),
        std::vector<int>(fail_node, fail_node + fail_number),
        degrade_node_ids,
        std::vector<int>(offset_value, offset_value + value_get_num),
        std::vector<size_t>(repair_value_len, repair_value_len + value_get_num), offset_value_vec, repair_value_len_vec);
    // ProcessGetRequests(recv_data, recv_len, node_ids, offset_value_vec, repair_value_len_vec);
    // resort_recvdata(recv_data, recv_len, num);
    int real_degrade_num = degrade_node_ids.size();
    node_ids.resize(real_degrade_num);
    for (int i = 0; i < real_degrade_num; i++)
    {
        string temp = "node_id_" + std::to_string(degrade_node_ids[i]);
        node_ids[i] = temp; // Store the node_id as a string
    }
    vector<bool> request_success(num, false);
    bool all_requests_successful = ProcessGetRequestsWithErrorHandling(
        recv_data, recv_len, node_ids, offset_value_vec, repair_value_len_vec, request_success);

    // 处理数据
    if (symbols < 20)
    {
        resort_recvdata(recv_data, recv_len, num);
    }
    else
    {
        // symbos > 20, 可能存在同一节点不同数据,重复的node_id
        size_t *papping_two_fail = new size_t[2];
        papping_two_fail[0] = repair_value_len[fail_node[0] - node_id[0]];
        papping_two_fail[1] = repair_value_len[fail_node[1] - node_id[0]];
        overlap_resort_recvdata(recv_data, recv_len, num, papping_two_fail);
    }

    size_t afer_computer_fail_value_len = 0;

    if (symbols == 0)
    {
        afer_computer_fail_value_len = blocksize;
    }
    else if (symbols == 10 || symbols == 11 || symbols == 12)
    {
        afer_computer_fail_value_len = repair_value_len[fail_node[0] - node_id[0]];
    }
    else if (symbols == 20 || symbols == 21 || symbols == 22)
    {
        afer_computer_fail_value_len = repair_value_len[fail_node[0] - node_id[0]] + repair_value_len[fail_node[1] - node_id[0]];
    }

    cout << "symbols : " << symbols << " afer_computer_fail_value_len : " << afer_computer_fail_value_len << endl;

    char **data_ptrs = new char *[k];
    char **global_parity = new char *[r];
    char **local_parity = new char *[p];
    int sum = k + r + p;
    int pos = 0;
    for (int i = 0; i < k; i++)
        data_ptrs[i] = nullptr;
    for (int i = 0; i < r; i++)
        global_parity[i] = nullptr;
    for (int i = 0; i < p; i++)
        local_parity[i] = nullptr;

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

        if (!is_failed && pos < num && i == node_need_id[pos])
        {

            // 重复读,用data_already_get填充recv_data
            if (recv_len[pos] < afer_computer_fail_value_len)
            {
                cout << "some information already get, from node_" << i << endl;
                string temp;
                // 错误发生在头部
                if (symbols == 11)
                {
                    temp = std::string(data_alredy_get[i - node_id[0]] + offset_value[fail_node[0] - node_id[0]], afer_computer_fail_value_len - recv_len[pos]);

                    // Free the old memory if needed
                    if (recv_data[pos] != nullptr)
                    {
                        temp.append(recv_data[pos], recv_len[pos]);
                        delete[] recv_data[pos];
                    }
                    recv_data[pos] = new char[afer_computer_fail_value_len];
                    memcpy(recv_data[pos], temp.c_str(), afer_computer_fail_value_len);
                    // recv_data[pos][afer_computer_fail_value_len] = '\0';
                    // 发生在尾部
                }
                else if (symbols == 12)
                {
                    if (recv_data[pos] != nullptr)
                    {
                        // 安全访问
                        temp = recv_data[pos];
                    }
                    else
                    {
                        recv_data[pos] = new char[afer_computer_fail_value_len];
                    }
                    string temp_1 = std::string(data_alredy_get[i - node_id[0]], afer_computer_fail_value_len - recv_len[pos]);
                    temp.append(temp_1);
                    // Free the old memory if needed
                    if (recv_data[pos] != nullptr)
                    {
                        delete[] recv_data[pos];
                    }
                    recv_data[pos] = new char[afer_computer_fail_value_len];
                    memcpy(recv_data[pos], temp.c_str(), afer_computer_fail_value_len);
                    // recv_data[pos][afer_computer_fail_value_len] = '\0';
                    // 错误发生在头部和尾部
                }
                else if (symbols == 22)
                {
                    string top = string(data_alredy_get[i - node_id[0]], repair_value_len[fail_node[1] - node_id[1]]);
                    string low = string(data_alredy_get[i - node_id[0]] + offset_value[fail_node[0] - node_id[0]], repair_value_len[fail_node[0] - node_id[0]]);
                    temp = top + low;
                    if (recv_data[pos] != nullptr)
                    {
                        delete[] recv_data[pos];
                    }
                    recv_data[pos] = new char[afer_computer_fail_value_len];
                    memcpy(recv_data[pos], temp.c_str(), afer_computer_fail_value_len);
                    // recv_data[pos][afer_computer_fail_value_len] = '\0';
                }
            }

            if (i < k && i >= 0)
            {
                // data_ptrs[i] = new char[afer_computer_fail_value_len];
                data_ptrs[i] = recv_data[pos];
                pos++;
                // cout << "size of data_ptrs[" << i << "] : " << strlen(data_ptrs[i]) << endl;
            }
            else if (i >= k && i < k + r)
            {
                // global_parity[i - k] = new char[afer_computer_fail_value_len];
                global_parity[i - k] = recv_data[pos];
                pos++;
                // cout << "size of global[" << i - k << "] : " << strlen(global_parity[i - k]) << endl;
            }
            else if (i >= (k + r) && i < (k + r + p))
            {
                // local_parity[i - k - r] = new char[afer_computer_fail_value_len];
                local_parity[i - k - r] = recv_data[pos];
                pos++;
                // cout << "size of local_parity[" << i - k - r << "] : " << strlen(local_parity[i - k - r]) << endl;
            }
        }
        else if (i != node_need_id[pos])
        {
            if (i < k)
            {
                data_ptrs[i] = new char[afer_computer_fail_value_len];
                // cout << "size of data_ptrs[" << i << "] : " << strlen(data_ptrs[i]) << endl;
            }
            else if (i >= k && i < k + r)
            {
                global_parity[i - k] = new char[afer_computer_fail_value_len];
                // cout << "size of global[" << i - k << "] : " << strlen(global_parity[i - k]) << endl;
            }
            else
            {
                local_parity[i - k - r] = new char[afer_computer_fail_value_len];
                // cout << "size of local_parity[" << i - k - r << "] : " << strlen(local_parity[i - k - r]) << endl;
            }
        }
    }
    bool repair_ok = false;
    // cout<< "decode_type: " << decode_type << endl;

    if (decode_type == "new_lrc")
    {
        NewLRC newlrc(k, r, p, blocksize);
        repair_ok = newlrc.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity, afer_computer_fail_value_len);
    }
    else if (decode_type == "azure")
    {
        AZURE_LRC azure(k, r, p, blocksize);
        repair_ok = azure.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity, afer_computer_fail_value_len);
    }
    else if (decode_type == "azure_1")
    {
        AZURE_LRC_1 azure_1(k, r, p, blocksize);
        repair_ok = azure_1.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity, afer_computer_fail_value_len);
    }
    else if (decode_type == "optimal")
    {
        OPTIMAL_LRC optimal(k, r, p, blocksize);
        repair_ok = optimal.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity, afer_computer_fail_value_len);
    }
    else if (decode_type == "uniform")
    {
        UNIFORM_LRC uniform(k, r, p, blocksize);
        repair_ok = uniform.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity, afer_computer_fail_value_len);
    }
    else if (decode_type == "sep-uni")
    {
        UNBALANCED_LRC unbalanced(k, r, p, blocksize);
        repair_ok = unbalanced.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity, afer_computer_fail_value_len);
    }
    else if (decode_type == "sep-azure")
    {
        SEP_AZURE sep_azure(k, r, p, blocksize);
        repair_ok = sep_azure.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity, afer_computer_fail_value_len);
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
        if (data_alredy_get[i] != nullptr )
        {
            value.append(data_alredy_get[i]);
        }
        
    }
    
    if (symbols == 0)
    {
        for (int i = 0; i < value_get_num; i++)
        {
            char *temp = data_ptrs[node_id[i]];
            value.append(temp + offset_value[i], repair_value_len[i]);
        }
    }
    else if (symbols == 10 || symbols == 11 || symbols == 12)
    { // 优化对齐repair 只有一个错
      // 文件位于block中并且该block故障
        cout << "fail node : " << fail_node[0] << " node_id_ : " << node_id[0] << endl;
        if (data_ptrs[fail_node[0]] == nullptr)
        {
            cout << "something wrong!!!!!!!!!!!data is none!!!!!!!!!!!" << endl;
        }
        else
        {
            string temp = string(data_ptrs[fail_node[0]], afer_computer_fail_value_len);

            // 在 value 的 fail_node[0] - node_id[0] 位置插入 temp
            size_t insert_pos = fail_node[0] - node_id[0];
            size_t insert_offset = 0;
            if (insert_pos > 0)
            {
                for (int i = 0; i < insert_pos; i++)
                {
                    insert_offset += i * repair_value_len[i];
                }
            }
            else
            {
                insert_offset = 0;
            }
            cout << "inser_offset : " << insert_offset << endl;
            value.insert(insert_offset, temp);
        }

    } // 文件位于多个block中并且该block故障
    else
    {
        for (int i = 0; i < value_get_num; i++)
        {
            if (node_id[i] != fail_node[0] && node_id[i] != fail_node[1])
            {
                //value.append(data_alredy_get[i] + offset_value[i], repair_value_len[i]);
            }
            else
            {
                // 如果是故障节点，使用修复后的数据
                string temp = string(data_ptrs[node_id[i]] + offset_value[i], repair_value_len[i]);
                value.append(temp);
            }
        }
    }
}

int Proxy::compute_degrade_offsets_and_lens(const int fail_num, const size_t block_size, const std::vector<int> &first_get_node_ids, const std::vector<int> &fail_node_ids, std::vector<int> &degrade_get_node_ids,
                                            const std::vector<int> &offsets, const std::vector<size_t> &value_lens,
                                            std::vector<int> &offset_vec, std::vector<size_t> &value_len_vec)
{
    int symbols = 0;
    // 是否需要优化
    size_t total_fail_value_len_ = 0;
    for (int i = 0; i < fail_num; i++)
    {
        total_fail_value_len_ += value_lens[(fail_node_ids[i] - first_get_node_ids[0])];
    }
    int degrade_get_num = degrade_get_node_ids.size();
    int first_get_num = first_get_node_ids.size();

    if (fail_num > 2 || total_fail_value_len_ >= block_size)
    {
        offset_vec.resize(degrade_get_num, 0);
        value_len_vec.resize(degrade_get_num, block_size);
        return symbols;
    }
    else
    {
        // 故障发生在头部或尾部
        if (fail_num == 1)
        {
            symbols = 10;

            size_t fail_start_pos = offsets[fail_node_ids[0] - first_get_node_ids[0]];
            size_t fail_value_len_ = value_lens[fail_node_ids[0] - first_get_node_ids[0]];
            size_t fail_end_pos = fail_start_pos + fail_value_len_;

            offset_vec.resize(degrade_get_num);
            value_len_vec.resize(degrade_get_num);

            size_t value_end_pos = 0;
            size_t value_start_pos = 0;
            for (int i = 0; i < degrade_get_num; i++)
            {
                bool if_overlap = false;

                for (int j = 0; j < first_get_num; j++)
                { // 存在重复读的可能
                    if (degrade_get_node_ids[i] == first_get_node_ids[j])
                    {
                        value_end_pos = offsets[first_get_node_ids[j] - first_get_node_ids[0]] + value_lens[first_get_node_ids[j] - first_get_node_ids[0]];
                        value_start_pos = offsets[first_get_node_ids[j] - first_get_node_ids[0]];
                        size_t overlap_end_diff = (value_end_pos >= fail_end_pos) ? (value_end_pos - fail_end_pos) : (fail_end_pos - value_end_pos);
                        size_t overlap_start_diff = (value_start_pos >= fail_start_pos) ? (value_start_pos - fail_start_pos) : (fail_start_pos - value_start_pos);
                        size_t overlap_value_len = block_size - overlap_start_diff - overlap_end_diff;
                        if (overlap_value_len > 0)
                        {
                            if_overlap = true;
                            if (fail_node_ids[0] == first_get_node_ids[0])
                            {
                                // 故障节点位于头部
                                offset_vec[i] = value_end_pos;
                                value_len_vec[i] = fail_end_pos - value_end_pos;
                                symbols = 11; // 表示故障节点位于头部
                            }
                            else
                            {
                                // 故障节点位于尾部
                                offset_vec[i] = (fail_start_pos);
                                value_len_vec[i] = (value_start_pos - fail_start_pos);
                                symbols = 12; // 表示故障节点位于尾部
                            }
                        }
                    }
                }
                if (if_overlap && symbols == 11)
                {
                    // 故障节点位于头部
                    offset_vec[i] = value_end_pos;
                    value_len_vec[i] = fail_end_pos - value_end_pos;
                }
                else if (if_overlap && symbols == 12)
                {
                    // 故障节点位于尾部
                    offset_vec[i] = fail_start_pos;
                    value_len_vec[i] = value_start_pos - fail_start_pos;
                }
                else if (!if_overlap)
                {
                    // 不存在重复读的情况,对其fail_node_ids[i]的偏移量和长度进行计算
                    offset_vec[i] = fail_start_pos;
                    value_len_vec[i] = fail_value_len_;
                }
            }

            // 故障发生在头部和尾部
        }
        else if (fail_num == 2)
        {
            symbols = 20;
            // 头部故障
            size_t fail_start_pos_1 = offsets[fail_node_ids[0] - first_get_node_ids[0]];
            size_t fail_value_len_1 = value_lens[fail_node_ids[0] - first_get_node_ids[0]];
            size_t fail_end_pos_1 = fail_start_pos_1 + fail_value_len_1;
            // 尾部故障
            size_t fail_start_pos_2 = offsets[fail_node_ids[1] - first_get_node_ids[0]];
            size_t fail_value_len_2 = value_lens[fail_node_ids[1] - first_get_node_ids[0]];
            size_t fail_end_pos_2 = fail_start_pos_2 + fail_value_len_2;

            offset_vec.resize(2 * degrade_get_num);
            value_len_vec.resize(2 * degrade_get_num);
            degrade_get_node_ids.resize(2 * degrade_get_num);
            for (int i = degrade_get_num - 1; i >= 0; --i)
            {
                degrade_get_node_ids[2 * i] = degrade_get_node_ids[i];
                degrade_get_node_ids[2 * i + 1] = degrade_get_node_ids[i];
            }

            // 不可能发生重复读
            if (first_get_num == 2)
            {
                for (int i = 0; i < 2 * degrade_get_num; i += 2)
                {
                    offset_vec[i] = fail_start_pos_2;
                    value_len_vec[i] = fail_value_len_2;
                    offset_vec[i + 1] = fail_start_pos_1;
                    value_len_vec[i + 1] = fail_value_len_1;
                }
                symbols = 21; // 表示故障节点位于头部和尾部且不可能发生重复读
                return symbols;
            }
            else
            {
                // 可能发生重复读
                for (int i = 0; i < 2 * degrade_get_num; i += 2)
                {
                    bool if_overlap = false;
                    for (int j = 0; j < first_get_num; j++)
                    {
                        if (degrade_get_node_ids[i] == first_get_node_ids[j])
                        {
                            if_overlap = true;
                            symbols = 22; // 表示故障节点位于头部和尾部且发生重复读
                        }
                    }
                    if (if_overlap)
                    {
                        // 必然存在重复读并且不需判断
                        offset_vec[i] = fail_start_pos_2;
                        value_len_vec[i] = 0;
                        offset_vec[i + 1] = fail_start_pos_1;
                        value_len_vec[i + 1] = 0;
                    }
                    else
                    {
                        // 不存在重复读的情况,对其fail_node_ids[i]的偏移量和长度进行计算
                        offset_vec[i] = fail_start_pos_2;
                        value_len_vec[i] = fail_value_len_2;
                        offset_vec[i + 1] = fail_start_pos_1;
                        value_len_vec[i + 1] = fail_value_len_1;
                    }
                }
            }
        }
        return symbols;
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
    std::vector<std::pair<int, int>> node_id_index_pairs;
    std::vector<size_t> original_lengths(num); // 保存原始长度

    for (int i = 0; i < num; ++i)
    {
        original_lengths[i] = recv_len[i]; // 保存实际接收长度

        // 将接收到的数据转换为字符串进行解析
        std::string data_str(recv_data[i], recv_len[i]);

        // 查找第一个换行符，node_id应该在第一行
        size_t first_newline = data_str.find('\n');

        if (first_newline != std::string::npos)
        {
            try
            {
                // 提取第一行作为node_id
                std::string node_id_str = data_str.substr(0, first_newline);

                // 检查是否以"node_id_"开头
                if (node_id_str.find("node_id_") == 0)
                {
                    // 提取node_id数值
                    std::string id_part = node_id_str.substr(8); // 跳过"node_id_"
                    int node_id = std::stoi(id_part);
                    node_id_index_pairs.emplace_back(node_id, i);

                    cout << "Parsed node_id: " << node_id << " from data[" << i << "]" << endl;
                }
                else
                {
                    std::cerr << "Invalid node_id format in data[" << i << "]: " << node_id_str << std::endl;
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "Error parsing node_id from data[" << i << "]: " << e.what() << std::endl;
            }
        }
        else
        {
            std::cerr << "No newline found in data[" << i << "], invalid format" << std::endl;
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

    // 从排序后的数据中提取实际数据内容（去掉node_id行和最后的换行符）
    for (int i = 0; i < num; ++i)
    {
        std::string data_str(sorted_data_get[i], sorted_lengths[i]);

        // 查找第一个换行符（node_id行结束）
        size_t first_newline = data_str.find('\n');

        if (first_newline != std::string::npos)
        {
            // 提取实际数据部分（跳过node_id行）
            size_t data_start = first_newline + 1;
            size_t data_length = sorted_lengths[i] - data_start;

            // 检查是否以换行符结尾，如果是则去掉
            if (data_length > 0 && sorted_data_get[i][sorted_lengths[i] - 1] == '\n')
            {
                data_length--;
            }

            // 创建新的缓冲区存储纯数据
            char *clean_data = new char[data_length];
            memcpy(clean_data, sorted_data_get[i] + data_start, data_length);

            // 释放旧内存，更新指针和长度
            delete[] sorted_data_get[i];
            sorted_data_get[i] = clean_data;
            sorted_lengths[i] = data_length;

            cout << "Node " << node_id_index_pairs[i].first
                 << " - size of cleaned data[" << i << "]: " << sorted_lengths[i] << endl;
        }
        else
        {
            std::cerr << "Error: No newline found in sorted data[" << i << "]" << std::endl;
        }
    }

    // 更新原始数据
    for (int i = 0; i < num; ++i)
    {
        // 释放原始数据内存
        delete[] recv_data[i];
        recv_data[i] = sorted_data_get[i];
        recv_len[i] = sorted_lengths[i]; // 更新长度信息
    }

    // 释放临时数组
    delete[] sorted_data_get;

    char **data_ptrs = new char *[k];
    char **global_parity = new char *[r];
    char **local_parity = new char *[p];
    for (int i = 0; i < k; i++)
        data_ptrs[i] = nullptr;
    for (int i = 0; i < r; i++)
        global_parity[i] = nullptr;
    for (int i = 0; i < p; i++)
        local_parity[i] = nullptr;
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

        if (!is_failed && pos < num && i == node_need_id[pos])
        {
            if (i < k && i >= 0)
            {
                data_ptrs[i] = new char[blocksize];
                data_ptrs[i] = recv_data[pos];
                pos++;
                // cout << "size of data_ptrs[" << i << "] : " << strlen(data_ptrs[i]) << endl;
            }
            else if (i >= k && i < k + r)
            {
                global_parity[i - k] = new char[blocksize];
                global_parity[i - k] = recv_data[pos];
                pos++;
                // cout << "size of global[" << i - k << "] : " << strlen(global_parity[i - k]) << endl;
            }
            else if (i >= (k + r) && i < (k + r + p))
            {
                local_parity[i - k - r] = new char[blocksize];
                local_parity[i - k - r] = recv_data[pos];
                pos++;
                // cout << "size of local_parity[" << i - k - r << "] : " << strlen(local_parity[i - k - r]) << endl;
            }
        }
        else if (i != node_need_id[pos])
        {
            if (i < k)
            {
                data_ptrs[i] = new char[blocksize];
                // cout << "size of data_ptrs[" << i << "] : " << strlen(data_ptrs[i]) << endl;
            }
            else if (i >= k && i < k + r)
            {
                global_parity[i - k] = new char[blocksize];
                // cout << "size of global[" << i - k << "] : " << strlen(global_parity[i - k]) << endl;
            }
            else
            {
                local_parity[i - k - r] = new char[blocksize];
                // cout << "size of local_parity[" << i - k - r << "] : " << strlen(local_parity[i - k - r]) << endl;
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
    else if (decode_type == "sep-uni")
    {
        UNBALANCED_LRC unbalanced(k, r, p, blocksize);
        repair_ok = unbalanced.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
    }
    else if (decode_type == "sep-azure")
    {
        SEP_AZURE sep_azure(k, r, p, blocksize);
        repair_ok = sep_azure.muti_decode(fail_number, fail_node, data_ptrs, global_parity, local_parity);
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

int Proxy::check_error_fixed(int num, size_t *recv_len, size_t *expected_len,
                             int *node_id, int *&error_id, const std::vector<bool> &request_success)
{
    int fail_num = 0;
    std::vector<int> failed_nodes;

    for (int i = 0; i < num; i++)
    {
        // 首先检查请求是否成功
        if (!request_success[i])
        {
            cout << "Node " << node_id[i] << " request failed" << endl;
            failed_nodes.push_back(node_id[i]);
            fail_num++;
            continue;
        }

        // 检查数据长度是否匹配
        if (recv_len[i] != expected_len[i])
        {
            cout << "Node " << node_id[i] << " data length mismatch: expected "
                 << expected_len[i] << ", got " << recv_len[i] << endl;
            failed_nodes.push_back(node_id[i]);
            fail_num++;
        }
        else
        {
            cout << "Node " << node_id[i] << " data verification passed: "
                 << recv_len[i] << " bytes" << endl;
        }
    }

    // 分配错误节点数组
    if (fail_num > 0)
    {
        error_id = new int[fail_num];
        for (int i = 0; i < fail_num; i++)
        {
            error_id[i] = failed_nodes[i];
        }

        // 打印失败节点信息
        cout << "Detected " << fail_num << " failed nodes: ";
        for (int i = 0; i < fail_num; i++)
        {
            cout << error_id[i] << " ";
        }
        cout << endl;
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
    int ip_index = node_id / DATANODES_PER_IP;
    int port_index = node_id % DATANODES_PER_IP;
    if (port_index != 0)
    {
        ip_index++;
    }
    datanode_port = DATANODE_PORT_BASE + port_index;
    cout << "datanode_port: " << datanode_port << endl;

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

    if (ip_index >= 0 && ip_index < sizeof(datanode_ips) / sizeof(datanode_ips[0]))
    {
        des_ip = datanode_ips[ip_index];
    }
    else
    {
        std::cerr << "Invalid node_id: " << ip_index << std::endl;
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
            //   Use the extracted length as needed
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

void Proxy::SendBlockToDatanode(int node_id, char *data, size_t block_size)
{

    // print_data_info(data, block_size, "SendBlockToDatanode");
    Socket socket_datanode;
    std::string node_id_str = "node_id_" + std::to_string(node_id);
    std::string data_str(data, block_size); // 直接使用 data 和 block_size
    std::string buf_str = "SET " + node_id_str + " " + data_str;

    size_t chunk_size = buf_str.size();

    const auto &ips = getDatanodeIPs();
    if (ips.empty())
    {
        std::cerr << "Error: No datanode IPs configured" << std::endl;
        return;
    }

    int ip_index = node_id / DATANODES_PER_IP;
    ip_index = ip_index % ips.size(); // 防止越界
    const char *des_ip = ips[ip_index].c_str();
    int datanode_port = getDatanodePortByNodeId(node_id);

    std::cout << "Sending to node " << node_id
              << ", IP: " << des_ip
              << ", port: " << datanode_port << std::endl;

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
    // 丢包重发
    int data_sent_times = 0;
    int max_data_sent_times = 3; // 最多重发3次
    while (data_sent_times < max_data_sent_times)
    {
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
            data_sent_times++;
            continue; // 重试发送
        }
        // 添加发送确认机制
        char ack_buffer[16];
        if (recv(sock_data, ack_buffer, sizeof(ack_buffer), 0) > 0)
        {
            break; // 收到确认，退出重试循环
        }
        else
        {
            std::cerr << "Failed to receive acknowledgment from datanode, retrying..." << std::endl;
            data_sent_times++;
            continue; // 重试发送
        }
    }
    // cout << "Successfully sent " << bytes_sent << " bytes to datanode" << endl;
    close(sock_data);
}

void Proxy::GetRevData(char **data, int num, size_t *value_len, size_t *&recv_len, bool short_or_long)
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

        recv_thrds.emplace_back([this, num, recv_len, index, conffd_accept, &data, &value_len, short_or_long]()
                                { recv_len[index] = HandleGetRev(num, data[index], conffd_accept, value_len[index], short_or_long); });
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

size_t Proxy::HandleGetRev(int one_or_two, char *&data, int connfd, size_t value_len, bool short_or_long)
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
    ssize_t total_received = 0;
    if (one_or_two > 1)
    {
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
    }
    else if (one_or_two == 1 && !short_or_long)
    {
        // 如果是one_or_two == 1，直接接收数据
        data = new char[value_len + 1];
        memset(data, 0, value_len + 1);
        cout << "Expect data of size: " << value_len << endl;
        ssize_t remain_recv = value_len;
        while (total_received < (ssize_t)value_len)
        {
            ssize_t received = read(connfd, data + total_received, remain_recv);
            if (received < 0)
            {
                std::cerr << "read() error: " << strerror(errno) << std::endl;
                close(connfd);
                return -1;
            }
            total_received += received;
            remain_recv -= received;
        }
    }
    else if (short_or_long)
    {
        if (data == nullptr)
        {
            data = new char[value_len + 1];
            memset(data, 0, value_len + 1);
        }
        ssize_t receive = read(connfd, data, value_len);
        if (receive < 0)
        {
            std::cerr << "read() error: " << strerror(errno) << std::endl;
            close(connfd);
            return -1;
        }
        total_received = receive;
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

    if (destination == "coordinator" || chunk_size < 1024)
    // 如果是发送给coordinator或者数据量小于1KB，直接发送数据
    {
        if (send(sock, value.c_str(), value.size(), 0) < 0)
        {
            std::cerr << "Error: Failed to send data" << std::endl;
            close(sock);
            return;
        }
    }
    else if (destination == "client")
    {
        // 发送长度头 - 转换为网络字节序
        uint32_t data_length_net = htonl(chunk_size);
        if (write(sock, &data_length_net, sizeof(data_length_net)) != sizeof(data_length_net))
        {
            cerr << "Failed to send data length header" << endl;
            close(sock);
            return;
        }

        cout << "Sending " << chunk_size << " bytes to " << destination << endl;

        Socket socket_send_messg;
        if (socket_send_messg.SendDataReliably(sock, value.c_str(), chunk_size) == false)
        {
            std::cerr << "Failed to send data to datanode" << std::endl;
            close(sock);
            return;
        }
    }

    // cout << "Sent ack to client " << endl;
    close(sock);
}

// 修改后的GET请求处理，添加线程隔离和错误处理

// 修复后的ProcessGetRequests函数，增加更好的错误处理
bool Proxy::ProcessGetRequestsWithErrorHandling(char **data_get, size_t *&recv_len,
                                                const std::vector<std::string> &node_ids, const std::vector<int> &offsets,
                                                const std::vector<size_t> &value_lens, std::vector<bool> &request_success)
{

    int num = node_ids.size();
    request_success.resize(num, false);

    // 使用future来处理异步结果和超时
    std::vector<std::future<int>> get_request_futures;
    std::vector<std::thread> threads;
    std::vector<int> get_request_return(num, -1); // 初始化为失败状态

    cout << "Starting GET requests to " << num << " nodes..." << endl;

    // 启动GET请求线程（发送阶段）
    for (int i = 0; i < num; i++)
    {
        if (value_lens[i] == 0)
        {
            cout << "Skipping GET request for node " << node_ids[i] << " due to overlap information" << endl;
            get_request_return[i] = -1; // 标记为失败后续不必接收数据
            request_success[i] = false; // 标记为失败
            data_get[i] = nullptr;      // 不需要接收数据
            continue;                   // 跳过此请求
        }

        auto promise = std::make_shared<std::promise<int>>();
        get_request_futures.push_back(promise->get_future());

        threads.emplace_back([this, i, node_ids, offsets, value_lens, promise, &get_request_return]()
                             {
            try {
                const auto timeout_duration = std::chrono::seconds(15); // 减少超时时间
                
                //cout << "Sending GET request to node " << node_ids[i] << endl;
                int result = SendGetRequestWithTimeout(node_ids[i], offsets[i], value_lens[i], timeout_duration);
                get_request_return[i] = result;
                promise->set_value(result);
                
                if (result == 0) {
                    //cout << "GET request to node " << node_ids[i] << " succeeded" << endl;
                } else {
                    cout << "GET request to node " << node_ids[i] << " failed with code " << result << endl;
                }
            } catch (const std::exception& e) {
                cerr << "GET request thread " << i << " failed with exception: " << e.what() << endl;
                get_request_return[i] = -1;
                promise->set_value(-1);
            } catch (...) {
                cerr << "GET request thread " << i << " failed with unknown exception" << endl;
                get_request_return[i] = -1;
                promise->set_value(-1);
            } });
    }

    // 等待所有GET请求完成或超时
    WaitForRequestsWithTimeout(get_request_futures, std::chrono::seconds(20));

    // 安全地join所有线程
    for (auto &thread : threads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }

    // 统计成功的请求数量并更新request_success
    int successful_requests = 0;
    for (int i = 0; i < num; i++)
    {
        if (get_request_return[i] == 0)
        {
            successful_requests++;
            request_success[i] = true;
        }
        else
        {
            request_success[i] = false;
            cout << "Node " << node_ids[i] << " request failed" << endl;
        }
    }

    cout << "GET requests completed: " << successful_requests << "/" << num << " successful" << endl;

    // 接收数据阶段 - 只处理成功的请求
    if (successful_requests > 0)
    {
        // 初始化数据数组
        for (int i = 0; i < num; i++)
        {
            data_get[i] = nullptr;
        }

        // 接收数据，传入request_success信息
        GetRevDataWithErrorHandling(node_ids, data_get, num, value_lens.data(), recv_len, request_success);

        return successful_requests == num; // 只有所有请求都成功才返回true
    }

    return false;
}

// 带超时的GET请求发送
int Proxy::SendGetRequestWithTimeout(const std::string &node_id, int offset, size_t value_len, std::chrono::seconds timeout)
{
    auto start_time = std::chrono::steady_clock::now();

    try
    {
        // 创建连接
        int sock = CreateConnectionWithTimeout(node_id, timeout);
        if (sock < 0)
        {
            std::cerr << "Failed to create connection for node " << node_id << std::endl;
            return -1;
        }

        // 检查是否超时
        auto elapsed = std::chrono::steady_clock::now() - start_time;
        if (elapsed >= timeout)
        {
            std::cerr << "Timeout before sending request for node " << node_id << std::endl;
            close(sock);
            return -1;
        }

        // 发送请求
        std::string command = "GET " + node_id + " offset_" + std::to_string(offset) +
                              " value_len_" + std::to_string(value_len);

        uint32_t data_length_net = htonl(command.size());

        // 发送长度头
        if (send(sock, &data_length_net, sizeof(data_length_net), MSG_NOSIGNAL) != sizeof(data_length_net))
        {
            std::cerr << "Failed to send command length for node " << node_id << std::endl;
            close(sock);
            return -1;
        }

        // 发送命令数据
        if (send(sock, command.c_str(), command.size(), MSG_NOSIGNAL) != (ssize_t)command.size())
        {
            std::cerr << "Failed to send command for node " << node_id << std::endl;
            close(sock);
            return -1;
        }

        // std::cout << "Successfully sent GET request to node " << node_id << std::endl;
        close(sock);
        return 0;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception in SendGetRequestWithTimeout for node " << node_id
                  << ": " << e.what() << std::endl;
        return -1;
    }
}

// 创建带超时的连接
int Proxy::CreateConnectionWithTimeout(string node_id, std::chrono::seconds timeout)
{
    int node_id_int = std::stoi(node_id.substr(8));
    const auto &ips = getDatanodeIPs();
    if (ips.empty())
    {
        std::cerr << "Error: No datanode IPs configured" << std::endl;
        return -1;
    }

    int ip_index = node_id_int / DATANODES_PER_IP;
    ip_index = ip_index % ips.size(); // 防止越界
    const char *des_ip = ips[ip_index].c_str();
    int datanode_port = getDatanodePortByNodeId(node_id_int);

    std::cout << "Sending to node " << node_id_int
              << ", IP: " << des_ip
              << ", port: " << datanode_port << std::endl;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
    {
        return -1;
    }

    // 设置非阻塞模式
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    struct sockaddr_in remote_addr;
    bzero(&remote_addr, sizeof(remote_addr));
    remote_addr.sin_family = AF_INET;
    remote_addr.sin_port = htons(datanode_port);

    if (inet_aton(des_ip, &remote_addr.sin_addr) == 0)
    {
        cout << "dest ip: " << des_ip << endl;
        perror("inet_aton fail!");
        close(sock);
        return -1;
    }

    int result = connect(sock, (struct sockaddr *)&remote_addr, sizeof(remote_addr));

    if (result == 0)
    {
        // 立即连接成功
        fcntl(sock, F_SETFL, flags); // 恢复阻塞模式
        return sock;
    }

    if (errno != EINPROGRESS)
    {
        close(sock);
        return -1;
    }

    // 使用select等待连接完成
    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(sock, &write_fds);

    struct timeval tv;
    tv.tv_sec = timeout.count();
    tv.tv_usec = 0;

    int select_result = select(sock + 1, nullptr, &write_fds, nullptr, &tv);

    if (select_result <= 0)
    {
        // 超时或错误
        close(sock);
        return -1;
    }

    // 检查连接是否真的成功
    int sock_error;
    socklen_t len = sizeof(sock_error);
    if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &sock_error, &len) < 0 || sock_error != 0)
    {
        close(sock);
        return -1;
    }

    // 恢复阻塞模式
    fcntl(sock, F_SETFL, flags);
    return sock;
}

// 等待请求完成或超时
void Proxy::WaitForRequestsWithTimeout(std::vector<std::future<int>> &futures, std::chrono::seconds timeout)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;

    for (size_t i = 0; i < futures.size(); i++)
    {
        auto remaining_time = deadline - std::chrono::steady_clock::now();

        if (remaining_time <= std::chrono::seconds(0))
        {
            std::cerr << "Timeout waiting for GET request " << i << std::endl;
            break;
        }

        auto status = futures[i].wait_for(remaining_time);
        if (status == std::future_status::timeout)
        {
            std::cerr << "GET request " << i << " timed out" << std::endl;
        }
        else if (status == std::future_status::ready)
        {
            try
            {
                int result = futures[i].get();
                // std::cout << "GET request " << i << " completed with result: " << result << std::endl;
            }
            catch (const std::exception &e)
            {
                std::cerr << "GET request " << i << " threw exception: " << e.what() << std::endl;
            }
        }
    }
}

// 带错误处理的数据接收函数
void Proxy::GetRevDataWithErrorHandling(const std::vector<std::string> &node_ids, char **data, int num, const size_t *value_lens,
                                        size_t *&recv_len, const std::vector<bool> &request_success)
{
    recv_len = new size_t[num];
    std::fill(recv_len, recv_len + num, 0);

    // 计算预期的成功连接数
    int expected_connections = 0;
    std::vector<int> success_node_indices; // 记录成功请求的节点索引

    for (int i = 0; i < num; i++)
    {
        if (request_success[i])
        {
            // success_node_indices.push_back(stoi(node_ids[i].substr(8))); // 假设node_ids格式为"node_id_X"
            success_node_indices.push_back(i);
            expected_connections++;
        }
    }

    if (expected_connections == 0)
    {
        cout << "No successful GET requests, skipping data reception" << endl;
        return;
    }

    cout << "Expecting " << expected_connections << " data connections" << endl;

    std::vector<std::thread> recv_threads;
    std::atomic<int> connections_handled(0);
    std::mutex accept_mutex;

    // 为每个成功的请求启动接收线程
    for (int thread_idx = 0; thread_idx < expected_connections; thread_idx++)
    {
        recv_threads.emplace_back([this, data, recv_len, value_lens, num,
                                   &connections_handled, &accept_mutex,
                                   &success_node_indices, thread_idx]()
                                  {
    try {
    int connfd = -1;

    // 线程安全的accept
    {
        std::lock_guard<std::mutex> lock(accept_mutex);

        struct timeval timeout;
        timeout.tv_sec = 20;
        timeout.tv_usec = 0;
        setsockopt(sock_proxy_data, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

     connfd = accept(sock_proxy_data, nullptr, nullptr);
    }

    if (connfd < 0) {
    cerr << "Accept failed in receive thread: " << strerror(errno) << endl;
    return;
    }

// 使用正确的节点索引
    int node_index = success_node_indices[thread_idx];

    //cout << "Thread " << thread_idx << " receiving data for node index " << node_index << endl;

    recv_len[node_index] = HandleGetRevWithIsolation(data[node_index], connfd, 
                  value_lens[node_index], node_index);
    connections_handled++;

    close(connfd);

    } catch (const std::exception& e) {
    cerr << "Exception in receive thread: " << e.what() << endl;
    } catch (...) {
    cerr << "Unknown exception in receive thread" << endl;
    } });
    }

    // 等待所有接收操作完成
    for (auto &thread : recv_threads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }

    cout << "Data reception completed: " << connections_handled.load()
         << "/" << expected_connections << " connections handled" << endl;
    // resort_recvdata(data, recv_len, num);
}

// 带隔离的数据接收处理
size_t Proxy::HandleGetRevWithIsolation(char *&data, int connfd, size_t value_len, int node_index)
{
    try
    {
        if (connfd < 0)
        {
            std::cerr << "Invalid file descriptor for node " << node_index << ": " << connfd << std::endl;
            return 0;
        }

        // 设置socket超时
        struct timeval timeout;
        timeout.tv_sec = 30; // 30秒超时
        timeout.tv_usec = 0;
        setsockopt(connfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        // 接收文件长度头
        uint32_t data_length_net;
        ssize_t header_received = recv(connfd, &data_length_net, sizeof(data_length_net), MSG_WAITALL);

        if (header_received != sizeof(data_length_net))
        {
            std::cerr << "Node " << node_index << ": Failed to receive data length header, received: "
                      << header_received << std::endl;
            return 0;
        }

        size_t expected_size = ntohl(data_length_net);
        std::cout << "Node " << node_index << ": Expected to receive " << expected_size << " bytes" << std::endl;

        // 验证数据长度合理性
        if (expected_size == 0 || expected_size > 1024 * 1024 * 100)
        {
            std::cerr << "Node " << node_index << ": Invalid data size: " << expected_size << std::endl;
            return 0;
        }

        // 分配内存并接收数据
        data = new char[expected_size + 1];
        memset(data, 0, expected_size + 1);

        size_t total_received = 0;
        auto start_time = std::chrono::steady_clock::now();
        const auto max_receive_time = std::chrono::seconds(30);

        while (total_received < expected_size)
        {
            // 检查超时
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            if (elapsed >= max_receive_time)
            {
                std::cerr << "Node " << node_index << ": Receive timeout after "
                          << elapsed.count() << " seconds" << std::endl;
                break;
            }

            size_t remaining = expected_size - total_received;
            ssize_t received = recv(connfd, data + total_received, remaining, 0);

            if (received == 0)
            {
                std::cout << "Node " << node_index << ": Connection closed by peer (received "
                          << total_received << "/" << expected_size << " bytes)" << std::endl;
                break;
            }
            else if (received < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    std::cout << "Node " << node_index << ": Timeout, retrying..." << std::endl;
                    continue;
                }
                std::cerr << "Node " << node_index << ": recv() error: " << strerror(errno) << std::endl;
                break;
            }

            total_received += received;
        }

        std::cout << "Node " << node_index << ": Received data size: " << total_received << std::endl;
        return total_received;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception in HandleGetRevWithIsolation for node " << node_index
                  << ": " << e.what() << std::endl;
        return 0;
    }
    catch (...)
    {
        std::cerr << "Unknown exception in HandleGetRevWithIsolation for node " << node_index << std::endl;
        return 0;
    }
}

int main()
{
    g_config = std::make_unique<ConfigReader>();

    // 加载配置文件
    if (!g_config->loadConfig("../include/config.xml"))
    {
        std::cerr << "Failed to load configuration file!" << std::endl;
        return -1;
    }

    ClientServer::Proxy proxy;
    proxy.StartProxy_Data();
    proxy.StartProxy();
}