// datanode.cc
#include "../include/datanode.hh"


using namespace ClientServer;

std::unordered_map<std::string, std::string> data_store;
std::mutex data_store_mutex;

void DataNode::StartListen_data() {
    std::thread server_thread(RunServer_data);
    server_thread.detach();
}

void DataNode::RunServer_data() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("Socket creation failed");
        return;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(8080);

    if (::bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        perror("Bind failed");
        close(server_fd);
        return;
    }

    if (listen(server_fd, 10) == -1) {
        perror("Listen failed");
        close(server_fd);
        return;
    }

    std::cout << "Server listening on port 8080..." << std::endl;

    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd == -1) {
            perror("Accept failed");
            continue;
        }
        std::thread client_thread(HandleConnection_data, client_fd);
        client_thread.detach();
    }
}

void DataNode::RevData(int coordinator, char* buf, size_t max_len) {
    ssize_t bytes_received = recv(coordinator, buf, max_len, 0);
    if (bytes_received == -1) {
        perror("Receive failed");
    } else {
        buf[bytes_received] = '\0';
    }
}

void DataNode::HandleConnection_data(int client_fd) {
    char buffer[1024];
    RevData(client_fd, buffer, sizeof(buffer) - 1);

    std::string request(buffer);
    if (request.compare(0, 4, "SET ") == 0) {
        size_t space_pos = request.find(' ', 4);
        if (space_pos != std::string::npos) {
            std::string key = request.substr(4, space_pos - 4);
            std::string value = request.substr(space_pos + 1);
            ProcessSet_data(client_fd, value);
        }
    } else if (request.compare(0, 4, "GET ") == 0) {
        std::string key = request.substr(4);
        ProcessGet_data(client_fd, key);
    } else {
        std::string response = "ERROR: Unknown command\n";
        send(client_fd, response.c_str(), response.size(), 0);
    }

    close(client_fd);
}

void DataNode::ProcessSet_data(int fd, const std::string& value) {
    std::lock_guard<std::mutex> lock(data_store_mutex);
    size_t space_pos = value.find(' ');
    if (space_pos != std::string::npos) {
        std::string key = value.substr(0, space_pos);
        std::string val = value.substr(space_pos + 1);
        data_store[key] = val;
        std::string response = "OK\n";
        send(fd, response.c_str(), response.size(), 0);
    } else {
        std::string response = "ERROR: Invalid SET format\n";
        send(fd, response.c_str(), response.size(), 0);
    }
}

void DataNode::ProcessGet_data(int fd, const std::string& key) {
    std::lock_guard<std::mutex> lock(data_store_mutex);
    auto it = data_store.find(key);
    if (it != data_store.end()) {
        std::string response = "VALUE " + it->second + "\n";
        send(fd, response.c_str(), response.size(), 0);
    } else {
        std::string response = "ERROR: Key not found\n";
        send(fd, response.c_str(), response.size(), 0);
    }
}

int main(){
    Coordinator coordinator_data;
    int datanode_number =coordinator_data.ecschema_.k_datablock + coordinator_data.ecschema_.r_gobalparityblock + coordinator_data.ecschema_.p_localgroup;
    std::vector<DataNode> datanodes(datanode_number);
    for (int i = 0; i < datanode_number; i++)
    {
        datanodes[i].StartListen_data();
    }

    
    
    
    
}

