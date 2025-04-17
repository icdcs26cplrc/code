// client.cpp
#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "../include/client.hh"

using namespace std;
using namespace ClientServer;

void Client::FileToKeyValue(std::string file_path,std::string key,std::string value){
    // 打开文件
    ifstream file(file_path);
    if (!file.is_open()) {
        cerr << "Error: Unable to open file " << file_path << endl;
        return;
    }

    // 读取文件内容并生成key-value对
    string content((istreambuf_iterator<char>(file)), istreambuf_iterator<char>());
    file.close();

    // 简单生成key-value对，key为文件名，value为文件内容
    size_t lastSlash = file_path.find_last_of("/\\");
    key = (lastSlash == string::npos) ? file_path : file_path.substr(lastSlash + 1);
    value = content;
}
bool Client::set(std::string key, std::string value) {
    // 1. 连接Coordinator
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in coordAddr{};
    coordAddr.sin_family = AF_INET;
    coordAddr.sin_port = htons(COORDINATOR_PORT); // Coordinator端口
    inet_pton(AF_INET, "127.0.0.1", &coordAddr.sin_addr);
    connect(sock, (sockaddr*)&coordAddr, sizeof(coordAddr));

    // 2. 向coordinator发送SET指令
    std::string cmd = "SET " + key + " " + value + "\n";

    // 发送命令内容
    send(sock, cmd.c_str(), cmd.size(), 0);

    // 接收响应
    
    // 关闭Proxy连接
    
}

int main(){
Client client;
//输入请求
cout<<"Please input the command: "<<"including set, get, repair\n"
    <<"for example: set file_path\n"
    <<"get file\n"
    <<"repair (which block)"<<endl;
string command;
//解析command
getline(cin, command);
std::string key;
std::string value;
if (command.find("set") == 0) {
    string file_path = command.substr(4);
    client.FileToKeyValue(file_path,key,value);
    client.set(key, value);
} else if (command.find("get") == 0) {
    string file_name = command.substr(4);
    string value;
    client.get(file_name, value);
} else if (command.find("repair") == 0) {
    vector<int> failed_node_list;
    // 解析失败节点列表
    client.repair(failed_node_list);
} else {
    cout << "Invalid command" << endl;
}
