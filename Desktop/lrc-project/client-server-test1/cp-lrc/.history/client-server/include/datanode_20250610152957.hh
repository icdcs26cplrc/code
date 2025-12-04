// datanode.hh
#ifndef DATANODE_HH
#define DATANODE_HH
#include "socket.hh"
#include  "common.hh"
#include  "metadata.hh"
#include  "coordinator.hh"
#include  "client.hh"
#include "config_defination.hh"
#include <shared_mutex>  

namespace ClientServer {
class DataNode {
public:
    typedef struct NodeStorage
    {
        std::string node_id;
        std::string value;
        std::shared_mutex mutex; 

        NodeStorage(const std::string& id, const std::string& val)
        : node_id(id), value(val) {}
    } NodeStorage;
    std::mutex mtx;
    size_t block_size;
    mutable std::shared_mutex node_list_mutex_;   // 读写锁优化

    DataNode();
    //void Start(int port); // 合并监听和运行逻辑
    void Stop();
    void StartListen_data(int dataport);
    void RunServer_data(int port);
    ssize_t RevData(int client_fd, char* buf, size_t max_len);//,size_t packet_size);
    void HandleConnection_data(int client_fd);
    void ProcessSet_data(std::string node_id ,const std::string  value);
    void ProcessGet_data(std::string node_id , const std::string key);
    void ProcessRepair_data(std::string node_id, const std::string offset);
    void SetNodeList(int node_id_number);
    void SendValueToProxy(std::string node_id, std::string value);
    const std::vector<std::unique_ptr<NodeStorage>>& GetNodeList() const;
    //void StartSingleDataNode(int node_index, int k, int r, int p, size_t block_size);
    void signal_handler(int sig);
    static void graceful_shutdown_handler(int sig) ;
    void StartSingleDataNode(int global_node_id, int ip_index, int local_port_index, int k, int r, int p, size_t block_size);
    bool IsProcessAlive(pid_t pid);
    void CleanupResources();
    bool HealthCheck();
    void Start(int port, const std::string& bind_ip = "") ;
    
    
private:
    int listen_fd_data=-1;
    std::unordered_map<int, std::string> recv_buffers_data; // 每个连接的接收缓冲区
    constexpr static char CMD_DELIMITER_data = '\n';
    std::thread server_thread_; // 监听线程
    std::vector<std::unique_ptr<NodeStorage>> node_storage_list_;
    std::mutex log_mutex_; 
    static std::mutex io_mutex_;      // 保护IO操作
}; // namespace ClientServer
}

#endif // DATANODE_HH