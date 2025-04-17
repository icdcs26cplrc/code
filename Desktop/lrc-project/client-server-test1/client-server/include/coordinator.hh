#ifndef COORDINATOR_HH
#define COORDINATOR_HH


#include "common.hh"
#include "metadata.hh"
#include "grade-lrc.hh"
#include "client.hh"
#include "datanode.hh"
#include <mutex>


namespace ClientServer {


struct buffer_node
{
    StripeItem stripe;
    size_t current_size;
    std::string  data;
};
class Coordinator {

public:
    size_t block_size;
    Coordinator();

    void SetMaxStripeBoundary(ECSchema ecschema) ;
    int GetMaxStripeBoundary(); 
    void SetBufferNode(int block_size);
    void GetBufferNode();
    void SetObjectList();
    vector<ObjectItem> GetObjectList();
    void SetBlockList();
    vector<BlockItem> GetBlockList();
    void SetStripeList();
    vector<StripeItem> GetStripeList();
    void SetNodeList(int k,int r,int p);
    vector<NodeItem> GetNodeList();
    void SetEcschema(ECSchema ecschema);
    ECSchema GetEcschema();

    void StartListen();
    ssize_t RecvCommand(int client_fd, char* buf, size_t max_len) ;
    void RunServer();
    void HandleConnection(int client_fd);

    


    bool ProcessSet(int fd, const std::string& key, const std::string& value);
    void ProcessGet(int fd, const std::string& key);
    void ProcessRepair(int fd, const std::vector<int>& failed_node);


    ECSchema ecschema_;
private:
    
    int max_stripe_boundary_;
    buffer_node buffer_node_;
    vector<ObjectItem> object_list_;
    vector<BlockItem> block_list_;
    vector<StripeItem> stripe_list_;
    vector<NodeItem> node_list_;
    int listen_fd_ = -1;
    std::unordered_map<int, std::string> recv_buffers_; // 每个连接的接收缓冲区
    constexpr static char CMD_DELIMITER = '\n';
};
} // namespace ClientServer
#endif // COORDINATOR_HH