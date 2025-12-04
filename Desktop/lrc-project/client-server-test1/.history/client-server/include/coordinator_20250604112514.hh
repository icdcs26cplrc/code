#ifndef COORDINATOR_HH
#define COORDINATOR_HH

#include "socket.hh"
#include "common.hh"
#include "metadata.hh"
#include "client.hh"
#include "datanode.hh"
#include"../include/lrc/azure-lrc.hh"
#include"../include/lrc/azure-lrc_1.hh"
#include"../include/lrc/grade-lrc.hh"
#include"../include/lrc/optimal-lrc.hh"
#include"../include/lrc/unbalaced-lrc.hh"
#include"../include/lrc/uniform-lrc.hh"
#include"../include/lrc/lrc_base.hh"
#include "config_defination.hh"
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

    void StoreDataInBuffer(const std::string& key, const std::string& value,int k, int r, int p,size_t value_len);

    void StartListen();
    ssize_t RecvCommand(int client_fd, char* buf, size_t max_len,size_t packet_size); ;
    void RunServer();
    void HandleConnection(int client_fd);

    void SendBlockToDatanode(int datanode_port,int node_id,const char* data) ;
    void SendGetRequest(int dataport, string node_id, int offset,size_t value_len);
    void GetRevData(char** buf,int num,size_t *buf_size);
    void HandleGetRev(char *&data,int connfd,size_t value_len);
    void SendValueToClient( std::string value);
    void SendCommandToProxy(int port,std::string value);

    bool ProcessSet(const std::string& key, const std::string& value);
    void ProcessGet(const std::string& key);
    void ProcessRepair(std::string des,std::string failed_block_number,std::string failed_block);
    int ParseFailMassage(string failed_block_number,string failed_block,int*&stripe_id,int*&node_id);
    void StartProxy();


    ECSchema ecschema_cordinate_;
private:
    
    int max_stripe_boundary_;
    buffer_node buffer_node_;
    vector<ObjectItem> object_list_;
    vector<BlockItem> block_list_;
    vector<StripeItem> stripe_list_;
    vector<NodeItem> node_list_;
    int listen_fd_ = -1;
    int sock_proxy = -1;
    std::unordered_map<int, std::string> recv_buffers_; // 每个连接的接收缓冲区
    constexpr static char CMD_DELIMITER = '\n';

    void SendRequestToDatanode(int dataport,size_t offset);
    int InitClient_Cor();
    
};
} // namespace ClientServer
#endif // COORDINATOR_HH