// datanode.hh
#ifndef DATANODE_HH
#define DATANODE_HH
#include  "common.hh"
#include  "metadata.hh"
#include  "coordinator.hh"
#include  "client.hh"


namespace ClientServer {
class DataNode {
public:
    DataNode();
    void StartListen_data();
    void RunServer_data();
    void RevData(int coordinator, char* buf, size_t max_len);
    void HandleConnection_data(int client_fd);
    void ProcessSet_data(int fd, const std::string& value);
    void ProcessGet_data(int fd, const std::string& key);
    
    
};
}
#endif // DATANODE_HH