#include "../include/datanode.hh"

int main(int argc, char* argv[]) {
    if (argc != 8) {
        std::cerr << "Usage: " << argv[0] << " <node_id> <ip_index> <port_index> <k> <r> <p> <block_size>" << std::endl;
        return 1;
    }
    
    int node_id = std::stoi(argv[1]);
    int ip_index = std::stoi(argv[2]);
    int port_index = std::stoi(argv[3]);
    int k = std::stoi(argv[4]);
    int r = std::stoi(argv[5]);
    int p = std::stoi(argv[6]);
    size_t block_size = std::stoull(argv[7]);
    
    // load configuration
    g_config = std::make_unique<ConfigReader>();
    if (!g_config->loadConfig("../include/config.xml")) {
        std::cerr << "Failed to load configuration file!" << std::endl;
        return -1;
    }
    
    // start datanode
    auto datanode = std::make_unique<DataNode>();
    datanode->block_size = block_size;
    datanode->SetNodeList(node_id);
    
    std::vector<std::string> ips = getDatanodeIPs();
    std::vector<int> ports = getDatanodePorts();
    
    datanode->Start(ports[port_index], "0.0.0.0");
    
    return 0;
}