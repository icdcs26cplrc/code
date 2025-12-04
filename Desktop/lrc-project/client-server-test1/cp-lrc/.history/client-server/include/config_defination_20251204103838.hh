#ifndef CONFIG_DEFINITIONS_H
#define CONFIG_DEFINITIONS_H

#include "ConfigReader.hh"
#include <vector>
#include <string>

// ============ 端口配置 ============
// 注意：你需要在配置文件中添加这些端口配置
#define CLIENT_PORT GET_CONFIG_INT("client.port", 12345)
#define COORDINATOR_PORT GET_CONFIG_INT("coordinator.port", 12378)
//#define PROXY_PORT_BASE GET_CONFIG_INT("proxy.port.base", 23456)
#define DATANODE_PORT_BASE GET_CONFIG_INT("datanode.port.base", 34567)
#define PROXY_PORT GET_CONFIG_INT("proxy.port", 34589)
#define PROXY_PORT_DATA GET_CONFIG_INT("proxy.port.data", 35645)

// ============ IP地址配置 ============
#define CLIENT_IP GET_CONFIG_STRING("client.addr", "127.0.0.1").c_str()
#define COORDINATOR_IP GET_CONFIG_STRING("coordinator.addr", "127.0.0.1").c_str()
#define PROXY_IP GET_CONFIG_STRING("proxy.addr", "127.0.0.1").c_str()


// 每个IP启动的DataNode数量
#define DATANODES_PER_IP 10
// 获取数据节点IP列表的辅助函数
inline std::vector<std::string> getDatanodeIPs() {
    static std::vector<std::string> datanode_ips;
    static bool initialized = false;
    
    if (!initialized && g_config) {
        datanode_ips = g_config->getStringList("datanode.addr");
        //cout<<"datanode_ips.size() = "<<datanode_ips.size()<<endl;
        // 确保至少有14个IP地址
        while (datanode_ips.size() < 14) {
            datanode_ips.push_back("127.0.0.1");
        }
        initialized = true;
    }
    
    return datanode_ips;
}

// 获取数据节点端口列表（每个IP使用8个连续端口）
inline std::vector<int> getDatanodePorts() {
    static std::vector<int> datanode_ports;
    static bool initialized = false;
    
    if (!initialized && g_config) {
        std::vector<std::string> port_strings = g_config->getStringList("datanode.ports");
        
        if (!port_strings.empty() && port_strings.size() >= DATANODES_PER_IP) {
            // 使用配置文件中的端口列表，只取前8个作为基础端口
            for (int i = 0; i < DATANODES_PER_IP && i < static_cast<int>(port_strings.size()); i++) {
                try {
                    datanode_ports.push_back(std::stoi(port_strings[i]));
                } catch (const std::exception& e) {
                    // 解析失败时使用默认值
                    datanode_ports.push_back(34567 + i);
                }
            }
        } else {
            // 如果没有配置端口列表，使用基础端口生成8个连续端口
            int base_port = GET_CONFIG_INT("datanode.port.base", 34567);
            for (int i = 0; i < DATANODES_PER_IP; ++i) {
                datanode_ports.push_back(base_port + i);
            }
        }
        
        initialized = true; 
    }
    
    return datanode_ports;
}

// 根据全局node_id获取对应的IP地址
inline const char* getDatanodeIPByNodeId(int global_node_id) {
    const auto& ips = getDatanodeIPs();
    int ip_index = global_node_id / DATANODES_PER_IP;  // 每8个节点使用一个IP
    
    if (ip_index >= 0 && ip_index < static_cast<int>(ips.size())) {
        return ips[ip_index].c_str();
    }
    
    // 如果超出范围，使用模运算循环
    if (!ips.empty()) {
        return ips[ip_index % ips.size()].c_str();
    }
    
    return "127.0.0.1";
}

// 根据全局node_id获取对应的端口
inline int getDatanodePortByNodeId(int global_node_id) {
    const auto& ports = getDatanodePorts();
    int port_index = global_node_id % DATANODES_PER_IP;  
    
    if (port_index >= 0 && port_index < static_cast<int>(ports.size())) {
        return ports[port_index];
    }
    
    // 如果超出范围，使用默认端口
    return 34567 + port_index;
}

// 根据IP索引和本地端口索引获取全局node_id
inline int getGlobalNodeId(int ip_index, int local_port_index) {
    return ip_index * DATANODES_PER_IP + local_port_index;
}

// 根据全局node_id获取IP索引
inline int getIPIndex(int global_node_id) {
    return global_node_id / DATANODES_PER_IP;
}

// 根据全局node_id获取本地端口索引
inline int getLocalPortIndex(int global_node_id) {
    return global_node_id % DATANODES_PER_IP;
}

// 兼容性函数：根据索引获取IP和端口（保持向后兼容）
inline const char* getDatanodeIP(int index) {
    return getDatanodeIPByNodeId(index);
}

inline int getDatanodePort(int index) {
    return getDatanodePortByNodeId(index);
}

// 获取最大数据节点数量 (14 * 8 = 112)
inline int getMaxDatanodeCount() {
    const auto& ips = getDatanodeIPs();
    return static_cast<int>(ips.size()) * DATANODES_PER_IP;
}

// 获取IP数量
inline int getIPCount() {
    return static_cast<int>(getDatanodeIPs().size());
}

// 数据节点IP宏定义（保持兼容性）

#define DATANODE_IP_0 getDatanodeIPs()[0].c_str()
#define DATANODE_IP_1 getDatanodeIPs()[1].c_str()
#define DATANODE_IP_2 getDatanodeIPs()[2].c_str()
#define DATANODE_IP_3 getDatanodeIPs()[3].c_str()
#define DATANODE_IP_4 getDatanodeIPs()[4].c_str()
#define DATANODE_IP_5 getDatanodeIPs()[5].c_str()
#define DATANODE_IP_6 getDatanodeIPs()[6].c_str()
#define DATANODE_IP_7 getDatanodeIPs()[7].c_str()
#define DATANODE_IP_8 getDatanodeIPs()[8].c_str()
#define DATANODE_IP_9 getDatanodeIPs()[9].c_str()
#define DATANODE_IP_10 getDatanodeIPs()[10].c_str()
#define DATANODE_IP_11 getDatanodeIPs()[11].c_str()
#define DATANODE_IP_12 getDatanodeIPs()[12].c_str()
#define DATANODE_IP_13 getDatanodeIPs()[13].c_str()
#define DATANODE_IP_14 getDatanodeIPs()[14].c_str()
#define DATANODE_IP_15 getDatanodeIPs()[15].c_str()

// 数据节点端口宏定义（新增）
#define DATANODE_PORT_0 getDatanodePort(0)
#define DATANODE_PORT_1 getDatanodePort(1)
#define DATANODE_PORT_2 getDatanodePort(2)
#define DATANODE_PORT_3 getDatanodePort(3)
#define DATANODE_PORT_4 getDatanodePort(4)
#define DATANODE_PORT_5 getDatanodePort(5)
#define DATANODE_PORT_6 getDatanodePort(6)
#define DATANODE_PORT_7 getDatanodePort(7)
#define DATANODE_PORT_8 getDatanodePort(8)
#define DATANODE_PORT_9 getDatanodePort(9)
// #define DATANODE_PORT_10 getDatanodePort(10)
// #define DATANODE_PORT_11 getDatanodePort(11)
// #define DATANODE_PORT_12 getDatanodePort(12)
// #define DATANODE_PORT_13 getDatanodePort(13)
// #define DATANODE_PORT_14 getDatanodePort(14)
// #define DATANODE_PORT_15 getDatanodePort(15)

// ============ 其他配置访问宏 ============
#define CONTROLLER_THREAD_NUM GET_CONFIG_INT("controller.thread.num", 20)
#define AGENT_THREAD_NUM GET_CONFIG_INT("agent.thread.num", 20)
#define CMDDIST_THREAD_NUM GET_CONFIG_INT("cmddist.thread.num", 10)
#define LOCAL_ADDR GET_CONFIG_STRING("local.addr", "127.0.0.1")
#define BLOCK_DIRECTORY GET_CONFIG_STRING("block.directory", "/tmp/blkDir")
#define STRIPESTORE_DIRECTORY GET_CONFIG_STRING("stripestore.directory", "/tmp/stripeStore")
#define TRADEOFFPOINT_DIRECTORY GET_CONFIG_STRING("tradeoffpoint.directory", "/tmp/tradeoffPoint")
#define PARARC_MODE GET_CONFIG_STRING("pararc.mode", "standalone")
#define FULLNODE_ADDR GET_CONFIG_STRING("fullnode.addr", "127.0.0.1")

#endif // CONFIG_DEFINITIONS_H