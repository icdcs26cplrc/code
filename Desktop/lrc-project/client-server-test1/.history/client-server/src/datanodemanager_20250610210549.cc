#include "../include/datanode.hh"
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include <vector>
#include <map>
#include <iostream>
#include <algorithm>

using namespace ClientServer;

// 全局变量用于管理所有子进程
static std::map<int, std::vector<pid_t>> g_ip_process_map; // IP索引 -> 该IP下的进程PID列表
static std::vector<pid_t> g_all_child_pids; // 所有子进程PID
static bool g_shutdown_requested = false;

// 信号处理函数
void cluster_signal_handler(int sig) {
    std::cout << "\nReceived signal " << sig << ", shutting down all DataNode processes..." << std::endl;
    g_shutdown_requested = true;
    
    // 优雅关闭所有子进程
    for (pid_t child_pid : g_all_child_pids) {
        if (kill(child_pid, SIGTERM) == 0) {
            std::cout << "Sent SIGTERM to process " << child_pid << std::endl;
        }
    }
    
    // 等待一段时间让进程优雅退出
    sleep(2);
    
    // 强制终止仍未退出的进程
    for (pid_t child_pid : g_all_child_pids) {
        if (kill(child_pid, 0) == 0) { // 进程仍然存在
            std::cout << "Force killing process " << child_pid << std::endl;
            kill(child_pid, SIGKILL);
        }
    }
    
    exit(0);
}

// 集群管理器类
class DataNodeClusterManager {
private:
    int total_datanodes_;
    int required_ips_;
    std::vector<std::string> datanode_ips_;
    std::vector<int> datanode_ports_;
    
public:
    DataNodeClusterManager(int k, int r, int p) {
        total_datanodes_ = k + r + p;
        
        // 计算需要的IP数量
        required_ips_ = (total_datanodes_ + DATANODES_PER_IP - 1) / DATANODES_PER_IP;
        
        // 获取配置的IP和端口列表
        datanode_ips_ = getDatanodeIPs();
        datanode_ports_ = getDatanodePorts();
        
        std::cout << "Cluster Configuration:" << std::endl;
        std::cout << "  Total DataNodes needed: " << total_datanodes_ << std::endl;
        std::cout << "  Required IPs: " << required_ips_ << std::endl;
        std::cout << "  Available IPs: " << datanode_ips_.size() << std::endl;
        std::cout << "  Ports per IP: " << DATANODES_PER_IP << std::endl;
        
        if (required_ips_ > static_cast<int>(datanode_ips_.size())) {
            throw std::runtime_error("Not enough IP addresses configured for required DataNodes");
        }
    }
    
    // 启动集群中的所有DataNode
    bool StartCluster(int k, int r, int p, size_t block_size) {
        std::cout << "\n=== Starting DataNode Cluster ===" << std::endl;
        
        int global_node_id = 0;
        
        // 遍历需要的IP地址
        for (int ip_index = 0; ip_index < required_ips_; ip_index++) {
            std::vector<pid_t> ip_processes;
            
            // 计算当前IP需要启动的DataNode数量
            int nodes_for_this_ip = std::min(DATANODES_PER_IP, 
                                           total_datanodes_ - ip_index * DATANODES_PER_IP);
            
            std::cout << "\nStarting " << nodes_for_this_ip 
                      << " DataNodes on IP " << datanode_ips_[ip_index] 
                      << " (IP index: " << ip_index << ")" << std::endl;
            
            // 在当前IP上启动DataNode进程
            for (int local_port_index = 0; local_port_index < nodes_for_this_ip; local_port_index++) {
                pid_t pid = fork();
                
                if (pid < 0) {
                    std::cerr << "Fork failed for DataNode " << global_node_id << std::endl;
                    // 清理已创建的进程
                    CleanupProcesses();
                    return false;
                }
                else if (pid == 0) {
                    // 子进程：启动单个DataNode
                    StartSingleDataNodeProcess(global_node_id, ip_index, local_port_index, 
                                             k, r, p, block_size);
                    exit(0); // 子进程不应该到达这里
                }
                else {
                    // 父进程：记录子进程信息
                    ip_processes.push_back(pid);
                    g_all_child_pids.push_back(pid);
                    
                    std::cout << "  Created DataNode " << global_node_id 
                              << " (PID: " << pid 
                              << ", IP: " << datanode_ips_[ip_index]
                              << ", Port: " << datanode_ports_[local_port_index] << ")" << std::endl;
                    
                    global_node_id++;
                    
                    // 短暂延迟避免端口绑定冲突
                    usleep(50000); // 50ms
                }
            }
            
            g_ip_process_map[ip_index] = ip_processes;
            
            // IP间延迟更长一些
            if (ip_index < required_ips_ - 1) {
                usleep(200000); // 200ms
            }
        }
        
        std::cout << "\n=== Cluster Startup Complete ===" << std::endl;
        std::cout << "Total processes created: " << g_all_child_pids.size() << std::endl;
        
        return true;
    }
    
    // 监控集群状态
    void MonitorCluster() {
        std::cout << "\n=== Monitoring Cluster ===" << std::endl;
        std::cout << "Press Ctrl+C to shutdown cluster gracefully" << std::endl;
        
        while (!g_shutdown_requested && !g_all_child_pids.empty()) {
            int status;
            pid_t finished_pid = waitpid(-1, &status, WNOHANG); // 非阻塞等待
            
            if (finished_pid > 0) {
                // 有进程结束了
                HandleProcessExit(finished_pid, status);
            }
            else if (finished_pid == 0) {
                // 没有进程结束，继续监控
                sleep(1);
            }
            else {
                // waitpid出错
                if (errno != ECHILD) {
                    perror("waitpid error");
                }
                break;
            }
        }
        
        std::cout << "All DataNode processes have finished." << std::endl;
    }
    
    // 获取集群状态统计
    void PrintClusterStatus() {
        std::cout << "\n=== Cluster Status ===" << std::endl;
        std::cout << "Active processes: " << g_all_child_pids.size() << std::endl;
        
        for (const auto& pair : g_ip_process_map) {
            int ip_index = pair.first;
            const std::vector<pid_t>& processes = pair.second;
            int active_count = 0;
            
            for (pid_t pid : processes) {
                if (kill(pid, 0) == 0) {
                    active_count++;
                }
            }
            
            std::cout << "IP " << ip_index << " (" << datanode_ips_[ip_index] 
                      << "): " << active_count << "/" << processes.size() 
                      << " processes active" << std::endl;
        }
    }
    
private:
    // 启动单个DataNode进程
    void StartSingleDataNodeProcess(int global_node_id, int ip_index, int local_port_index,
                                  int k, int r, int p, size_t block_size) {
        try {
            // 设置子进程的信号处理
            signal(SIGTERM, DataNode::graceful_shutdown_handler);
            signal(SIGINT, DataNode::graceful_shutdown_handler);
            
            auto datanode = std::make_unique<DataNode>();
            
            // 计算实际的端口号
            int actual_port = datanode_ports_[local_port_index];
            int total_nodes = k + r + p;
            
            datanode->block_size = block_size;
            datanode->SetNodeList(total_nodes);
            
            std::cout << "DataNode " << global_node_id 
                      << " starting on " << datanode_ips_[ip_index] 
                      << ":" << actual_port << std::endl;
            
            datanode->Start(actual_port,datanode_ips_[ip_index]);
        }
        catch (const std::exception& e) {
            std::cerr << "DataNode " << global_node_id << " failed: " << e.what() << std::endl;
            exit(1);
        }
    }
    
    // 处理进程退出
    void HandleProcessExit(pid_t pid, int status) {
        // 从全局列表中移除
        auto it = std::find(g_all_child_pids.begin(), g_all_child_pids.end(), pid);
        if (it != g_all_child_pids.end()) {
            g_all_child_pids.erase(it);
        }
        
        // 从IP映射中移除
        for (auto& pair : g_ip_process_map) {
            std::vector<pid_t>& processes = pair.second;
            auto ip_it = std::find(processes.begin(), processes.end(), pid);
            if (ip_it != processes.end()) {
                processes.erase(ip_it);
                break;
            }
        }
        
        // 打印退出信息
        if (WIFEXITED(status)) {
            std::cout << "DataNode process " << pid 
                      << " exited with status " << WEXITSTATUS(status) << std::endl;
        } else if (WIFSIGNALED(status)) {
            std::cout << "DataNode process " << pid 
                      << " was killed by signal " << WTERMSIG(status) << std::endl;
        }
    }
    
    // 清理所有进程
    void CleanupProcesses() {
        std::cout << "Cleaning up all processes..." << std::endl;
        for (pid_t child_pid : g_all_child_pids) {
            kill(child_pid, SIGTERM);
        }
        
        // 等待进程退出
        sleep(1);
        
        // 强制终止
        for (pid_t child_pid : g_all_child_pids) {
            kill(child_pid, SIGKILL);
        }
    }
};

// 主启动函数
int main() {
    std::cout << "=== Distributed DataNode Cluster Launcher ===" << std::endl;
    std::cout << "Please input the number of data nodes (k), global parity (r), local parity (p), and block size:" << std::endl;
    
    int k, r, p;
    size_t block_size;
    
    std::cin >> k >> r >> p >> block_size;
    
    if (k <= 0 || r < 0 || p < 0) {
        std::cerr << "Invalid parameters: k must be > 0, r and p must be >= 0" << std::endl;
        return 1;
    }

    g_config = std::make_unique<ConfigReader>();
    
    // 加载配置文件
    if (!g_config->loadConfig("../include/config.xml")) {
        std::cerr << "Failed to load configuration file!" << std::endl;
        return -1;
    }
    
    // 打印所有配置（调试用）
    g_config->printAllConfigs();
    
    std::cout << "\n=== 使用示例 ===" << std::endl;
    
    // 使用原有的宏定义方式
    std::cout << "CLIENT_PORT: " << CLIENT_PORT << std::endl;
    std::cout << "COORDINATOR_PORT: " << COORDINATOR_PORT << std::endl;
    std::cout << "CLIENT_IP: " << CLIENT_IP << std::endl;
    std::cout << "COORDINATOR_IP: " << COORDINATOR_IP << std::endl;
    std::cout << "PROXY_IP: " << PROXY_IP << std::endl;
    
    // 使用宏定义访问
    std::cout << "\n=== 使用宏定义访问 ===" << std::endl;
    std::cout << "DATANODE_IP_0: " << DATANODE_IP_0 << std::endl;
    std::cout << "DATANODE_PORT_0: " << DATANODE_PORT_0 << std::endl;
    std::cout << "DATANODE_IP_1: " << DATANODE_IP_1 << std::endl;
    std::cout << "DATANODE_PORT_1: " << DATANODE_PORT_1 << std::endl;
    
    
    
    int total_nodes = k + r + p;
    if (total_nodes > getMaxDatanodeCount()) {
        std::cerr << "Error: Requested " << total_nodes 
                  << " nodes exceeds maximum capacity of " << getMaxDatanodeCount() << std::endl;
        return 1;
    }
    
    try {
        // 设置信号处理
        signal(SIGINT, cluster_signal_handler);
        signal(SIGTERM, cluster_signal_handler);
        
        // 创建集群管理器
        DataNodeClusterManager cluster_manager(k, r, p);
        
        // 启动集群
        if (!cluster_manager.StartCluster(k, r, p, block_size)) {
            std::cerr << "Failed to start cluster" << std::endl;
            return 1;
        }
        
        // 监控集群
        cluster_manager.MonitorCluster();
        
    } catch (const std::exception& e) {
        std::cerr << "Cluster startup failed: " << e.what() << std::endl;
        
        // 清理可能已创建的进程
        for (pid_t child_pid : g_all_child_pids) {
            kill(child_pid, SIGTERM);
        }
        
        return 1;
    }
    
    return 0;
}