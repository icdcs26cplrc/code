#include "../include/datanode.hh"
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include <vector>
#include <map>
#include <iostream>
#include <algorithm>
#include <thread>
#include <chrono>
#include <fstream>
#include <sstream>

using namespace ClientServer;

// 全局变量用于管理所有子进程
static std::map<std::string, std::vector<pid_t>> g_ip_process_map; // IP -> 该IP下的进程PID列表
static std::vector<pid_t> g_all_local_pids;                        // 本机所有子进程PID
static bool g_shutdown_requested = false;

// 信号处理函数
void cluster_signal_handler(int sig)
{
    std::cout << "\nReceived signal " << sig << ", shutting down all DataNode processes..." << std::endl;
    g_shutdown_requested = true;

    for (pid_t child_pid : g_all_local_pids)
    {
        if (kill(child_pid, SIGTERM) == 0)
        {
            std::cout << "Sent SIGTERM to local process " << child_pid << std::endl;
        }
    }

    // 等待一段时间让进程优雅退出
    sleep(2);

    // 强制终止仍未退出的进程
    for (pid_t child_pid : g_all_local_pids)
    {
        if (kill(child_pid, 0) == 0)
        {
            std::cout << "Force killing local process " << child_pid << std::endl;
            kill(child_pid, SIGKILL);
        }
    }

    exit(0);
}

// 分布式集群管理器类
class DistributedDataNodeManager
{
private:
    int total_datanodes_;
    int required_ips_;
    std::vector<std::string> datanode_ips_;
    std::vector<int> datanode_ports_;
    std::string current_machine_ip_;
    std::string ssh_user_;
    std::string binary_path_;
    std::map<std::string, bool> ip_reachability_;

public:
    DistributedDataNodeManager(int k, int r, int p)
    {
        total_datanodes_ = k + r + p;
        required_ips_ = (total_datanodes_ + DATANODES_PER_IP - 1) / DATANODES_PER_IP;

        datanode_ips_ = getDatanodeIPs();
        datanode_ports_ = getDatanodePorts();

        // 获取当前机器IP
        current_machine_ip_ = getCurrentMachineIP();

        // 从环境变量或配置获取SSH用户名和二进制路径
        ssh_user_ = getenv("SSH_USER") ? getenv("SSH_USER") : "sep-lrc";
        binary_path_ = getenv("DATANODE_BINARY_PATH") ? getenv("DATANODE_BINARY_PATH") : "/home/sep-lrc/client-server-test1/client-server/target_1/datanode_single";

        std::cout << "Distributed Cluster Configuration:" << std::endl;
        std::cout << "  Total DataNodes needed: " << total_datanodes_ << std::endl;
        std::cout << "  Required IPs: " << required_ips_ << std::endl;
        std::cout << "  Available IPs: " << datanode_ips_.size() << std::endl;
        std::cout << "  Current Machine IP: " << current_machine_ip_ << std::endl;
        std::cout << "  SSH User: " << ssh_user_ << std::endl;
        std::cout << "  Binary Path: " << binary_path_ << std::endl;

        if (required_ips_ > static_cast<int>(datanode_ips_.size()))
        {
            throw std::runtime_error("Not enough IP addresses configured for required DataNodes");
        }

        // 检查IP可达性
        CheckIPReachability();
    }

    // 检查IP可达性
    void CheckIPReachability()
    {
        std::cout << "\n=== Checking IP Reachability ===" << std::endl;

        for (const auto &ip : datanode_ips_)
        {
            if (ip == current_machine_ip_)
            {
                ip_reachability_[ip] = true;
                std::cout << "  " << ip << " (local): OK" << std::endl;
            }
            else
            {
                bool reachable = PingHost(ip);
                ip_reachability_[ip] = reachable;
                std::cout << "  " << ip << " (remote): " << (reachable ? "OK" : "UNREACHABLE") << std::endl;
            }
        }
    }

    // Ping检查主机是否可达
    bool PingHost(const std::string &ip)
    {
        std::string cmd = "ping -c 1 -W 2 " + ip + " > /dev/null 2>&1";
        int result = system(cmd.c_str());
        return result == 0;
    }

    // 获取当前机器IP
    std::string getCurrentMachineIP()
    {
        // 简单实现：通过连接到配置中的第一个IP来获取本机IP
        // 实际部署时应该通过更可靠的方式获取
        return datanode_ips_.empty() ? "127.0.0.1" : datanode_ips_[0];
    }

    // 启动分布式集群
    // 修复后的 StartDistributedCluster 方法
    bool StartDistributedCluster(int k, int r, int p, size_t block_size)
    {
        std::cout << "\n=== Starting Distributed DataNode Cluster ===" << std::endl;

        int global_node_id = 0;

        // 预先创建所有需要的脚本文件
        if (!CreateAllScripts(k, r, p, block_size))
        {
            std::cerr << "Failed to create startup scripts" << std::endl;
            return false;
        }

        // 遍历需要的IP地址
        for (int ip_index = 0; ip_index < required_ips_; ip_index++)
        {
            std::string target_ip = datanode_ips_[ip_index];

            if (!ip_reachability_[target_ip])
            {
                std::cerr << "Skipping unreachable IP: " << target_ip << std::endl;
                continue;
            }

            // 计算当前IP需要启动的DataNode数量
            int nodes_for_this_ip = std::min(DATANODES_PER_IP,
                                             total_datanodes_ - ip_index * DATANODES_PER_IP);

            std::cout << "\nStarting " << nodes_for_this_ip
                      << " DataNodes on IP " << target_ip
                      << " (IP index: " << ip_index << ")" << std::endl;

            if (target_ip == current_machine_ip_)
            {
                // 本机部署
                if (!StartLocalDataNodes(ip_index, nodes_for_this_ip, global_node_id, k, r, p, block_size))
                {
                    std::cerr << "Failed to start local DataNodes on " << target_ip << std::endl;
                    return false;
                }
            }
            else
            {
                // 远程部署
                if (!StartRemoteDataNodes(target_ip, ip_index, nodes_for_this_ip, global_node_id, k, r, p, block_size))
                {
                    std::cerr << "Failed to start remote DataNodes on " << target_ip << std::endl;
                    return false;
                }
            }

            global_node_id += nodes_for_this_ip;
        }

        std::cout << "\n=== Distributed Cluster Startup Complete ===" << std::endl;
        return true;
    }

    // 新增：创建所有脚本文件的方法
    bool CreateAllScripts(int k, int r, int p, size_t block_size)
    {
        std::cout << "Creating startup scripts..." << std::endl;

        // 创建单个DataNode启动脚本
        CreateSingleDataNodeScript(k, r, p, block_size);

        // 验证单个脚本是否创建成功
        if (access("start_single_datanode.sh", F_OK) != 0)
        {
            std::cerr << "Failed to create start_single_datanode.sh" << std::endl;
            return false;
        }

        // 为每个需要的远程IP创建启动脚本
        int global_node_id = 0;
        for (int ip_index = 0; ip_index < required_ips_; ip_index++)
        {
            std::string target_ip = datanode_ips_[ip_index];

            if (target_ip != current_machine_ip_)
            {
                int nodes_for_this_ip = std::min(DATANODES_PER_IP,
                                                 total_datanodes_ - ip_index * DATANODES_PER_IP);

                std::string script_name = CreateRemoteStartScript(target_ip, ip_index, nodes_for_this_ip, global_node_id, k, r, p, block_size);

                // 验证脚本是否创建成功
                if (access(script_name.c_str(), F_OK) != 0)
                {
                    std::cerr << "Failed to create " << script_name << std::endl;
                    return false;
                }

                std::cout << "Created script: " << script_name << std::endl;
            }

            global_node_id += std::min(DATANODES_PER_IP, total_datanodes_ - ip_index * DATANODES_PER_IP);
        }

        return true;
    }

    // 启动本地DataNodes
    bool StartLocalDataNodes(int ip_index, int node_count, int start_node_id,
                             int k, int r, int p, size_t block_size)
    {
        std::vector<pid_t> local_processes;

        for (int i = 0; i < node_count; i++)
        {
            int global_node_id = start_node_id + i;
            int local_port_index = i;

            pid_t pid = fork();

            if (pid < 0)
            {
                std::cerr << "Fork failed for local DataNode " << global_node_id << std::endl;
                return false;
            }
            else if (pid == 0)
            {
                // 子进程：启动单个DataNode
                StartSingleDataNodeProcess(global_node_id, ip_index, local_port_index, k, r, p, block_size);
                exit(0);
            }
            else
            {
                // 父进程：记录子进程信息
                local_processes.push_back(pid);
                g_all_local_pids.push_back(pid);

                std::cout << "  Created local DataNode " << global_node_id
                          << " (PID: " << pid
                          << ", IP: " << datanode_ips_[ip_index]
                          << ", Port: " << datanode_ports_[local_port_index] << ")" << std::endl;

                usleep(50000); // 50ms延迟
            }
        }

        g_ip_process_map[datanode_ips_[ip_index]] = local_processes;
        return true;
    }

    // 启动远程DataNodes
    // 修复后的 StartRemoteDataNodes 方法
    // 创建单个DataNode启动脚本
    // 修复后的 CreateSingleDataNodeScript 方法
    void CreateSingleDataNodeScript(int k, int r, int p, size_t block_size)
    {
        std::string script_path = "start_single_datanode.sh";
        std::ofstream script(script_path);

        if (!script.is_open())
        {
            std::cerr << "Failed to create " << script_path << std::endl;
            return;
        }

        script << "#!/bin/bash\n";
        script << "# Auto-generated DataNode startup script\n";
        script << "NODE_ID=$1\n";
        script << "IP_INDEX=$2\n";
        script << "PORT_INDEX=$3\n";
        script << "K=$4\n";
        script << "R=$5\n";
        script << "P=$6\n";
        script << "BLOCK_SIZE=$7\n";
        script << "\n";
        script << "# Set environment variables\n";
        script << "export NODE_ID\n";
        script << "export IP_INDEX\n";
        script << "export PORT_INDEX\n";
        script << "export K\n";
        script << "export R\n";
        script << "export P\n";
        script << "export BLOCK_SIZE\n";
        script << "\n";
        script << "# Start DataNode\n";
        script << binary_path_ << " $NODE_ID $IP_INDEX $PORT_INDEX $K $R $P $BLOCK_SIZE\n";
        script.close();

        // 设置可执行权限
        std::string chmod_cmd = "chmod +x " + script_path;
        int result = system(chmod_cmd.c_str());
        if (result != 0)
        {
            std::cerr << "Failed to set executable permission for " << script_path << std::endl;
        }

        std::cout << "Created and set permissions for " << script_path << std::endl;
    }

    // 创建远程启动脚本
    // 修复后的 CreateRemoteStartScript 方法
    // 修复后的 CreateRemoteStartScript 方法
    std::string CreateRemoteStartScript(const std::string &target_ip, int ip_index, int node_count,
                                        int start_node_id, int k, int r, int p, size_t block_size)
    {
        std::string script_name = "start_datanodes_" + target_ip + ".sh";
        std::ofstream script(script_name);

        if (!script.is_open())
        {
            std::cerr << "Failed to create " << script_name << std::endl;
            return "";
        }

        script << "#!/bin/bash\n";
        script << "# Auto-generated remote DataNode startup script for " << target_ip << "\n";
        script << "set -e  # Exit on any error\n";
        script << "\n";
        script << "echo \"Starting " << node_count << " DataNodes on " << target_ip << "\"\n";
        script << "echo \"Current user: $(whoami)\"\n";
        script << "echo \"Current directory: $(pwd)\"\n";
        script << "\n";
        script << "# Kill existing DataNode processes\n";
        script << "echo \"Cleaning up existing processes...\"\n";
        script << "pkill -f datanode_single 2>/dev/null || true\n";
        script << "sleep 3\n";
        script << "\n";
        script << "# Check if binary exists\n";
        script << "BINARY_PATH=\"" << binary_path_ << "\"\n";
        script << "if [ ! -f \"$BINARY_PATH\" ]; then\n";
        script << "    echo \"Error: Binary not found at $BINARY_PATH\"\n";
        script << "    exit 1\n";
        script << "fi\n";
        script << "echo \"Binary found at $BINARY_PATH\"\n";
        script << "\n";
        script << "# Create log directory\n";
        script << "mkdir -p /tmp/datanode_logs\n";
        script << "\n";
        script << "# Start DataNodes\n";
        script << "STARTED_COUNT=0\n";

        // 为每个DataNode创建启动命令
        for (int i = 0; i < node_count; i++)
        {
            int global_node_id = start_node_id + i;

            script << "\n# Start DataNode " << global_node_id << "\n";
            script << "echo \"Starting DataNode " << global_node_id << " on port " << datanode_ports_[i] << "\"\n";

            // 使用更可靠的启动方式
            script << "cd /home/sep-lrc/client-server-test1/client-server/target_1\n";
            script << "nohup $BINARY_PATH " << global_node_id << " " << ip_index << " " << i
                   << " " << k << " " << r << " " << p << " " << block_size
                   << " > /tmp/datanode_logs/datanode_" << global_node_id << ".log 2>&1 &\n";
            script << "DATANODE_PID=$!\n";
            script << "echo \"DataNode " << global_node_id << " started with PID: $DATANODE_PID\"\n";
            script << "echo $DATANODE_PID > /tmp/datanode_logs/datanode_" << global_node_id << ".pid\n";

            // 验证进程是否成功启动
            script << "sleep 1\n";
            script << "if kill -0 $DATANODE_PID 2>/dev/null; then\n";
            script << "    echo \"DataNode " << global_node_id << " is running (PID: $DATANODE_PID)\"\n";
            script << "    STARTED_COUNT=$((STARTED_COUNT + 1))\n";
            script << "else\n";
            script << "    echo \"Error: DataNode " << global_node_id << " failed to start\"\n";
            script << "    cat /tmp/datanode_logs/datanode_" << global_node_id << ".log\n";
            script << "fi\n";

            script << "sleep 0.5\n"; // 增加延迟避免端口冲突
        }

        script << "\n";
        script << "# Wait for all processes to stabilize\n";
        script << "echo \"Waiting for all DataNodes to stabilize...\"\n";
        script << "sleep 5\n";
        script << "\n";
        script << "# Final status check\n";
        script << "echo \"Final status check:\"\n";
        script << "RUNNING_COUNT=$(pgrep -f datanode_single | wc -l)\n";
        script << "echo \"DataNodes started: $STARTED_COUNT/" << node_count << "\"\n";
        script << "echo \"DataNodes running: $RUNNING_COUNT/" << node_count << "\"\n";
        script << "\n";
        script << "# Show process details\n";
        script << "echo \"Process details:\"\n";
        script << "ps aux | grep datanode_single | grep -v grep || echo \"No processes found\"\n";
        script << "\n";
        script << "# Show port status\n";
        script << "echo \"Port status:\"\n";
        script << "ss -tuln | grep \"345[6-7][0-9]\" || echo \"No DataNode ports listening\"\n";
        script << "\n";
        script << "# Log file information\n";
        script << "echo \"Log files:\"\n";
        script << "ls -la /tmp/datanode_logs/ 2>/dev/null || echo \"No log directory\"\n";
        script << "\n";
        script << "if [ $RUNNING_COUNT -eq " << node_count << " ]; then\n";
        script << "    echo \"SUCCESS: All " << node_count << " DataNodes started successfully on " << target_ip << "\"\n";
        script << "    exit 0\n";
        script << "else\n";
        script << "    echo \"ERROR: Only $RUNNING_COUNT out of " << node_count << " DataNodes started on " << target_ip << "\"\n";
        script << "    exit 1\n";
        script << "fi\n";

        script.close();

        // 设置可执行权限
        std::string chmod_cmd = "chmod +x " + script_name;
        int result = system(chmod_cmd.c_str());
        if (result != 0)
        {
            std::cerr << "Failed to set executable permission for " << script_name << std::endl;
        }

        return script_name;
    }

    // 修复后的 StartRemoteDataNodes 方法
    bool StartRemoteDataNodes(const std::string &target_ip, int ip_index, int node_count,
                              int start_node_id, int k, int r, int p, size_t block_size)
    {

        std::string script_name = "start_datanodes_" + target_ip + ".sh";

        // 检查脚本是否存在
        if (access(script_name.c_str(), F_OK) != 0)
        {
            std::cerr << "Script " << script_name << " does not exist" << std::endl;
            return false;
        }

        std::cout << "Starting " << node_count << " DataNodes on " << target_ip << std::endl;

        // 1. 先清理远程机器上的旧进程
        std::string cleanup_cmd = "ssh " + ssh_user_ + "@" + target_ip + " 'pkill -f datanode_single 2>/dev/null || true; sleep 2'";
        system(cleanup_cmd.c_str());
        std::cout << "Cleaned up old processes on " << target_ip << std::endl;

        // 2. 复制脚本到远程机器
        std::string scp_cmd = "scp " + script_name + " " + ssh_user_ + "@" + target_ip + ":/tmp/";
        std::cout << "Copying script to " << target_ip << "..." << std::endl;

        if (system(scp_cmd.c_str()) != 0)
        {
            std::cerr << "Failed to copy script to " << target_ip << std::endl;
            return false;
        }

        // 3. 在远程机器上执行脚本 - 修改执行方式，确保脚本有执行权限
        std::string ssh_cmd = "ssh " + ssh_user_ + "@" + target_ip + " 'chmod +x /tmp/" + script_name + "; /tmp/" + script_name + " > /tmp/startup_" + target_ip + ".log 2>&1'";
        std::cout << "Executing remote script on " << target_ip << "..." << std::endl;

        // 使用同步方式执行，确保脚本完全执行完毕
        int ssh_result = system(ssh_cmd.c_str());
        if (ssh_result != 0)
        {
            std::cerr << "SSH command failed on " << target_ip << " with exit code: " << ssh_result << std::endl;

            // 显示远程执行日志
            std::string log_cmd = "ssh " + ssh_user_ + "@" + target_ip + " 'cat /tmp/startup_" + target_ip + ".log 2>/dev/null || echo \"No log file found\"'";
            std::cout << "Remote execution log:" << std::endl;
            system(log_cmd.c_str());

            return false;
        }

        // 4. 等待进程启动 - 增加等待时间
        std::cout << "Waiting for remote DataNodes to start..." << std::endl;
        sleep(8); // 增加等待时间到8秒

        // 5. 验证远程DataNode是否成功启动 - 多次尝试验证
        int max_attempts = 3;
        int running_count = 0;

        for (int attempt = 1; attempt <= max_attempts; attempt++)
        {
            std::string verify_cmd = "ssh " + ssh_user_ + "@" + target_ip + " 'pgrep -f datanode_single | wc -l'";
            std::string result = exec_command(verify_cmd);

            try
            {
                running_count = std::stoi(result);
                std::cout << "  Attempt " << attempt << ": " << running_count << "/" << node_count << " DataNodes running on " << target_ip << std::endl;

                if (running_count >= node_count)
                {
                    break;
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << "  Failed to parse process count: " << result << std::endl;
                running_count = 0;
            }

            if (attempt < max_attempts)
            {
                std::cout << "  Waiting 3 more seconds before next check..." << std::endl;
                sleep(3);
            }
        }

        // 6. 检查结果和显示详细信息
        if (running_count < node_count)
        {
            std::cerr << "Warning: Only " << running_count << " out of " << node_count << " DataNodes started on " << target_ip << std::endl;

            // 显示详细的进程信息
            std::string ps_cmd = "ssh " + ssh_user_ + "@" + target_ip + " 'ps aux | grep datanode_single | grep -v grep || echo \"No datanode processes found\"'";
            std::cout << "Remote process details:" << std::endl;
            system(ps_cmd.c_str());

            // 显示启动日志
            std::string log_cmd = "ssh " + ssh_user_ + "@" + target_ip + " 'cat /tmp/startup_" + target_ip + ".log 2>/dev/null || echo \"No startup log found\"'";
            std::cout << "Remote startup log:" << std::endl;
            system(log_cmd.c_str());

            // 检查端口监听状态
            std::string port_cmd = "ssh " + ssh_user_ + "@" + target_ip + " 'ss -tuln | grep \"345[6-7][0-9]\" || echo \"No DataNode ports listening\"'";
            std::cout << "Remote port status:" << std::endl;
            system(port_cmd.c_str());

            return false;
        }

        std::cout << "  Successfully started " << node_count << " remote DataNodes on " << target_ip << std::endl;

        // 7. 显示端口监听状态确认
        std::string port_confirm_cmd = "ssh " + ssh_user_ + "@" + target_ip + " 'ss -tuln | grep \"345[6-7][0-9]\" | wc -l'";
        std::string port_count = exec_command(port_confirm_cmd);
        std::cout << "  Port verification: " << port_count << " ports listening on " << target_ip << std::endl;

        return true;
    }
    // 添加执行命令并获取输出的辅助函数
    std::string exec_command(const std::string &cmd)
    {
        std::array<char, 128> buffer;
        std::string result;
        std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);

        if (!pipe)
        {
            return "0";
        }

        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr)
        {
            result += buffer.data();
        }

        // 移除换行符
        if (!result.empty() && result.back() == '\n')
        {
            result.pop_back();
        }

        return result.empty() ? "0" : result;
    }

    // 改进的远程进程检查方法
    void CheckRemoteProcesses()
    {
        std::cout << "\n=== Remote DataNode Status Check ===" << std::endl;

        for (const auto &ip : datanode_ips_)
        {
            if (ip != current_machine_ip_ && ip_reachability_[ip])
            {
                std::cout << "\nChecking " << ip << ":" << std::endl;

                // 检查进程数量
                std::string count_cmd = "ssh " + ssh_user_ + "@" + ip + " 'pgrep -f datanode_single | wc -l'";
                std::string count_result = exec_command(count_cmd);

                // 检查端口监听
                std::string port_cmd = "ssh " + ssh_user_ + "@" + ip + " 'ss -tuln | grep \"345[6-7][0-9]\" | wc -l'";
                std::string port_result = exec_command(port_cmd);

                std::cout << "  Processes: " << count_result << " DataNode processes running" << std::endl;
                std::cout << "  Ports: " << port_result << " DataNode ports listening" << std::endl;

                // 如果进程数量异常，显示详细信息
                try
                {
                    int process_count = std::stoi(count_result);
                    if (process_count == 0)
                    {
                        std::cout << "  Warning: No DataNode processes found on " << ip << std::endl;

                        // 显示最新的启动日志
                        std::string log_cmd = "ssh " + ssh_user_ + "@" + ip + " 'tail -10 /tmp/startup_" + ip + ".log 2>/dev/null || echo \"No startup log found\"'";
                        std::cout << "  Recent startup log:" << std::endl;
                        system(log_cmd.c_str());
                    }
                    else
                    {
                        std::cout << "  Status: OK" << std::endl;
                    }
                }
                catch (const std::exception &e)
                {
                    std::cout << "  Status: Unable to parse process count" << std::endl;
                }
            }
        }
    }

    // 启动单个DataNode进程（用于本地进程）
    void StartSingleDataNodeProcess(int global_node_id, int ip_index, int local_port_index,
                                    int k, int r, int p, size_t block_size)
    {
        try
        {
            signal(SIGTERM, DataNode::graceful_shutdown_handler);
            signal(SIGINT, DataNode::graceful_shutdown_handler);

            auto datanode = std::make_unique<DataNode>();

            int actual_port = datanode_ports_[local_port_index];
            int total_nodes = k + r + p;

            datanode->block_size = block_size;
            datanode->SetNodeList(global_node_id);

            std::cout << "DataNode " << global_node_id
                      << " starting on " << datanode_ips_[ip_index]
                      << ":" << actual_port << std::endl;

            // 本地DataNode绑定到本机IP
            datanode->Start(actual_port, "0.0.0.0"); // 绑定到所有接口
        }
        catch (const std::exception &e)
        {
            std::cerr << "DataNode " << global_node_id << " failed: " << e.what() << std::endl;
            exit(1);
        }
    }

    // 监控集群状态
    void MonitorCluster()
    {
        std::cout << "\n=== Monitoring Distributed Cluster ===" << std::endl;
        std::cout << "Press Ctrl+C to shutdown cluster gracefully" << std::endl;

        while (!g_shutdown_requested)
        {
            // 监控本地进程
            MonitorLocalProcesses();

            // 定期检查远程进程状态
            CheckRemoteProcesses();

            sleep(5); // 每5秒检查一次
        }
    }

    // 监控本地进程
    void MonitorLocalProcesses()
    {
        int status;
        pid_t finished_pid = waitpid(-1, &status, WNOHANG);

        if (finished_pid > 0)
        {
            HandleLocalProcessExit(finished_pid, status);
        }
    }

    // 处理本地进程退出
    void HandleLocalProcessExit(pid_t pid, int status)
    {
        auto it = std::find(g_all_local_pids.begin(), g_all_local_pids.end(), pid);
        if (it != g_all_local_pids.end())
        {
            g_all_local_pids.erase(it);
        }

        if (WIFEXITED(status))
        {
            std::cout << "Local DataNode process " << pid
                      << " exited with status " << WEXITSTATUS(status) << std::endl;
        }
        else if (WIFSIGNALED(status))
        {
            std::cout << "Local DataNode process " << pid
                      << " was killed by signal " << WTERMSIG(status) << std::endl;
        }
    }

    // 打印集群状态
    void PrintClusterStatus()
    {
        std::cout << "\n=== Distributed Cluster Status ===" << std::endl;
        std::cout << "Local active processes: " << g_all_local_pids.size() << std::endl;

        for (const auto &ip : datanode_ips_)
        {
            if (ip == current_machine_ip_)
            {
                std::cout << "IP " << ip << " (local): " << g_all_local_pids.size() << " processes" << std::endl;
            }
            else
            {
                std::cout << "IP " << ip << " (remote): status check needed" << std::endl;
            }
        }
    }

    // 清理资源
    void CleanupDistributedCluster()
    {
        std::cout << "\n=== Cleaning up Distributed Cluster ===" << std::endl;

        // 清理本地进程
        for (pid_t child_pid : g_all_local_pids)
        {
            kill(child_pid, SIGTERM);
        }

        // 清理远程进程
        for (const auto &ip : datanode_ips_)
        {
            if (ip != current_machine_ip_ && ip_reachability_[ip])
            {
                std::string cmd = "ssh " + ssh_user_ + "@" + ip + " 'pkill -f datanode_single'";
                system(cmd.c_str());
                std::cout << "Sent cleanup command to " << ip << std::endl;
            }
        }

        // 清理临时文件
        system("rm -f start_datanodes_*.sh start_single_datanode.sh");
    }
};

// 在类中添加调试方法
void DebugPrintWorkingDirectory()
{
    char *cwd = getcwd(NULL, 0);
    if (cwd)
    {
        std::cout << "Current working directory: " << cwd << std::endl;
        free(cwd);
    }
}

// 主启动函数
int main()
{
    std::cout << "=== Distributed DataNode Cluster Launcher ===" << std::endl;
    std::cout << "Please input the number of data nodes (k), global parity (r), local parity (p), and block size:" << std::endl;

    int k, r, p;
    size_t block_size;

    std::cin >> k >> r >> p >> block_size;

    if (k <= 0 || r < 0 || p < 0)
    {
        std::cerr << "Invalid parameters: k must be > 0, r and p must be >= 0" << std::endl;
        return 1;
    }

    g_config = std::make_unique<ConfigReader>();

    if (!g_config->loadConfig("../include/config.xml"))
    {
        std::cerr << "Failed to load configuration file!" << std::endl;
        return -1;
    }

    int total_nodes = k + r + p;
    if (total_nodes > getMaxDatanodeCount())
    {
        std::cerr << "Error: Requested " << total_nodes
                  << " nodes exceeds maximum capacity of " << getMaxDatanodeCount() << std::endl;
        return 1;
    }

    try
    {

        // 设置信号处理
        signal(SIGINT, cluster_signal_handler);
        signal(SIGTERM, cluster_signal_handler);

        // 创建分布式集群管理器
        DistributedDataNodeManager cluster_manager(k, r, p);

        // 启动分布式集群
        if (!cluster_manager.StartDistributedCluster(k, r, p, block_size))
        {
            std::cerr << "Failed to start distributed cluster" << std::endl;
            return 1;
        }

        // 监控集群
        cluster_manager.MonitorCluster();

        // 清理资源
        cluster_manager.CleanupDistributedCluster();
    }
    catch (const std::exception &e)
    {
        std::cerr << "Distributed cluster startup failed: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}