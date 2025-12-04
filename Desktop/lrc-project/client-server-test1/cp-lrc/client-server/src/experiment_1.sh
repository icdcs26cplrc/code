#!/bin/bash
# experiment1_remote_fixed.sh - 修复后的远程执行实验一

# 实验配置
BLOCKSIZE="1MB"
STRIPE_NUMBER=10
COORDINATOR_IP="192.168.1.244"
COORDINATOR_PORT="12378"
PROXY_IP="192.168.0.45"
PROXY_PORT="34589"
CLIENT_IP="192.168.1.243"
USERNAME="sep-lrc"

# 编码方案列表
ENCODING_SCHEMES=("azure" "azure_1" "optimal" "uniform" "sep-azure" "sep-uni")

# 参数组合 (n,k,r,p)
PARAMS=(
    "10 6 2 2"
    "16 12 2 2"
    "21 16 3 2"
    "28 20 3 5"
    "28 24 2 2"
    "55 48 4 3"
    "105 96 5 4"
)

# 检查并创建结果目录
create_directories() {
    echo "创建必要的目录..."
    mkdir -p experiment1_results
    mkdir -p testfiles
    
    # 检查目录创建是否成功
    if [[ ! -d "experiment1_results" ]]; then
        echo "错误: 无法创建 experiment1_results 目录"
        exit 1
    fi
    
    if [[ ! -d "testfiles" ]]; then
        echo "错误: 无法创建 testfiles 目录"
        exit 1
    fi
    
    echo "目录创建成功"
}

# 检查远程服务器依赖
check_remote_dependencies() {
    echo "检查远程服务器依赖..."
    
    # 检查coordinator服务器
    ssh "$USERNAME@$COORDINATOR_IP" "which expect" > /dev/null 2>&1
    if [[ $? -ne 0 ]]; then
        echo "在coordinator服务器上安装expect..."
        ssh "$USERNAME@$COORDINATOR_IP" "sudo apt-get update && sudo apt-get install -y expect"
    fi
    
    # 检查datanode服务器
    ssh "$USERNAME@192.168.1.245" "which expect" > /dev/null 2>&1
    if [[ $? -ne 0 ]]; then
        echo "在datanode服务器上安装expect..."
        ssh "$USERNAME@192.168.1.245" "sudo apt-get update && sudo apt-get install -y expect"
    fi
    
    echo "依赖检查完成"
}

# 远程启动coordinator函数
start_coordinator_remote() {
    local encoding_scheme=$1
    local k=$2
    local r=$3
    local p=$4
    local blocksize_bytes=$5
    
    echo "远程启动coordinator: encoding=$encoding_scheme, k=$k, r=$r, p=$p, blocksize=${blocksize_bytes}bytes"
    
    # 在coordinator服务器上启动coordinator
    ssh "$USERNAME@$COORDINATOR_IP" << EOF &
cd ~/client-server-test1/client-server/target
if [[ ! -f "./coordinator" ]]; then
    echo "错误: coordinator 可执行文件不存在"
    exit 1
fi

expect << 'EXPECT_EOF'
set timeout 30
spawn ./coordinator
expect {
    "参数设置" {
        send "$encoding_scheme $k $r $p $blocksize_bytes\r"
        expect "Successfully sent all"
        interact
    }
    timeout {
        puts "启动coordinator超时"
        exit 1
    }
}
EXPECT_EOF
EOF
    
    COORDINATOR_SSH_PID=$!
    sleep 5
    
    echo "Coordinator启动完成"
}

# 远程启动proxy函数
start_proxy_remote() {
    echo "远程启动proxy"
    
    # 在proxy服务器上启动proxy
    ssh "$USERNAME@$PROXY_IP" << EOF &
cd ~/client-server-test1/client-server/target
if [[ ! -f "./proxy" ]]; then
    echo "错误: proxy 可执行文件不存在"
    exit 1
fi
./proxy
EOF
    
    PROXY_SSH_PID=$!
    sleep 3
    
    echo "Proxy启动完成"
}

# 远程启动datanode集群函数
start_datanode_cluster_remote() {
    local k=$1
    local r=$2
    local p=$3
    local blocksize_bytes=$4
    
    echo "远程启动datanode集群: k=$k, r=$r, p=$p, blocksize=${blocksize_bytes}bytes"
    
    # 在主datanode服务器上启动集群
    ssh "$USERNAME@192.168.1.245" << EOF &
cd ~/client-server-test1/client-server/target
if [[ ! -f "./datanode" ]]; then
    echo "错误: datanode 可执行文件不存在"
    exit 1
fi

expect << 'EXPECT_EOF'
set timeout 30
spawn ./datanode
expect {
    "Please input the number of data nodes" {
        send "$k $r $p $blocksize_bytes\r"
        expect "Press Ctrl+C to shutdown cluster gracefully"
        interact
    }
    timeout {
        puts "启动datanode集群超时"
        exit 1
    }
}
EXPECT_EOF
EOF
    
    DATANODE_SSH_PID=$!
    sleep 10
    
    echo "DataNode集群启动完成"
}

# 远程启动完整集群函数
start_cluster_remote() {
    local encoding_scheme=$1
    local n=$2
    local k=$3
    local r=$4
    local p=$5
    local blocksize_bytes=$6
    
    echo "远程启动完整集群: encoding=$encoding_scheme, n=$n, k=$k, r=$r, p=$p, blocksize=${blocksize_bytes}bytes"
    
    # 启动proxy
    start_proxy_remote
    
    # 启动coordinator
    start_coordinator_remote "$encoding_scheme" $k $r $p $blocksize_bytes
    
    # 启动datanode集群
    start_datanode_cluster_remote $k $r $p $blocksize_bytes
    
    echo "集群启动完成"
}

# 本地执行SET操作函数
execute_set_operations_local() {
    local file_count=$1
    local result_file=$2
    
    echo "执行SET操作..." | tee -a "$result_file"
    
    # 检查结果文件是否可写
    if [[ ! -f "$result_file" ]]; then
        echo "错误: 无法创建结果文件 $result_file"
        return 1
    fi
    
    local set_count=0
    local max_sets=10
    
    for i in $(seq 1 $file_count); do
        local file_path="$(pwd)/testfiles/random_file_$(printf "%04d" $i).txt"
        
        if [[ -f "$file_path" ]] && [[ $set_count -lt $max_sets ]]; then
            echo "SET操作 $((set_count + 1)): $file_path" | tee -a "$result_file"
            
            # 检查client可执行文件
            if [[ ! -f "~/client-server-test1/client-server/target/client" ]]; then
                echo "错误: client 可执行文件不存在" | tee -a "$result_file"
                return 1
            fi
            
            # 本地执行set操作
            cd ~/client-server-test1/client-server/target
            
            timeout 30 expect << EOF >> "$result_file" 2>&1
spawn ./client
expect {
    "Please input the command" {
        send "set $file_path\r"
        expect "Data sent successfully to proxy"
        send "exit\r"
        expect eof
    }
    timeout {
        puts "SET操作超时"
        exit 1
    }
}
EOF
            
            if tail -n 10 "$result_file" | grep -q "Data sent successfully to proxy"; then
                echo "SET操作 $((set_count + 1)) 成功" | tee -a "$result_file"
                set_count=$((set_count + 1))
                
                if [[ $((set_count % 10)) -eq 0 ]]; then
                    echo "等待数据发送到datanode..." | tee -a "$result_file"
                    sleep 3
                fi
            else
                echo "SET操作 $((set_count + 1)) 失败" | tee -a "$result_file"
            fi
            
            sleep 1
        fi
        
        if [[ $set_count -ge $max_sets ]]; then
            break
        fi
    done
    
    echo "完成 $set_count 个SET操作" | tee -a "$result_file"
}

# 本地执行REPAIR操作函数
execute_repair_operations_local() {
    local n=$1
    local k=$2
    local result_file=$3
    
    echo "执行REPAIR操作..." | tee -a "$result_file"
    echo "修复时间记录:" | tee -a "$result_file"
    
    local total_repair_time=0
    local data_block_repair_time=0
    local repair_count=0
    local data_repair_count=0
    
    for stripe in $(seq 0 $((STRIPE_NUMBER - 1))); do
        for block in $(seq 0 $((n - 1))); do
            local block_name="stripe_${stripe}_block_${block}"
            
            echo "修复块: $block_name" | tee -a "$result_file"
            
            local start_time=$(date +%s.%N)
            
            cd ~/client-server-test1/client-server/target
            
            timeout 30 expect << EOF >> "$result_file" 2>&1
spawn ./client
expect {
    "Please input the command" {
        send "repair $block_name\r"
        expect "success repair"
        send "exit\r"
        expect eof
    }
    timeout {
        puts "REPAIR操作超时"
        exit 1
    }
}
EOF
            
            local end_time=$(date +%s.%N)
            local repair_time=$(echo "$end_time - $start_time" | bc)
            
            echo "修复时间: ${repair_time}s" | tee -a "$result_file"
            
            total_repair_time=$(echo "$total_repair_time + $repair_time" | bc)
            repair_count=$((repair_count + 1))
            
            if [[ $block -lt $k ]]; then
                data_block_repair_time=$(echo "$data_block_repair_time + $repair_time" | bc)
                data_repair_count=$((data_repair_count + 1))
            fi
            
            sleep 0.5
        done
    done
    
    local avg_repair_time=$(echo "scale=6; $total_repair_time / $repair_count" | bc)
    local avg_data_repair_time=$(echo "scale=6; $data_block_repair_time / $data_repair_count" | bc)
    
    echo "=== 修复时间统计 ===" | tee -a "$result_file"
    echo "总修复次数: $repair_count" | tee -a "$result_file"
    echo "总修复时间: ${total_repair_time}s" | tee -a "$result_file"
    echo "平均修复时间: ${avg_repair_time}s" | tee -a "$result_file"
    echo "数据块修复次数: $data_repair_count" | tee -a "$result_file"
    echo "数据块总修复时间: ${data_block_repair_time}s" | tee -a "$result_file"
    echo "数据块平均修复时间: ${avg_data_repair_time}s" | tee -a "$result_file"
}

# 远程停止集群函数
stop_cluster_remote() {
    echo "远程停止集群..."
    
    # 停止coordinator
    ssh "$USERNAME@$COORDINATOR_IP" "pkill -f coordinator" 2>/dev/null &
    
    # 停止proxy
    ssh "$USERNAME@$PROXY_IP" "pkill -f proxy" 2>/dev/null &
    
    # 停止datanode集群
    ssh "$USERNAME@192.168.1.245" "pkill -f datanode" 2>/dev/null &
    
    # 停止远程datanode进程
    for ip in 192.168.1.246 192.168.1.247 192.168.1.248 192.168.2.138 192.168.2.139 192.168.2.140; do
        ssh "$USERNAME@$ip" "pkill -f datanode_single" 2>/dev/null &
    done
    
    # 停止本地SSH进程
    if [[ -n "$COORDINATOR_SSH_PID" ]]; then
        kill $COORDINATOR_SSH_PID 2>/dev/null
    fi
    
    if [[ -n "$PROXY_SSH_PID" ]]; then
        kill $PROXY_SSH_PID 2>/dev/null
    fi
    
    if [[ -n "$DATANODE_SSH_PID" ]]; then
        kill $DATANODE_SSH_PID 2>/dev/null
    fi
    
    wait
    sleep 5
    echo "集群停止完成"
}

# 生成随机文件函数
generate_random_files() {
    local n=$1
    local blocksize_bytes=$2
    local file_count=$((2 * n * 10))
    
    echo "生成 $file_count 个随机文件，大小范围：$((blocksize_bytes / 5)) - $((blocksize_bytes * 2)) 字节"
    
    for i in $(seq 1 $file_count); do
        local min_size=$((blocksize_bytes / 5))
        local max_size=$((blocksize_bytes * 2))
        local file_size=$((min_size + RANDOM % (max_size - min_size)))
        
        dd if=/dev/urandom of="testfiles/random_file_$(printf "%04d" $i).txt" bs=1 count=$file_size 2>/dev/null
    done
    
    echo "随机文件生成完成"
}

# 主实验循环
main() {
    echo "开始远程实验一：固定blocksize(1MB)，测试不同参数和编码方案"
    echo "实验开始时间: $(date)"
    
    # 初始化检查
    create_directories
    check_remote_dependencies
    
    for encoding_scheme in "${ENCODING_SCHEMES[@]}"; do
        echo "=== 测试编码方案: $encoding_scheme ==="
        
        for param in "${PARAMS[@]}"; do
            read -r n k r p <<< "$param"
            
            echo "--- 测试参数: encoding=$encoding_scheme, n=$n, k=$k, r=$r, p=$p ---"
            
            result_file="experiment1_results/result_${encoding_scheme}_${n}_${k}_${r}_${p}.txt"
            
            # 创建结果文件
            cat > "$result_file" << EOF
实验参数: encoding=$encoding_scheme, n=$n, k=$k, r=$r, p=$p
Blocksize: $BLOCKSIZE
开始时间: $(date)
EOF
            
            generate_random_files $n 1048576
            
            start_cluster_remote "$encoding_scheme" $n $k $r $p 1048576
            
            sleep 15
            
            file_count=$((2 * n * 10))
            execute_set_operations_local $file_count "$result_file"
            
            sleep 5
            
            execute_repair_operations_local $n $k "$result_file"
            
            stop_cluster_remote
            
            echo "结束时间: $(date)" >> "$result_file"
            echo "--- 参数 ($encoding_scheme,$n,$k,$r,$p) 测试完成 ---"
            
            rm -rf testfiles/*
            
            sleep 20
        done
        
        echo "=== 编码方案 $encoding_scheme 测试完成 ==="
    done
    
    echo "实验一完成！结果保存在 experiment1_results/ 目录中"
}

# 脚本入口
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi