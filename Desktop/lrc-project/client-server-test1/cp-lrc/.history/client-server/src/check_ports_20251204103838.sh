#!/bin/bash

# define the ports to check
PORTS=(
  12345    # CLIENT_PORT（原 123345，已修正）
  12378    # COORDINATOR_PORT（原 123378，已修正）
  23456    # PROXY_PORT_BASE
  34567    # DATANODE_PORT_BASE
  34589
)

for port in "${PORTS[@]}"; do
  echo "检查端口: $port"
  # detect the process using the port
  pid=$(lsof -ti :$port)
  if [ -n "$pid" ]; then
    echo "端口 $port 被进程占用 (PID: $pid)，正在终止..."
    kill -9 $pid
    echo "已终止进程 $pid"
  else
    echo "端口 $port 未被占用"
  fi
done