#!/bin/bash

# 杀掉现有进程
echo "Killing existing processes..."
lsof -i :50051 -i :50052 -i :50053 -i :60051 -i :60052 -i :60053 | grep LISTEN | awk '{print $2}' | xargs -r kill 2>/dev/null
sleep 1

cd /Users/milley/DevSpace/RustProject/claude-dev-training-space/04_MacroService/server-rust

# 清理旧数据（可选，注释掉以保留数据）
# rm -rf ./data

echo "Starting 3-node Raft cluster..."

# Node 1
cargo run -- --node-id 1 --client-port 50051 --raft-port 60051 \
    --peers "2@127.0.0.1:60052,3@127.0.0.1:60053" \
    --data-dir "./data" &

sleep 1

# Node 2
cargo run -- --node-id 2 --client-port 50052 --raft-port 60052 \
    --peers "1@127.0.0.1:60051,3@127.0.0.1:60053" \
    --data-dir "./data" &

sleep 1

# Node 3
cargo run -- --node-id 3 --client-port 50053 --raft-port 60053 \
    --peers "1@127.0.0.1:60051,2@127.0.0.1:60052" \
    --data-dir "./data" &

echo ""
echo "Cluster started!"
echo "  Node 1: client=50051, raft=60051, data=./data/node_1"
echo "  Node 2: client=50052, raft=60052, data=./data/node_2"
echo "  Node 3: client=50053, raft=60053, data=./data/node_3"
echo ""
echo "Press Ctrl+C to stop all nodes"

wait
