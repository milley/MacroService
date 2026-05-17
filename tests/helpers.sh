#!/bin/bash

# 集成测试公共函数

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 项目路径
PROJECT_DIR="/Users/milley/DevSpace/RustProject/claude-dev-training-space/04_MacroService"
SERVER_DIR="$PROJECT_DIR/server-rust"
CLIENT_DIR="$PROJECT_DIR/client-node"
DATA_DIR="$SERVER_DIR/data"

# 端口配置
CLIENT_PORTS=(50051 50052 50053)
RAFT_PORTS=(60051 60052 60053)

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
}

# 停止集群
stop_cluster() {
    log_info "Stopping cluster..."
    lsof -i :50051 -i :50052 -i :50053 -i :60051 -i :60052 -i :60053 2>/dev/null | grep LISTEN | awk '{print $2}' | xargs -r kill 2>/dev/null
    sleep 1
    log_info "Cluster stopped"
}

# 清理数据目录
clean_data() {
    log_info "Cleaning data directory..."
    rm -rf "$DATA_DIR"
    log_info "Data directory cleaned"
}

# 启动单个节点
start_node() {
    local node_id=$1
    local client_port=$2
    local raft_port=$3
    local peers=$4

    cd "$SERVER_DIR"
    cargo run --quiet -- \
        --node-id $node_id \
        --client-port $client_port \
        --raft-port $raft_port \
        --peers "$peers" \
        --data-dir "./data" \
        > "$DATA_DIR/node_${node_id}.log" 2>&1 &

    log_info "Node $node_id started (PID: $!)"
}

# 启动集群
start_cluster() {
    log_info "Starting 3-node cluster..."

    mkdir -p "$DATA_DIR"

    # Node 1
    start_node 1 50051 60051 "2@127.0.0.1:60052,3@127.0.0.1:60053"
    sleep 1

    # Node 2
    start_node 2 50052 60052 "1@127.0.0.1:60051,3@127.0.0.1:60053"
    sleep 1

    # Node 3
    start_node 3 50053 60053 "1@127.0.0.1:60051,2@127.0.0.1:60052"

    log_info "Cluster started"
}

# 等待 Leader 选出
wait_for_leader() {
    log_info "Waiting for leader election..."
    local max_attempts=30
    local attempt=0

    # 首先等待节点启动
    sleep 3

    while [ $attempt -lt $max_attempts ]; do
        # 尝试写入验证 Leader 是否就绪
        cd "$CLIENT_DIR"
        if node health-check.js >/dev/null 2>&1; then
            log_info "Leader elected and ready"
            return 0
        fi

        attempt=$((attempt + 1))
        sleep 1
    done

    log_error "Leader election timeout"
    return 1
}

# 运行 Node.js 测试
run_node_test() {
    local test_type=$1
    cd "$CLIENT_DIR"
    node integration-test.js "$test_type"
    return $?
}

# 获取节点日志
get_node_log() {
    local node_id=$1
    if [ -f "$DATA_DIR/node_${node_id}.log" ]; then
        tail -20 "$DATA_DIR/node_${node_id}.log"
    fi
}

# 打印所有节点日志
print_all_logs() {
    log_warn "=== Node 1 logs ==="
    get_node_log 1
    log_warn "=== Node 2 logs ==="
    get_node_log 2
    log_warn "=== Node 3 logs ==="
    get_node_log 3
}
