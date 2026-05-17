#!/bin/bash

# Raft KV 存储集成测试

set -e

# 导入公共函数
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

# 测试计数器
TESTS_PASSED=0
TESTS_FAILED=0

# 测试结果记录
record_pass() {
    log_pass "$1"
    TESTS_PASSED=$((TESTS_PASSED + 1))
}

record_fail() {
    log_fail "$1"
    TESTS_FAILED=$((TESTS_FAILED + 1))
}

# 清理函数
cleanup() {
    log_info "Cleaning up..."
    stop_cluster
}

# 设置退出时清理
trap cleanup EXIT

echo "=========================================="
echo "  Raft KV Store Integration Test"
echo "=========================================="
echo ""

# ==========================================
# Phase 1: 基础测试
# ==========================================
echo "--- Phase 1: Basic Tests ---"

# 清理环境
stop_cluster
clean_data

# 启动集群
start_cluster

# 等待 Leader 选出
if ! wait_for_leader; then
    record_fail "Leader election"
    print_all_logs
    exit 1
fi

# 运行基础写入测试
log_info "Running basic write test..."
if run_node_test "basic"; then
    record_pass "Basic write test"
else
    record_fail "Basic write test"
fi

# ==========================================
# Phase 2: 一致性测试
# ==========================================
echo ""
echo "--- Phase 2: Consistency Test ---"

log_info "Running consistency test..."
if run_node_test "consistency"; then
    record_pass "Consistency test"
else
    record_fail "Consistency test"
fi

# ==========================================
# Phase 3: 持久化恢复测试
# ==========================================
echo ""
echo "--- Phase 3: Persistence Recovery Test ---"

log_info "Stopping cluster for restart test..."
stop_cluster
sleep 2

log_info "Restarting cluster (without cleaning data)..."
start_cluster

if ! wait_for_leader; then
    record_fail "Leader election after restart"
    print_all_logs
    exit 1
fi

log_info "Running recovery test..."
if run_node_test "recovery"; then
    record_pass "Recovery test"
else
    record_fail "Recovery test"
fi

# ==========================================
# 测试报告
# ==========================================
echo ""
echo "=========================================="
echo "  Test Summary"
echo "=========================================="
echo "  Passed: $TESTS_PASSED"
echo "  Failed: $TESTS_FAILED"
echo "=========================================="

if [ $TESTS_FAILED -eq 0 ]; then
    log_pass "All tests passed!"
    exit 0
else
    log_fail "Some tests failed!"
    print_all_logs
    exit 1
fi
