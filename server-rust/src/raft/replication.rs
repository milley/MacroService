use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::NodeConfig;
use crate::proto::raft::raft_service_client::RaftServiceClient;
use crate::raft::{LogStore, PendingRequests, RaftState};

/// 日志复制管理器
pub struct Replication {
    state: Arc<RwLock<RaftState>>,
    log: Arc<RwLock<LogStore>>,
    peers: Vec<(u32, String)>,
    pending_requests: Option<Arc<PendingRequests>>,
}

impl Replication {
    pub fn new(
        state: Arc<RwLock<RaftState>>,
        log: Arc<RwLock<LogStore>>,
        config: &NodeConfig,
    ) -> Self {
        let peers: Vec<(u32, String)> = config
            .peers
            .iter()
            .map(|p| (p.id, p.raft_addr.clone()))
            .collect();

        Self {
            state,
            log,
            peers,
            pending_requests: None,
        }
    }

    pub fn with_pending_requests(mut self, pending: Arc<PendingRequests>) -> Self {
        self.pending_requests = Some(pending);
        self
    }

    /// 发送心跳和日志复制
    pub async fn send_heartbeat(&self) {
        let state = self.state.read().await;

        // 只有 Leader 才发送心跳
        if state.role != crate::raft::NodeRole::Leader {
            return;
        }

        let persistent = state.persistent.read().await;
        let term = persistent.current_term;
        let leader_id = state.node_id;
        drop(persistent);

        let volatile = state.volatile.read().await;
        let commit_index = volatile.commit_index;
        drop(volatile);

        // 获取 Leader 状态和 next_index
        let next_indices: Vec<u64> = {
            let leader_state_guard = state.leader_state.read().await;
            if let Some(leader_state) = leader_state_guard.as_ref() {
                leader_state.next_index.clone()
            } else {
                return;
            }
        };

        let node_id = state.node_id;
        drop(state);

        for (i, (peer_id, peer_addr)) in self.peers.iter().enumerate() {
            let peer_idx = i + 1; // 索引：0 是 Leader 自己，1, 2, ... 是 peers
            let next_index = next_indices[peer_idx];

            // 获取需要发送的日志条目
            let log = self.log.read().await;
            let entries: Vec<crate::proto::raft::LogEntry> = log
                .entries_from(next_index)
                .into_iter()
                .map(|e| crate::proto::raft::LogEntry {
                    term: e.term,
                    index: e.index,
                    command: e.command,
                })
                .collect();

            let prev_log_index = if next_index > 1 { next_index - 1 } else { 0 };
            let prev_log_term = if prev_log_index > 0 {
                log.get(prev_log_index).map(|e| e.term).unwrap_or(0)
            } else {
                0
            };
            drop(log);

            let request = crate::proto::raft::AppendEntriesRequest {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: commit_index,
            };

            // 发送 RPC
            let peer_id = *peer_id;
            let peer_addr = peer_addr.clone();
            let state_clone = self.state.clone();

            tokio::spawn(async move {
                match RaftServiceClient::connect(format!("http://{}", peer_addr)).await {
                    Ok(mut client) => {
                        match client.append_entries(request).await {
                            Ok(response) => {
                                let response = response.into_inner();

                                // 更新 Leader 状态
                                let mut state = state_clone.write().await;
                                let persistent = state.persistent.read().await;

                                // 如果响应任期更大，转换为 Follower
                                if response.term > persistent.current_term {
                                    drop(persistent);
                                    state.become_follower(response.term, None).await;
                                    return;
                                }
                                drop(persistent);

                                let mut leader_state_guard = state.leader_state.write().await;
                                if let Some(leader_state) = leader_state_guard.as_mut() {
                                    if response.success {
                                        leader_state.match_index[peer_idx] = response.match_index;
                                        leader_state.next_index[peer_idx] = response.match_index + 1;
                                        tracing::debug!(
                                            "Leader {} updated match_index for peer {} (idx {}) to {}",
                                            node_id,
                                            peer_id,
                                            peer_idx,
                                            response.match_index
                                        );
                                    } else {
                                        // 失败，减少 next_index 重试
                                        if leader_state.next_index[peer_idx] > 1 {
                                            leader_state.next_index[peer_idx] -= 1;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::debug!("AppendEntries to {} failed: {}", peer_id, e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Failed to connect to {}: {}", peer_addr, e);
                    }
                }
            });
        }
    }

    /// 更新 commit index
    pub async fn update_commit_index(&self) {
        let state = self.state.read().await;

        // 只有 Leader 才更新 commit index
        if state.role != crate::raft::NodeRole::Leader {
            return;
        }

        let persistent = state.persistent.read().await;
        let current_term = persistent.current_term;
        drop(persistent);

        // 获取 match_index 数组
        let match_indices: Vec<u64> = {
            let leader_state_guard = state.leader_state.read().await;
            if let Some(leader_state) = leader_state_guard.as_ref() {
                leader_state.match_index.clone()
            } else {
                return;
            }
        };

        let node_id = state.node_id;
        let commit_index = state.volatile.read().await.commit_index;
        drop(state);

        let log = self.log.read().await;
        let last_log_index = log.last_index();
        drop(log);

        // 找到可以 commit 的最高索引
        for n in (commit_index + 1..=last_log_index).rev() {
            let log = self.log.read().await;
            let entry_term = log.get(n).map(|e| e.term).unwrap_or(0);
            drop(log);

            if entry_term != current_term {
                continue;
            }

            // 计算有多少节点的 match_index >= N
            let mut count = 1; // Leader 自己
            for (i, _) in self.peers.iter().enumerate() {
                let peer_idx = i + 1;
                if match_indices[peer_idx] >= n {
                    count += 1;
                }
            }

            let majority = (self.peers.len() + 1) / 2 + 1;
            if count >= majority {
                let state = self.state.write().await;
                let mut volatile = state.volatile.write().await;
                tracing::info!(
                    "Leader {} advancing commit_index to {} (count: {}, majority: {})",
                    node_id,
                    n,
                    count,
                    majority
                );
                volatile.commit_index = n;
                break;
            }
        }
    }

    /// 应用已提交的日志到状态机
    pub async fn apply_committed_entries(&self, kv_store: &crate::kv::KVStore) {
        let state = self.state.read().await;
        let commit_index = state.volatile.read().await.commit_index;
        let last_applied = state.volatile.read().await.last_applied;
        let node_id = state.node_id;
        drop(state);

        if commit_index <= last_applied {
            return;
        }

        let log = self.log.read().await;

        for i in (last_applied + 1)..=commit_index {
            if let Some(entry) = log.get(i) {
                if !entry.command.is_empty() {
                    tracing::info!(
                        "Node {} applying log entry {} (term {})",
                        node_id,
                        i,
                        entry.term
                    );
                    kv_store.apply_command(&entry.command).await.unwrap_or_else(|e| {
                        tracing::error!("Failed to apply command: {}", e);
                    });
                }
            }
        }

        let state = self.state.write().await;
        let mut volatile = state.volatile.write().await;
        volatile.last_applied = commit_index;
        drop(volatile);
        drop(state);

        // 通知等待的客户端请求
        if let Some(pending) = &self.pending_requests {
            pending.notify_committed(commit_index).await;
        }
    }
}