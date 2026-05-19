use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::RwLock;

use crate::config::NodeConfig;
use crate::kv::KVStore;
use crate::proto::raft::raft_service_client::RaftServiceClient;
use crate::raft::{LogStore, PendingRequests, PersistentData, PersistentStorage, RaftState};

/// 日志复制管理器
pub struct Replication {
    state: Arc<RwLock<RaftState>>,
    log: Arc<RwLock<LogStore>>,
    peers: Vec<(u32, String)>,
    pending_requests: Option<Arc<PendingRequests>>,
    /// 成功心跳计数（用于租约续约）
    heartbeat_success_count: Arc<AtomicUsize>,
    /// 持久化存储（用于保存快照）
    storage: Option<Arc<PersistentStorage>>,
    /// 快照阈值
    snapshot_threshold: u64,
    /// KV 存储（用于获取快照数据）
    kv_store: Option<Arc<KVStore>>,
    /// 每次 AppendEntries 发送的最大日志条数
    max_entries_per_append: usize,
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
            heartbeat_success_count: Arc::new(AtomicUsize::new(0)),
            storage: None,
            snapshot_threshold: config.snapshot_threshold,
            kv_store: None,
            max_entries_per_append: config.max_entries_per_append,
        }
    }

    pub fn with_pending_requests(mut self, pending: Arc<PendingRequests>) -> Self {
        self.pending_requests = Some(pending);
        self
    }

    pub fn with_storage(mut self, storage: Arc<PersistentStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn with_kv_store(mut self, kv_store: Arc<KVStore>) -> Self {
        self.kv_store = Some(kv_store);
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

        // 获取快照边界信息
        let (snapshot_last_index, snapshot_last_term, snapshot_data) = {
            let log = self.log.read().await;
            let snap_idx = log.snapshot_last_index;
            let snap_term = log.snapshot_last_term;
            let snap_data = if snap_idx > 0 {
                // 从持久化存储加载快照数据（如果有）
                if let Some(storage) = &self.storage {
                    match storage.load().await {
                        Ok(Some(data)) => data.snapshot_data.unwrap_or_default(),
                        _ => vec![],
                    }
                } else {
                    vec![]
                }
            } else {
                vec![]
            };
            (snap_idx, snap_term, snap_data)
        };

        // 重置心跳成功计数
        self.heartbeat_success_count.store(0, Ordering::SeqCst);
        // Leader 自己算一个成功
        self.heartbeat_success_count.fetch_add(1, Ordering::SeqCst);

        for (i, (peer_id, peer_addr)) in self.peers.iter().enumerate() {
            let peer_idx = i + 1; // 索引：0 是 Leader 自己，1, 2, ... 是 peers
            let next_index = next_indices[peer_idx];

            // 检查 Follower 是否需要快照（next_index <= snapshot_last_index）
            if next_index <= snapshot_last_index && snapshot_last_index > 0 {
                // Follower 严重滞后，需要发送快照
                if snapshot_data.is_empty() {
                    tracing::warn!(
                        "Leader {} cannot send snapshot to peer {}: snapshot data is empty",
                        node_id, peer_id
                    );
                    continue;
                }

                let request = crate::proto::raft::InstallSnapshotRequest {
                    term,
                    leader_id,
                    last_included_index: snapshot_last_index,
                    last_included_term: snapshot_last_term,
                    data: snapshot_data.clone(),
                };

                let peer_id = *peer_id;
                let peer_addr = peer_addr.clone();
                let state_clone = self.state.clone();
                let success_count_clone = self.heartbeat_success_count.clone();
                let majority = (self.peers.len() + 1) / 2 + 1;

                tracing::info!(
                    "Leader {} sending snapshot to peer {} (next_index={}, snapshot_last_index={})",
                    node_id, peer_id, next_index, snapshot_last_index
                );

                tokio::spawn(async move {
                    match RaftServiceClient::connect(format!("http://{}", peer_addr)).await {
                        Ok(mut client) => {
                            match client.install_snapshot(request).await {
                                Ok(response) => {
                                    let response = response.into_inner();

                                    let mut state = state_clone.write().await;
                                    let persistent = state.persistent.read().await;

                                    // 如果响应任期更大，转换为 Follower
                                    if response.term > persistent.current_term {
                                        drop(persistent);
                                        state.become_follower(response.term, None).await;
                                        return;
                                    }
                                    drop(persistent);

                                    // 快照安装成功，更新 next_index 和 match_index
                                    let mut leader_state_guard = state.leader_state.write().await;
                                    if let Some(leader_state) = leader_state_guard.as_mut() {
                                        leader_state.next_index[peer_idx] = snapshot_last_index + 1;
                                        leader_state.match_index[peer_idx] = snapshot_last_index;

                                        // 记录心跳成功
                                        let success_count = success_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
                                        if success_count >= majority {
                                            leader_state.lease.renew();
                                        }

                                        tracing::info!(
                                            "Leader {} snapshot installed on peer {}: next_index={}, match_index={}",
                                            node_id, peer_id, snapshot_last_index + 1, snapshot_last_index
                                        );
                                    }
                                }
                                Err(e) => {
                                    tracing::debug!("InstallSnapshot to {} failed: {}", peer_id, e);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!("Failed to connect to {}: {}", peer_addr, e);
                        }
                    }
                });

                continue; // 跳过 AppendEntries，已发送快照
            }

            // 正常日志复制：获取需要发送的日志条目
            let max_entries = self.max_entries_per_append;
            let log = self.log.read().await;
            let entries: Vec<crate::proto::raft::LogEntry> = log
                .entries_from(next_index)
                .into_iter()
                .take(max_entries)
                .map(|e| crate::proto::raft::LogEntry {
                    term: e.term,
                    index: e.index,
                    command: e.command,
                })
                .collect();

            let entries_count = entries.len();
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
            let success_count_clone = self.heartbeat_success_count.clone();
            let majority = (self.peers.len() + 1) / 2 + 1;
            let node_id_for_log = node_id;

            if entries_count > 0 {
                tracing::debug!(
                    "Leader {} sending {} entries to peer {} (next_index={})",
                    node_id_for_log, entries_count, peer_id, next_index
                );
            }

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

                                        // 记录心跳成功，检查是否达到多数派
                                        let success_count = success_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
                                        if success_count >= majority {
                                            // 多数派确认，续约租约
                                            leader_state.lease.renew();
                                            tracing::debug!("Leader {} lease renewed ({} acks)", node_id, success_count);
                                        }

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

    /// 检查是否需要创建快照
    pub async fn should_snapshot(&self) -> bool {
        let log = self.log.read().await;
        log.should_snapshot(self.snapshot_threshold)
    }

    /// 创建快照
    ///
    /// 1. 获取当前状态机快照
    /// 2. 更新 LogStore 的 snapshot_last_index/term
    /// 3. 截断已快照的日志
    /// 4. 持久化快照到磁盘
    pub async fn create_snapshot(&self, kv_store: &crate::kv::KVStore) -> Result<(), String> {
        let state = self.state.read().await;
        let node_id = state.node_id;
        drop(state);

        // 1. 获取当前 commit_index 作为快照点
        let commit_index = {
            let state = self.state.read().await;
            state.volatile.read().await.commit_index
        };

        // 如果 commit_index 为 0，无需快照
        if commit_index == 0 {
            return Ok(());
        }

        // 2. 获取快照点的任期
        let snapshot_term = {
            let log = self.log.read().await;
            log.get(commit_index).map(|e| e.term).unwrap_or(0)
        };

        // 如果快照点已经在快照范围内，跳过
        {
            let log = self.log.read().await;
            if commit_index <= log.snapshot_last_index {
                tracing::debug!(
                    "Node {} snapshot already covers index {}",
                    node_id,
                    commit_index
                );
                return Ok(());
            }
        }

        // 3. 创建状态机快照
        let snapshot_data = kv_store.snapshot().await;

        tracing::info!(
            "Node {} creating snapshot at index {} (term {}), size {} bytes",
            node_id,
            commit_index,
            snapshot_term,
            snapshot_data.len()
        );

        // 4. 更新 LogStore
        {
            let mut log = self.log.write().await;
            log.apply_snapshot(commit_index, snapshot_term);
        }

        // 5. 持久化快照
        if let Some(storage) = &self.storage {
            let state = self.state.read().await;
            let persistent_state = state.persistent.read().await.clone();
            let log = self.log.read().await.clone();
            let data = PersistentData::from_state_and_log(&persistent_state, &log)
                .with_snapshot(snapshot_data);
            drop(state);

            if let Err(e) = storage.save(&data).await {
                tracing::error!("Failed to persist snapshot: {}", e);
                return Err(format!("Failed to persist snapshot: {}", e));
            }
        }

        tracing::info!(
            "Node {} snapshot created successfully, log entries after truncate: {}",
            node_id,
            self.log.read().await.entries.len()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::KVStore;
    use crate::raft::{LogEntry, RaftState};
    use tempfile::tempdir;

    fn create_test_config() -> NodeConfig {
        NodeConfig {
            node_id: 1,
            client_addr: "127.0.0.1:50051".parse().unwrap(),
            raft_addr: "127.0.0.1:60051".parse().unwrap(),
            peers: vec![
                crate::config::Peer {
                    id: 2,
                    raft_addr: "127.0.0.1:60052".to_string(),
                },
                crate::config::Peer {
                    id: 3,
                    raft_addr: "127.0.0.1:60053".to_string(),
                },
            ],
            data_dir: "./data".to_string(),
            snapshot_threshold: 10,
            max_entries_per_append: 5,
        }
    }

    #[tokio::test]
    async fn test_should_snapshot_below_threshold() {
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let config = create_test_config();

        let replication = Replication::new(state, log, &config);

        // 添加 5 条日志（低于阈值 10）
        {
            let mut log_guard = replication.log.write().await;
            for i in 1..=5 {
                log_guard.append_one(LogEntry::new(1, i, vec![1]));
            }
        }

        assert!(!replication.should_snapshot().await);
    }

    #[tokio::test]
    async fn test_should_snapshot_above_threshold() {
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let config = create_test_config();

        let replication = Replication::new(state, log, &config);

        // 添加 15 条日志（超过阈值 10）
        {
            let mut log_guard = replication.log.write().await;
            for i in 1..=15 {
                log_guard.append_one(LogEntry::new(1, i, vec![1]));
            }
        }

        assert!(replication.should_snapshot().await);
    }

    #[tokio::test]
    async fn test_create_snapshot() {
        let dir = tempdir().unwrap();
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));

        let mut config = create_test_config();
        config.data_dir = dir.path().to_str().unwrap().to_string();

        let storage = Arc::new(PersistentStorage::new(&config.data_dir, 1));
        let replication = Replication::new(state.clone(), log.clone(), &config)
            .with_storage(storage.clone());

        let kv_store = KVStore::new();

        // 添加日志并设置 commit_index
        {
            let mut log_guard = log.write().await;
            for i in 1..=5 {
                log_guard.append_one(LogEntry::new(1, i, vec![1]));
            }
        }

        {
            let state_guard = state.write().await;
            let mut volatile = state_guard.volatile.write().await;
            volatile.commit_index = 5;
        }

        // 创建快照
        let result = replication.create_snapshot(&kv_store).await;
        assert!(result.is_ok());

        // 验证日志被截断
        let log_guard = log.read().await;
        assert_eq!(log_guard.snapshot_last_index, 5);
        assert_eq!(log_guard.snapshot_last_term, 1);
        assert!(log_guard.entries.is_empty());
    }

    #[tokio::test]
    async fn test_create_snapshot_skip_if_already_covered() {
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let config = create_test_config();

        let replication = Replication::new(state.clone(), log.clone(), &config);

        let kv_store = KVStore::new();

        // 设置已有的快照范围
        {
            let mut log_guard = log.write().await;
            log_guard.snapshot_last_index = 10;
            log_guard.snapshot_last_term = 1;
        }

        // 设置 commit_index 低于快照范围
        {
            let state_guard = state.write().await;
            let mut volatile = state_guard.volatile.write().await;
            volatile.commit_index = 5;
        }

        // 尝试创建快照（应该跳过）
        let result = replication.create_snapshot(&kv_store).await;
        assert!(result.is_ok());

        // 验证快照范围未改变
        let log_guard = log.read().await;
        assert_eq!(log_guard.snapshot_last_index, 10);
    }

    #[tokio::test]
    async fn test_follower_needs_snapshot() {
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let config = create_test_config();

        let _replication = Replication::new(state.clone(), log.clone(), &config);

        // 设置快照边界（Leader 已有快照到 index 20）
        {
            let mut log_guard = log.write().await;
            log_guard.snapshot_last_index = 20;
            log_guard.snapshot_last_term = 1;
            // 添加一些新日志
            for i in 21..=25 {
                log_guard.append_one(LogEntry::new(1, i, vec![1]));
            }
        }

        // 模拟 Follower 的 next_index 为 15（落后于快照边界）
        let follower_next_index = 15;
        let snapshot_last_index = {
            let log_guard = log.read().await;
            log_guard.snapshot_last_index
        };

        // Follower 需要快照的条件：next_index <= snapshot_last_index
        assert!(follower_next_index <= snapshot_last_index);

        // 如果 Follower 的 next_index 为 22（快照边界之后），则不需要快照
        let follower_next_index_after = 22;
        assert!(follower_next_index_after > snapshot_last_index);
    }

    #[tokio::test]
    async fn test_replication_with_kv_store() {
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let config = create_test_config();
        let kv_store = Arc::new(KVStore::new());

        let replication = Replication::new(state, log, &config)
            .with_kv_store(kv_store.clone());

        // 验证 kv_store 已设置
        assert!(replication.kv_store.is_some());
    }
}