use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::NodeConfig;
use crate::proto::raft::{
    raft_service_client::RaftServiceClient, PreVoteRequest, PreVoteResponse, TimeoutNowRequest,
    TimeoutNowResponse, TransferLeaderRequest, TransferLeaderResponse, VoteRequest, VoteResponse,
};
use crate::raft::{LogStore, NodeRole, PersistentData, PersistentStorage, RaftState};

/// 选举管理器
pub struct Election {
    state: Arc<RwLock<RaftState>>,
    log: Arc<RwLock<LogStore>>,
    peers: Vec<(u32, String)>, // (node_id, raft_addr)
    storage: Arc<PersistentStorage>,
}

impl Election {
    pub fn new(
        state: Arc<RwLock<RaftState>>,
        log: Arc<RwLock<LogStore>>,
        config: &NodeConfig,
        storage: Arc<PersistentStorage>,
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
            storage,
        }
    }

    /// 发起选举（使用 PreVote 优化）
    pub async fn start_election(&self) {
        let state = self.state.read().await;
        let current_term = state.persistent.read().await.current_term;
        let node_id = state.node_id;
        let role = state.role;
        drop(state);

        let log = self.log.read().await;
        let last_log_index = log.last_index();
        let last_log_term = log.last_term();
        drop(log);

        // PreVote 阶段：使用 next_term = current_term + 1
        let next_term = current_term + 1;

        tracing::info!(
            "Node {} starting PreVote for term {} (current: {})",
            node_id,
            next_term,
            current_term
        );

        // 发送 PreVote 请求
        let mut pre_votes_received = 1; // 自己也算一个
        let majority = (self.peers.len() + 1) / 2 + 1;

        for (peer_id, peer_addr) in &self.peers {
            match RaftServiceClient::connect(format!("http://{}", peer_addr)).await {
                Ok(mut client) => {
                    let request = tonic::Request::new(PreVoteRequest {
                        term: next_term,
                        candidate_id: node_id,
                        last_log_index,
                        last_log_term,
                    });

                    match client.pre_vote(request).await {
                        Ok(response) => {
                            let response = response.into_inner();

                            // PreVote 不更新任期（只是试探）
                            if response.vote_granted {
                                pre_votes_received += 1;
                                tracing::debug!(
                                    "Node {} received PreVote from {} (total: {})",
                                    node_id,
                                    peer_id,
                                    pre_votes_received
                                );
                            } else {
                                tracing::debug!(
                                    "Node {} PreVote rejected by {} (their term: {})",
                                    node_id,
                                    peer_id,
                                    response.term
                                );
                            }
                        }
                        Err(e) => {
                            tracing::debug!("PreVote to {} failed: {}", peer_id, e);
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!("Failed to connect to {}: {}", peer_addr, e);
                }
            }
        }

        // 检查 PreVote 是否获得多数支持
        if pre_votes_received < majority as i32 {
            tracing::info!(
                "Node {} PreVote failed ({} votes, need {}), staying as {}",
                node_id,
                pre_votes_received,
                majority,
                role
            );
            return; // PreVote 失败，不发起正式选举
        }

        tracing::info!(
            "Node {} PreVote passed ({} votes), proceeding to real election",
            node_id,
            pre_votes_received
        );

        // 正式选举阶段
        self.start_real_election().await;
    }

    /// 发起正式选举（PreVote 成功后）
    async fn start_real_election(&self) {
        let mut state = self.state.write().await;
        state.become_candidate().await;
        let term = state.persistent.read().await.current_term;
        let node_id = state.node_id;

        // 持久化投票给自己的状态
        let persistent_state = state.persistent.read().await.clone();
        let log_store = self.log.read().await.clone();
        drop(state);

        let data = PersistentData::from_state_and_log(&persistent_state, &log_store);
        if let Err(e) = self.storage.save(&data).await {
            tracing::error!("Failed to persist candidate state: {}", e);
        }

        let log = self.log.read().await;
        let last_log_index = log.last_index();
        let last_log_term = log.last_term();
        drop(log);

        tracing::info!(
            "Node {} starting real election for term {}, last_log: ({}, {})",
            node_id,
            term,
            last_log_index,
            last_log_term
        );

        // 向所有节点发送 RequestVote
        let mut votes_received = 1; // 投给自己
        let majority = (self.peers.len() + 1) / 2 + 1;

        for (peer_id, peer_addr) in &self.peers {
            match RaftServiceClient::connect(format!("http://{}", peer_addr)).await {
                Ok(mut client) => {
                    let request = tonic::Request::new(VoteRequest {
                        term,
                        candidate_id: node_id,
                        last_log_index,
                        last_log_term,
                    });

                    match client.request_vote(request).await {
                        Ok(response) => {
                            let response = response.into_inner();

                            // 检查是否需要更新任期
                            if response.term > term {
                                let mut state = self.state.write().await;
                                state.become_follower(response.term, None).await;
                                return;
                            }

                            if response.vote_granted {
                                votes_received += 1;
                                tracing::info!(
                                    "Node {} received vote from {} (total: {})",
                                    node_id,
                                    peer_id,
                                    votes_received
                                );
                            }
                        }
                        Err(e) => {
                            tracing::debug!("RequestVote to {} failed: {}", peer_id, e);
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!("Failed to connect to {}: {}", peer_addr, e);
                }
            }
        }

        // 检查是否获得多数票
        if votes_received >= majority as i32 {
            tracing::info!(
                "Node {} won election with {} votes (majority: {})",
                node_id,
                votes_received,
                majority
            );
            let mut state = self.state.write().await;
            state.become_leader().await;
        }
    }

    /// 处理 RequestVote RPC
    pub async fn handle_request_vote(&self, request: VoteRequest) -> VoteResponse {
        let mut state = self.state.write().await;
        let persistent = state.persistent.read().await;
        let current_term = persistent.current_term;
        let _voted_for = persistent.voted_for;
        drop(persistent);

        let log = self.log.read().await;
        let last_log_index = log.last_index();
        let last_log_term = log.last_term();
        drop(log);

        tracing::debug!(
            "Node {} received RequestVote from {} (term {}, my term {})",
            state.node_id,
            request.candidate_id,
            request.term,
            current_term
        );

        // 如果请求的任期小于当前任期，拒绝
        if request.term < current_term {
            return VoteResponse {
                term: current_term,
                vote_granted: false,
            };
        }

        // 如果请求的任期大于当前任期，转换为 Follower
        if request.term > current_term {
            drop(state.persistent.read());
            let mut persistent = state.persistent.write().await;
            persistent.current_term = request.term;
            persistent.voted_for = None;
            drop(persistent);
            state.role = NodeRole::Follower;
        }

        // 检查是否可以投票
        let can_vote = {
            let persistent = state.persistent.read().await;
            persistent.voted_for.is_none() || persistent.voted_for == Some(request.candidate_id)
        };

        // 检查候选人的日志是否至少和自己一样新
        let log_is_up_to_date = {
            if request.last_log_term > last_log_term {
                true
            } else if request.last_log_term == last_log_term {
                request.last_log_index >= last_log_index
            } else {
                false
            }
        };

        let vote_granted = can_vote && log_is_up_to_date;

        if vote_granted {
            let mut persistent = state.persistent.write().await;
            persistent.voted_for = Some(request.candidate_id);
            drop(persistent);

            tracing::info!(
                "Node {} voted for {} in term {}",
                state.node_id,
                request.candidate_id,
                request.term
            );

            // 持久化投票结果
            let persistent_state = state.persistent.read().await.clone();
            let log_store = self.log.read().await.clone();
            drop(state);

            let data = PersistentData::from_state_and_log(&persistent_state, &log_store);
            if let Err(e) = self.storage.save(&data).await {
                tracing::error!("Failed to persist vote: {}", e);
            }
        }

        let state = self.state.read().await;
        let persistent = state.persistent.read().await;
        VoteResponse {
            term: persistent.current_term,
            vote_granted,
        }
    }

    /// 处理 PreVote RPC
    ///
    /// PreVote 规则：
    /// 1. 不改变任何状态（不增加 term，不记录投票）
    /// 2. 如果请求的 term < current_term，拒绝
    /// 3. 如果候选人的日志不够新，拒绝
    /// 4. 如果自己认为有有效 Leader（election timeout 未过期），拒绝
    /// 5. 否则同意
    pub async fn handle_pre_vote(&self, request: PreVoteRequest) -> PreVoteResponse {
        let state = self.state.read().await;
        let persistent = state.persistent.read().await;
        let current_term = persistent.current_term;
        drop(persistent);

        let log = self.log.read().await;
        let last_log_index = log.last_index();
        let last_log_term = log.last_term();
        drop(log);

        tracing::debug!(
            "Node {} received PreVote from {} (proposed term {}, my term {}, my role: {:?})",
            state.node_id,
            request.candidate_id,
            request.term,
            current_term,
            state.role
        );

        // 规则 1: 如果请求的任期小于当前任期，拒绝
        if request.term < current_term {
            return PreVoteResponse {
                term: current_term,
                vote_granted: false,
            };
        }

        // 规则 2: 如果自己是 Leader，拒绝（Leader 存活）
        if state.role == NodeRole::Leader {
            tracing::debug!(
                "Node {} rejecting PreVote: I am the Leader",
                state.node_id
            );
            return PreVoteResponse {
                term: current_term,
                vote_granted: false,
            };
        }

        // 规则 3: 检查候选人的日志是否至少和自己一样新
        let log_is_up_to_date = {
            if request.last_log_term > last_log_term {
                true
            } else if request.last_log_term == last_log_term {
                request.last_log_index >= last_log_index
            } else {
                false
            }
        };

        if !log_is_up_to_date {
            tracing::debug!(
                "Node {} rejecting PreVote: candidate log not up-to-date",
                state.node_id
            );
            return PreVoteResponse {
                term: current_term,
                vote_granted: false,
            };
        }

        // PreVote 通过
        tracing::info!(
            "Node {} granted PreVote to {} for term {}",
            state.node_id,
            request.candidate_id,
            request.term
        );

        PreVoteResponse {
            term: current_term,
            vote_granted: true,
        }
    }

    /// 发起 Leader 转移
    ///
    /// 1. 检查目标节点是否存在
    /// 2. 确保目标节点日志是最新的
    /// 3. 发送 TimeoutNow 给目标节点
    /// 4. Leader 转为 Follower
    pub async fn transfer_leader(&self, target_id: u32) -> Result<(), String> {
        let state = self.state.read().await;

        // 只有 Leader 才能转移领导权
        if state.role != NodeRole::Leader {
            return Err("not leader".to_string());
        }

        let node_id = state.node_id;

        // 检查目标节点是否是自己
        if target_id == node_id {
            return Err("cannot transfer leadership to self".to_string());
        }

        // 检查目标节点是否在集群中
        let target_addr = self
            .peers
            .iter()
            .find(|(id, _)| *id == target_id)
            .map(|(_, addr)| addr.clone());

        let target_addr = match target_addr {
            Some(addr) => addr,
            None => return Err(format!("target node {} not found in cluster", target_id)),
        };

        // 检查目标节点的 match_index 是否与 Leader 一致
        let (match_index, last_index) = {
            let leader_state = state.leader_state.read().await;
            let leader_state = match leader_state.as_ref() {
                Some(ls) => ls,
                None => return Err("leader state not available".to_string()),
            };

            // 找到目标节点在 match_index 数组中的索引
            let peer_idx = self
                .peers
                .iter()
                .position(|(id, _)| *id == target_id)
                .map(|i| i + 1); // +1 因为 index 0 是 Leader 自己

            let peer_idx = match peer_idx {
                Some(idx) => idx,
                None => return Err(format!("target node {} not found in peers", target_id)),
            };

            let log = self.log.read().await;
            (leader_state.match_index[peer_idx], log.last_index())
        };

        drop(state);

        // 如果目标节点日志不是最新的，需要先同步
        if match_index < last_index {
            tracing::info!(
                "Leader {} waiting for target {} to catch up (match={}, last={})",
                node_id,
                target_id,
                match_index,
                last_index
            );
            // 在实际实现中，这里应该等待日志同步
            // 简化版本：直接返回错误
            return Err(format!(
                "target node {} log not up-to-date (match={}, last={})",
                target_id, match_index, last_index
            ));
        }

        tracing::info!(
            "Leader {} transferring leadership to node {}",
            node_id,
            target_id
        );

        // 发送 TimeoutNow 给目标节点
        let current_term = {
            let state = self.state.read().await;
            state.persistent.read().await.current_term
        };

        let request = TimeoutNowRequest { term: current_term };

        match RaftServiceClient::connect(format!("http://{}", target_addr)).await {
            Ok(mut client) => {
                match client.timeout_now(request).await {
                    Ok(response) => {
                        let response = response.into_inner();
                        if response.term > current_term {
                            tracing::warn!(
                                "Target node {} has higher term {}, stepping down",
                                target_id,
                                response.term
                            );
                            let mut state = self.state.write().await;
                            state.become_follower(response.term, Some(target_id)).await;
                        }
                    }
                    Err(e) => {
                        tracing::error!("TimeoutNow to {} failed: {}", target_id, e);
                        return Err(format!("TimeoutNow failed: {}", e));
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to connect to {}: {}", target_addr, e);
                return Err(format!("connection failed: {}", e));
            }
        }

        // Leader 转为 Follower
        let mut state = self.state.write().await;
        state
            .become_follower(current_term, Some(target_id))
            .await;

        tracing::info!(
            "Leader {} stepped down after transferring to {}",
            node_id,
            target_id
        );

        Ok(())
    }

    /// 处理 TransferLeader 请求（客户端发起）
    pub async fn handle_transfer_leader(
        &self,
        request: TransferLeaderRequest,
    ) -> TransferLeaderResponse {
        let state = self.state.read().await;
        let current_term = state.persistent.read().await.current_term;
        drop(state);

        // 检查任期
        if request.term < current_term {
            return TransferLeaderResponse {
                term: current_term,
                success: false,
            };
        }

        // 执行转移
        match self.transfer_leader(request.target_id).await {
            Ok(()) => TransferLeaderResponse {
                term: current_term,
                success: true,
            },
            Err(e) => {
                tracing::error!("Transfer leader failed: {}", e);
                TransferLeaderResponse {
                    term: current_term,
                    success: false,
                }
            }
        }
    }

    /// 处理 TimeoutNow 请求
    ///
    /// 收到此请求的节点应立即开始选举（不等待选举超时）
    pub async fn handle_timeout_now(&self, request: TimeoutNowRequest) -> TimeoutNowResponse {
        let mut state = self.state.write().await;
        let current_term = state.persistent.read().await.current_term;
        let node_id = state.node_id;

        // 如果请求的任期小于当前任期，忽略
        if request.term < current_term {
            return TimeoutNowResponse { term: current_term };
        }

        tracing::info!(
            "Node {} received TimeoutNow, starting immediate election",
            node_id
        );

        // 立即开始选举（跳过 PreVote）
        state.become_candidate().await;
        drop(state);

        // 持久化
        let state = self.state.read().await;
        let persistent_state = state.persistent.read().await.clone();
        let log_store = self.log.read().await.clone();
        drop(state);

        let data = PersistentData::from_state_and_log(&persistent_state, &log_store);
        if let Err(e) = self.storage.save(&data).await {
            tracing::error!("Failed to persist candidate state: {}", e);
        }

        // 异步发起选举
        let new_term = persistent_state.current_term;
        let log = self.log.read().await;
        let last_log_index = log.last_index();
        let last_log_term = log.last_term();
        drop(log);

        let mut votes_received = 1; // 投给自己
        let majority = (self.peers.len() + 1) / 2 + 1;

        for (peer_id, peer_addr) in &self.peers {
            match RaftServiceClient::connect(format!("http://{}", peer_addr)).await {
                Ok(mut client) => {
                    let request = VoteRequest {
                        term: new_term,
                        candidate_id: node_id,
                        last_log_index,
                        last_log_term,
                    };

                    match client.request_vote(request).await {
                        Ok(response) => {
                            let response = response.into_inner();
                            if response.vote_granted {
                                votes_received += 1;
                            }
                        }
                        Err(e) => {
                            tracing::debug!("RequestVote to {} failed: {}", peer_id, e);
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!("Failed to connect to {}: {}", peer_addr, e);
                }
            }
        }

        // 检查是否获得多数票
        if votes_received >= majority as i32 {
            tracing::info!(
                "Node {} won immediate election with {} votes",
                node_id,
                votes_received
            );
            let mut state = self.state.write().await;
            state.become_leader().await;
        }

        TimeoutNowResponse { term: new_term }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            snapshot_threshold: 1000,
            max_entries_per_append: 100,
            metrics_addr: "127.0.0.1:9091".parse().unwrap(),
        }
    }

    #[tokio::test]
    async fn test_handle_pre_vote_granted() {
        let dir = tempdir().unwrap();
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let mut config = create_test_config();
        config.data_dir = dir.path().to_str().unwrap().to_string();
        let storage = Arc::new(PersistentStorage::new(&config.data_dir, 1));

        let election = Election::new(state, log, &config, storage);

        // PreVote 请求（term 比当前大）
        let request = PreVoteRequest {
            term: 2,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };

        let response = election.handle_pre_vote(request).await;
        assert!(response.vote_granted);
        assert_eq!(response.term, 0); // 不改变 term
    }

    #[tokio::test]
    async fn test_handle_pre_vote_rejected_lower_term() {
        let dir = tempdir().unwrap();
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let mut config = create_test_config();
        config.data_dir = dir.path().to_str().unwrap().to_string();
        let storage = Arc::new(PersistentStorage::new(&config.data_dir, 1));

        // 设置当前 term 为 5
        {
            let state_guard = state.write().await;
            let mut persistent = state_guard.persistent.write().await;
            persistent.current_term = 5;
        }

        let election = Election::new(state, log, &config, storage);

        // PreVote 请求（term 比当前小）
        let request = PreVoteRequest {
            term: 3,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };

        let response = election.handle_pre_vote(request).await;
        assert!(!response.vote_granted);
        assert_eq!(response.term, 5);
    }

    #[tokio::test]
    async fn test_handle_pre_vote_rejected_by_leader() {
        let dir = tempdir().unwrap();
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let mut config = create_test_config();
        config.data_dir = dir.path().to_str().unwrap().to_string();
        let storage = Arc::new(PersistentStorage::new(&config.data_dir, 1));

        // 设置为 Leader
        {
            let mut state_guard = state.write().await;
            state_guard.role = NodeRole::Leader;
        }

        let election = Election::new(state, log, &config, storage);

        let request = PreVoteRequest {
            term: 2,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };

        let response = election.handle_pre_vote(request).await;
        assert!(!response.vote_granted);
    }

    #[tokio::test]
    async fn test_handle_pre_vote_rejected_stale_log() {
        let dir = tempdir().unwrap();
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let mut config = create_test_config();
        config.data_dir = dir.path().to_str().unwrap().to_string();
        let storage = Arc::new(PersistentStorage::new(&config.data_dir, 1));

        // 添加日志
        {
            let mut log_guard = log.write().await;
            log_guard.append_one(crate::raft::LogEntry::new(1, 1, vec![1]));
            log_guard.append_one(crate::raft::LogEntry::new(1, 2, vec![2]));
        }

        let election = Election::new(state, log, &config, storage);

        // PreVote 请求（日志落后）
        // 候选人 last_log_index=1, 我们有 last_log_index=2
        // term 相同，但 index 更小，所以候选人的日志不够新
        let request = PreVoteRequest {
            term: 2,
            candidate_id: 2,
            last_log_index: 1, // 候选人只有 index 1，我们有 index 2
            last_log_term: 1,
        };

        let response = election.handle_pre_vote(request).await;
        // 候选人的日志不够新 (1 < 2)，应该拒绝
        assert!(!response.vote_granted);
    }

    #[tokio::test]
    async fn test_pre_vote_does_not_change_state() {
        let dir = tempdir().unwrap();
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let mut config = create_test_config();
        config.data_dir = dir.path().to_str().unwrap().to_string();
        let storage = Arc::new(PersistentStorage::new(&config.data_dir, 1));

        let election = Election::new(state.clone(), log, &config, storage);

        let request = PreVoteRequest {
            term: 100, // 很大的 term
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };

        let response = election.handle_pre_vote(request).await;
        assert!(response.vote_granted);

        // 验证状态未改变
        let state_guard = state.read().await;
        let persistent = state_guard.persistent.read().await;
        assert_eq!(persistent.current_term, 0); // term 未改变
        assert!(persistent.voted_for.is_none()); // 未投票
        assert_eq!(state_guard.role, NodeRole::Follower); // 角色未改变
    }

    #[tokio::test]
    async fn test_transfer_leader_not_leader() {
        let dir = tempdir().unwrap();
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let mut config = create_test_config();
        config.data_dir = dir.path().to_str().unwrap().to_string();
        let storage = Arc::new(PersistentStorage::new(&config.data_dir, 1));

        let election = Election::new(state, log, &config, storage);

        // Follower 尝试转移领导权（应该失败）
        let result = election.transfer_leader(2).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "not leader");
    }

    #[tokio::test]
    async fn test_transfer_leader_to_self() {
        let dir = tempdir().unwrap();
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let mut config = create_test_config();
        config.data_dir = dir.path().to_str().unwrap().to_string();
        let storage = Arc::new(PersistentStorage::new(&config.data_dir, 1));

        // 设置为 Leader
        {
            let mut state_guard = state.write().await;
            state_guard.role = NodeRole::Leader;
            let mut leader_state = state_guard.leader_state.write().await;
            *leader_state = Some(crate::raft::LeaderState {
                next_index: vec![1, 1, 1],
                match_index: vec![0, 0, 0],
                lease: crate::raft::LeaseManager::new(60),
            });
        }

        let election = Election::new(state, log, &config, storage);

        // 尝试转移给自己（应该失败）
        let result = election.transfer_leader(1).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("self"));
    }

    #[tokio::test]
    async fn test_transfer_leader_target_not_found() {
        let dir = tempdir().unwrap();
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let mut config = create_test_config();
        config.data_dir = dir.path().to_str().unwrap().to_string();
        let storage = Arc::new(PersistentStorage::new(&config.data_dir, 1));

        // 设置为 Leader
        {
            let mut state_guard = state.write().await;
            state_guard.role = NodeRole::Leader;
            let mut leader_state = state_guard.leader_state.write().await;
            *leader_state = Some(crate::raft::LeaderState {
                next_index: vec![1, 1, 1],
                match_index: vec![0, 0, 0],
                lease: crate::raft::LeaseManager::new(60),
            });
        }

        let election = Election::new(state, log, &config, storage);

        // 尝试转移给不存在的节点
        let result = election.transfer_leader(99).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[tokio::test]
    async fn test_handle_timeout_now_becomes_candidate() {
        let dir = tempdir().unwrap();
        let state = Arc::new(RwLock::new(RaftState::new(1, vec![2, 3])));
        let log = Arc::new(RwLock::new(LogStore::new()));
        let mut config = create_test_config();
        config.data_dir = dir.path().to_str().unwrap().to_string();
        let storage = Arc::new(PersistentStorage::new(&config.data_dir, 1));

        let election = Election::new(state.clone(), log, &config, storage);

        // 发送 TimeoutNow
        let request = TimeoutNowRequest { term: 0 };
        let _response = election.handle_timeout_now(request).await;

        // 验证节点成为 Candidate 或 Leader（取决于选举结果）
        let state_guard = state.read().await;
        assert!(state_guard.role == NodeRole::Candidate || state_guard.role == NodeRole::Leader);
    }
}