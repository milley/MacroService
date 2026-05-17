use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::NodeConfig;
use crate::proto::raft::{raft_service_client::RaftServiceClient, VoteRequest, VoteResponse};
use crate::raft::{LogStore, NodeRole, RaftState};

/// 选举管理器
pub struct Election {
    state: Arc<RwLock<RaftState>>,
    log: Arc<RwLock<LogStore>>,
    peers: Vec<(u32, String)>, // (node_id, raft_addr)
}

impl Election {
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
        }
    }

    /// 发起选举
    pub async fn start_election(&self) {
        let mut state = self.state.write().await;
        state.become_candidate().await;
        let term = state.persistent.read().await.current_term;
        let node_id = state.node_id;
        drop(state);

        let log = self.log.read().await;
        let last_log_index = log.last_index();
        let last_log_term = log.last_term();
        drop(log);

        tracing::info!(
            "Node {} starting election for term {}, last_log: ({}, {})",
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
        }

        let persistent = state.persistent.read().await;
        VoteResponse {
            term: persistent.current_term,
            vote_granted,
        }
    }
}