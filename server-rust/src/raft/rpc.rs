use std::sync::Arc;
use tonic::{Request, Response, Status};
use tokio::sync::RwLock;

use crate::kv::KVStore;
use crate::proto::raft::{
    raft_service_server::RaftService, AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, PreVoteRequest, PreVoteResponse,
    TimeoutNowRequest, TimeoutNowResponse, TransferLeaderRequest, TransferLeaderResponse,
    VoteRequest, VoteResponse,
};
use crate::raft::{Election, ElectionTimer, LogStore, PersistentData, PersistentStorage, RaftState};

/// Raft gRPC 服务实现
pub struct RaftServiceImpl {
    state: Arc<RwLock<RaftState>>,
    log: Arc<RwLock<LogStore>>,
    election: Arc<Election>,
    election_timer: Arc<RwLock<ElectionTimer>>,
    storage: Arc<PersistentStorage>,
    kv_store: Arc<KVStore>,
}

impl RaftServiceImpl {
    pub fn new(
        state: Arc<RwLock<RaftState>>,
        log: Arc<RwLock<LogStore>>,
        election: Arc<Election>,
        election_timer: Arc<RwLock<ElectionTimer>>,
        storage: Arc<PersistentStorage>,
        kv_store: Arc<KVStore>,
    ) -> Self {
        Self {
            state,
            log,
            election,
            election_timer,
            storage,
            kv_store,
        }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn request_vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        let req = request.into_inner();

        // 重置选举定时器
        let mut timer = self.election_timer.write().await;
        timer.reset();
        drop(timer);

        let response = self.election.handle_request_vote(req).await;
        Ok(Response::new(response))
    }

    async fn pre_vote(
        &self,
        request: Request<PreVoteRequest>,
    ) -> Result<Response<PreVoteResponse>, Status> {
        let req = request.into_inner();

        // PreVote 不重置选举定时器（不改变状态）
        let response = self.election.handle_pre_vote(req).await;
        Ok(Response::new(response))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();

        // 重置选举定时器（收到心跳）
        let mut timer = self.election_timer.write().await;
        timer.reset();
        drop(timer);

        let mut state = self.state.write().await;
        let persistent = state.persistent.read().await;
        let current_term = persistent.current_term;
        drop(persistent);

        // 如果 term < currentTerm，拒绝
        if req.term < current_term {
            return Ok(Response::new(AppendEntriesResponse {
                term: current_term,
                success: false,
                match_index: 0,
            }));
        }

        // 如果 term > currentTerm，更新任期并转换为 Follower
        if req.term > current_term {
            state.become_follower(req.term, Some(req.leader_id)).await;
        } else if state.role == crate::raft::NodeRole::Candidate {
            // 同一任期但收到其他 Leader 的心跳
            state.become_follower(req.term, Some(req.leader_id)).await;
        }

        // 更新已知 Leader
        let mut leader_id = state.leader_id.write().await;
        *leader_id = Some(req.leader_id);
        drop(leader_id);

        // 检查日志一致性
        let mut log = self.log.write().await;
        if !log.match_entry(req.prev_log_index, req.prev_log_term) {
            tracing::debug!(
                "Node {} log mismatch: expected ({}, {}), last: ({}, {})",
                state.node_id,
                req.prev_log_index,
                req.prev_log_term,
                log.last_index(),
                log.last_term()
            );
            return Ok(Response::new(AppendEntriesResponse {
                term: req.term,
                success: false,
                match_index: log.last_index(),
            }));
        }

        // 追加日志条目
        if !req.entries.is_empty() {
            // 转换 Proto LogEntry 到内部 LogEntry
            let entries: Vec<crate::raft::LogEntry> = req
                .entries
                .into_iter()
                .map(|e| crate::raft::LogEntry::new(e.term, e.index, e.command))
                .collect();

            // 如果有冲突的条目，删除它们
            if let Some(first_entry) = entries.first() {
                log.truncate(first_entry.index);
            }

            log.append(entries);
            tracing::debug!(
                "Node {} appended entries, last_index: {}",
                state.node_id,
                log.last_index()
            );
        }

        let match_index = log.last_index();

        // 更新 commit index
        if req.leader_commit > 0 {
            let mut volatile = state.volatile.write().await;
            volatile.commit_index = std::cmp::min(req.leader_commit, match_index);
        }

        // 持久化日志
        {
            let persistent_state = state.persistent.read().await.clone();
            let log_store = log.clone();
            let data = PersistentData::from_state_and_log(&persistent_state, &log_store);
            if let Err(e) = self.storage.save(&data).await {
                tracing::error!("Failed to persist log: {}", e);
            }
        }

        drop(log);
        drop(state);

        Ok(Response::new(AppendEntriesResponse {
            term: req.term,
            success: true,
            match_index,
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let req = request.into_inner();

        // 重置选举定时器
        let mut timer = self.election_timer.write().await;
        timer.reset();
        drop(timer);

        let mut state = self.state.write().await;
        let persistent = state.persistent.read().await;
        let current_term = persistent.current_term;
        drop(persistent);

        tracing::info!(
            "Node {} received InstallSnapshot from {} (term {}, last_included: {})",
            state.node_id,
            req.leader_id,
            req.term,
            req.last_included_index
        );

        // 如果 term < currentTerm，拒绝
        if req.term < current_term {
            return Ok(Response::new(InstallSnapshotResponse {
                term: current_term,
            }));
        }

        // 如果 term > currentTerm，更新任期并转换为 Follower
        if req.term > current_term {
            state.become_follower(req.term, Some(req.leader_id)).await;
        } else if state.role == crate::raft::NodeRole::Candidate {
            state.become_follower(req.term, Some(req.leader_id)).await;
        }

        // 更新已知 Leader
        let mut leader_id = state.leader_id.write().await;
        *leader_id = Some(req.leader_id);
        drop(leader_id);

        // 应用快照
        let mut log = self.log.write().await;

        // 检查快照是否比当前状态更新
        if req.last_included_index > log.snapshot_last_index {
            // 恢复状态机
            if let Err(e) = self.kv_store.restore(&req.data).await {
                tracing::error!("Failed to restore snapshot: {}", e);
                return Ok(Response::new(InstallSnapshotResponse {
                    term: current_term,
                }));
            }

            // 更新日志快照边界
            log.apply_snapshot(req.last_included_index, req.last_included_term);

            // 更新 last_applied 和 commit_index
            let mut volatile = state.volatile.write().await;
            volatile.last_applied = req.last_included_index;
            volatile.commit_index = std::cmp::max(volatile.commit_index, req.last_included_index);
            drop(volatile);

            // 持久化
            let persistent_state = state.persistent.read().await.clone();
            let data = PersistentData::from_state_and_log(&persistent_state, &log)
                .with_snapshot(req.data.clone());
            if let Err(e) = self.storage.save(&data).await {
                tracing::error!("Failed to persist snapshot: {}", e);
            }

            tracing::info!(
                "Node {} applied snapshot: last_included_index={}, log_entries={}",
                state.node_id,
                req.last_included_index,
                log.entries.len()
            );
        }

        drop(log);
        drop(state);

        Ok(Response::new(InstallSnapshotResponse {
            term: req.term,
        }))
    }

    async fn transfer_leader(
        &self,
        request: Request<TransferLeaderRequest>,
    ) -> Result<Response<TransferLeaderResponse>, Status> {
        let req = request.into_inner();

        tracing::info!(
            "Received TransferLeader request: target={}",
            req.target_id
        );

        let response = self.election.handle_transfer_leader(req).await;
        Ok(Response::new(response))
    }

    async fn timeout_now(
        &self,
        request: Request<TimeoutNowRequest>,
    ) -> Result<Response<TimeoutNowResponse>, Status> {
        let req = request.into_inner();

        // 重置选举定时器
        let mut timer = self.election_timer.write().await;
        timer.reset();
        drop(timer);

        let response = self.election.handle_timeout_now(req).await;
        Ok(Response::new(response))
    }
}