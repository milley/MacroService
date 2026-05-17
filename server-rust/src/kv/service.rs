use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::kv::{KVCommand, KVStore};
use crate::proto::kv::kv_service_server::KvService;
use crate::proto::kv::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse,
};
use crate::raft::{LogStore, PendingRequests, PersistentData, PersistentStorage, RaftState};

/// KV gRPC 服务实现
pub struct KVServiceImpl {
    store: Arc<KVStore>,
    raft_state: Option<Arc<RwLock<RaftState>>>,
    log_store: Option<Arc<RwLock<LogStore>>>,
    pending_requests: Option<Arc<PendingRequests>>,
    storage: Option<Arc<PersistentStorage>>,
}

impl KVServiceImpl {
    pub fn new_with_raft(
        store: Arc<KVStore>,
        raft_state: Arc<RwLock<RaftState>>,
        log_store: Arc<RwLock<LogStore>>,
        pending_requests: Arc<PendingRequests>,
        storage: Arc<PersistentStorage>,
    ) -> Self {
        Self {
            store,
            raft_state: Some(raft_state),
            log_store: Some(log_store),
            pending_requests: Some(pending_requests),
            storage: Some(storage),
        }
    }

    /// 等待日志被 commit
    async fn wait_for_commit(&self, log_index: u64) -> bool {
        if let Some(pending) = &self.pending_requests {
            let notify = pending.register(log_index).await;

            // 设置超时（5秒）
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                notify.notified(),
            )
            .await
            {
                Ok(()) => true,
                Err(_) => {
                    tracing::warn!("Timeout waiting for commit at index {}", log_index);
                    false
                }
            }
        } else {
            true // 单节点模式，不需要等待
        }
    }

    /// 等待状态机应用到指定索引（ReadIndex 实现）
    async fn wait_for_apply(&self, target_index: u64) -> bool {
        if let Some(state) = &self.raft_state {
            // 如果目标索引为 0，无需等待
            if target_index == 0 {
                return true;
            }

            let start = std::time::Instant::now();
            let timeout = std::time::Duration::from_secs(5);

            loop {
                let last_applied = state.read().await.volatile.read().await.last_applied;
                if last_applied >= target_index {
                    return true;
                }

                if start.elapsed() > timeout {
                    tracing::warn!(
                        "Timeout waiting for apply: target={}, applied={}",
                        target_index,
                        last_applied
                    );
                    return false;
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
        }
        true
    }
}

#[tonic::async_trait]
impl KvService for KVServiceImpl {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let key = req.key;

        // 如果有 Raft，实现 ReadIndex 读取（支持租约优化）
        if let Some(state) = &self.raft_state {
            let state_guard = state.read().await;

            // 1. 检查是否是 Leader
            if state_guard.role != crate::raft::NodeRole::Leader {
                let leader_id = state_guard.leader_id.read().await;
                return Ok(Response::new(GetResponse {
                    found: false,
                    value: vec![],
                    error: "not leader".to_string(),
                    leader_hint: leader_id.unwrap_or(0),
                }));
            }

            // 2. 检查租约是否有效
            let leader_state_guard = state_guard.leader_state.read().await;
            if let Some(leader_state) = leader_state_guard.as_ref() {
                if leader_state.lease.is_valid() {
                    // 租约有效，直接读取（快速路径）
                    drop(leader_state_guard);
                    drop(state_guard);
                    return match self.store.get(&key).await {
                        Some(value) => Ok(Response::new(GetResponse {
                            found: true,
                            value,
                            error: String::new(),
                            leader_hint: 0,
                        })),
                        None => Ok(Response::new(GetResponse {
                            found: false,
                            value: vec![],
                            error: String::new(),
                            leader_hint: 0,
                        })),
                    };
                }
            }
            drop(leader_state_guard);

            // 3. 租约无效，回退到 ReadIndex
            let read_index = state_guard.volatile.read().await.commit_index;
            drop(state_guard);

            // 4. 等待状态机应用到 read_index
            if !self.wait_for_apply(read_index).await {
                return Ok(Response::new(GetResponse {
                    found: false,
                    value: vec![],
                    error: "timeout waiting for apply".to_string(),
                    leader_hint: 0,
                }));
            }
        }

        // 5. 安全读取
        match self.store.get(&key).await {
            Some(value) => Ok(Response::new(GetResponse {
                found: true,
                value,
                error: String::new(),
                leader_hint: 0,
            })),
            None => Ok(Response::new(GetResponse {
                found: false,
                value: vec![],
                error: String::new(),
                leader_hint: 0,
            })),
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();

        // 如果有 Raft，检查是否是 Leader
        if let (Some(state), Some(log)) = (&self.raft_state, &self.log_store) {
            let state_guard = state.read().await;

            if state_guard.role != crate::raft::NodeRole::Leader {
                let leader_id = state_guard.leader_id.read().await;
                return Ok(Response::new(PutResponse {
                    success: false,
                    error: "not leader".to_string(),
                    leader_hint: leader_id.unwrap_or(0),
                }));
            }

            // 创建日志条目
            let cmd = KVCommand::put(req.key.clone(), req.value.clone());
            let serialized = cmd.serialize();

            let persistent = state_guard.persistent.read().await;
            let term = persistent.current_term;
            drop(persistent);

            let mut log_guard = log.write().await;
            let index = log_guard.last_index() + 1;
            let entry = crate::raft::LogEntry::new(term, index, serialized);
            log_guard.append_one(entry);

            // Leader 持久化日志
            if let Some(storage) = &self.storage {
                let state_guard = state.read().await;
                let persistent_state = state_guard.persistent.read().await.clone();
                let data = PersistentData::from_state_and_log(&persistent_state, &log_guard);
                drop(state_guard);
                if let Err(e) = storage.save(&data).await {
                    tracing::error!("Failed to persist log: {}", e);
                }
            }

            drop(log_guard);
            drop(state_guard);

            tracing::info!(
                "Leader appended Put({}) to log at index {}",
                req.key,
                index
            );

            // 等待 commit
            let committed = self.wait_for_commit(index).await;

            if committed {
                Ok(Response::new(PutResponse {
                    success: true,
                    error: String::new(),
                    leader_hint: 0,
                }))
            } else {
                Ok(Response::new(PutResponse {
                    success: false,
                    error: "timeout waiting for commit".to_string(),
                    leader_hint: 0,
                }))
            }
        } else {
            // 单节点模式：直接写入
            self.store.put(req.key.clone(), req.value).await;

            Ok(Response::new(PutResponse {
                success: true,
                error: String::new(),
                leader_hint: 0,
            }))
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();

        // 如果有 Raft，检查是否是 Leader
        if let (Some(state), Some(log)) = (&self.raft_state, &self.log_store) {
            let state_guard = state.read().await;

            if state_guard.role != crate::raft::NodeRole::Leader {
                let leader_id = state_guard.leader_id.read().await;
                return Ok(Response::new(DeleteResponse {
                    success: false,
                    error: "not leader".to_string(),
                    leader_hint: leader_id.unwrap_or(0),
                }));
            }

            // 创建日志条目
            let cmd = KVCommand::delete(req.key.clone());
            let serialized = cmd.serialize();

            let persistent = state_guard.persistent.read().await;
            let term = persistent.current_term;
            drop(persistent);

            let mut log_guard = log.write().await;
            let index = log_guard.last_index() + 1;
            let entry = crate::raft::LogEntry::new(term, index, serialized);
            log_guard.append_one(entry);

            // Leader 持久化日志
            if let Some(storage) = &self.storage {
                let state_guard = state.read().await;
                let persistent_state = state_guard.persistent.read().await.clone();
                let data = PersistentData::from_state_and_log(&persistent_state, &log_guard);
                drop(state_guard);
                if let Err(e) = storage.save(&data).await {
                    tracing::error!("Failed to persist log: {}", e);
                }
            }

            drop(log_guard);
            drop(state_guard);

            tracing::info!(
                "Leader appended Delete({}) to log at index {}",
                req.key,
                index
            );

            // 等待 commit
            let committed = self.wait_for_commit(index).await;

            if committed {
                Ok(Response::new(DeleteResponse {
                    success: true,
                    error: String::new(),
                    leader_hint: 0,
                }))
            } else {
                Ok(Response::new(DeleteResponse {
                    success: false,
                    error: "timeout waiting for commit".to_string(),
                    leader_hint: 0,
                }))
            }
        } else {
            // 单节点模式
            let deleted = self.store.delete(&req.key).await;

            Ok(Response::new(DeleteResponse {
                success: deleted,
                error: String::new(),
                leader_hint: 0,
            }))
        }
    }
}
