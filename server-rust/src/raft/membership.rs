use std::sync::Arc;
use tokio::sync::RwLock;

use crate::proto::raft::{
    AddNodeRequest, AddNodeResponse, RemoveNodeRequest, RemoveNodeResponse,
};
use crate::raft::{LogStore, NodeRole, RaftState};

/// 成员变更管理器
pub struct Membership {
    state: Arc<RwLock<RaftState>>,
    log: Arc<RwLock<LogStore>>,
    /// 是否正在进行成员变更
    changing: Arc<RwLock<bool>>,
}

impl Membership {
    pub fn new(state: Arc<RwLock<RaftState>>, log: Arc<RwLock<LogStore>>) -> Self {
        Self {
            state,
            log,
            changing: Arc::new(RwLock::new(false)),
        }
    }

    /// 检查是否可以进行成员变更
    #[allow(dead_code)]
    pub async fn can_change(&self) -> bool {
        let changing = self.changing.read().await;
        !*changing
    }

    /// 添加节点
    ///
    /// 1. 检查是否是 Leader
    /// 2. 检查是否正在进行其他成员变更
    /// 3. 检查节点是否已存在
    /// 4. 更新 peers 列表
    pub async fn add_node(&self, node_id: u32, node_addr: String) -> Result<(), String> {
        let state = self.state.read().await;

        // 只有 Leader 才能添加节点
        if state.role != NodeRole::Leader {
            return Err("not leader".to_string());
        }

        // 检查是否正在进行成员变更
        let changing = self.changing.read().await;
        if *changing {
            return Err("membership change in progress".to_string());
        }
        drop(changing);

        // 检查节点是否已存在
        if state.node_id == node_id {
            return Err("node already exists (self)".to_string());
        }

        if state.peers.contains(&node_id) {
            return Err(format!("node {} already in cluster", node_id));
        }

        let leader_id = state.node_id;
        drop(state);

        // 标记正在变更
        {
            let mut changing = self.changing.write().await;
            *changing = true;
        }

        // 更新 peers 列表
        {
            let mut state = self.state.write().await;
            state.peers.push(node_id);

            // 更新 Leader 状态（增加新节点的 next_index 和 match_index）
            let mut leader_state = state.leader_state.write().await;
            if let Some(ls) = leader_state.as_mut() {
                // 新节点从 log.last_index + 1 开始
                let log = self.log.read().await;
                let next_idx = log.last_index() + 1;
                drop(log);

                ls.next_index.push(next_idx);
                ls.match_index.push(0);
            }

            tracing::info!(
                "Leader {} added node {} ({}), total peers: {}",
                leader_id,
                node_id,
                node_addr,
                state.peers.len()
            );
        }

        // 变更完成
        {
            let mut changing = self.changing.write().await;
            *changing = false;
        }

        Ok(())
    }

    /// 移除节点
    ///
    /// 1. 检查是否是 Leader
    /// 2. 检查是否正在进行其他成员变更
    /// 3. 检查节点是否存在
    /// 4. 检查移除后是否还能形成多数派
    /// 5. 更新 peers 列表
    pub async fn remove_node(&self, node_id: u32) -> Result<(), String> {
        let state = self.state.read().await;

        // 只有 Leader 才能移除节点
        if state.role != NodeRole::Leader {
            return Err("not leader".to_string());
        }

        // 检查是否正在进行成员变更
        let changing = self.changing.read().await;
        if *changing {
            return Err("membership change in progress".to_string());
        }
        drop(changing);

        // 不能移除自己
        if state.node_id == node_id {
            return Err("cannot remove self, use transfer_leader first".to_string());
        }

        // 检查节点是否存在
        let peer_idx = state.peers.iter().position(|&id| id == node_id);
        let peer_idx = match peer_idx {
            Some(idx) => idx,
            None => return Err(format!("node {} not in cluster", node_id)),
        };

        // 检查移除后是否还能形成多数派
        let new_cluster_size = state.peers.len(); // 移除一个后
        if new_cluster_size == 0 {
            return Err("cannot remove last peer".to_string());
        }

        let leader_id = state.node_id;
        drop(state);

        // 标记正在变更
        {
            let mut changing = self.changing.write().await;
            *changing = true;
        }

        // 更新 peers 列表
        {
            let mut state = self.state.write().await;
            state.peers.remove(peer_idx);

            // 更新 Leader 状态（移除对应节点的 next_index 和 match_index）
            let mut leader_state = state.leader_state.write().await;
            if let Some(ls) = leader_state.as_mut() {
                // peer_idx + 1 是因为 next_index/match_index[0] 是 Leader 自己
                ls.next_index.remove(peer_idx + 1);
                ls.match_index.remove(peer_idx + 1);
            }

            tracing::info!(
                "Leader {} removed node {}, remaining peers: {}",
                leader_id,
                node_id,
                state.peers.len()
            );
        }

        // 变更完成
        {
            let mut changing = self.changing.write().await;
            *changing = false;
        }

        Ok(())
    }

    /// 处理 AddNode RPC
    pub async fn handle_add_node(&self, request: AddNodeRequest) -> AddNodeResponse {
        let state = self.state.read().await;
        let current_term = state.persistent.read().await.current_term;
        drop(state);

        // 检查任期
        if request.term < current_term {
            return AddNodeResponse {
                term: current_term,
                success: false,
            };
        }

        match self.add_node(request.node_id, request.node_addr).await {
            Ok(()) => AddNodeResponse {
                term: current_term,
                success: true,
            },
            Err(e) => {
                tracing::error!("AddNode failed: {}", e);
                AddNodeResponse {
                    term: current_term,
                    success: false,
                }
            }
        }
    }

    /// 处理 RemoveNode RPC
    pub async fn handle_remove_node(&self, request: RemoveNodeRequest) -> RemoveNodeResponse {
        let state = self.state.read().await;
        let current_term = state.persistent.read().await.current_term;
        drop(state);

        // 检查任期
        if request.term < current_term {
            return RemoveNodeResponse {
                term: current_term,
                success: false,
            };
        }

        match self.remove_node(request.node_id).await {
            Ok(()) => RemoveNodeResponse {
                term: current_term,
                success: true,
            },
            Err(e) => {
                tracing::error!("RemoveNode failed: {}", e);
                RemoveNodeResponse {
                    term: current_term,
                    success: false,
                }
            }
        }
    }

    /// 应用配置变更（从日志恢复时调用）
    #[allow(dead_code)]
    pub async fn apply_config_change(
        &self,
        change_type: crate::proto::raft::config_change::ChangeType,
        node_id: u32,
        node_addr: Option<String>,
    ) -> Result<(), String> {
        match change_type {
            crate::proto::raft::config_change::ChangeType::AddNode => {
                let addr = node_addr.unwrap_or_default();
                self.add_node(node_id, addr).await
            }
            crate::proto::raft::config_change::ChangeType::RemoveNode => {
                self.remove_node(node_id).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_state() -> (Arc<RwLock<RaftState>>, Arc<RwLock<LogStore>>) {
        (
            Arc::new(RwLock::new(RaftState::new(1, vec![2, 3]))),
            Arc::new(RwLock::new(LogStore::new())),
        )
    }

    #[tokio::test]
    async fn test_add_node_not_leader() {
        let (state, log) = create_test_state();
        let membership = Membership::new(state, log);

        // Follower 尝试添加节点（应该失败）
        let result = membership.add_node(4, "127.0.0.1:60054".to_string()).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "not leader");
    }

    #[tokio::test]
    async fn test_add_node_already_exists() {
        let (state, log) = create_test_state();

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

        let membership = Membership::new(state, log);

        // 尝试添加已存在的节点
        let result = membership.add_node(2, "127.0.0.1:60052".to_string()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already"));
    }

    #[tokio::test]
    async fn test_add_node_success() {
        let (state, log) = create_test_state();

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

        let membership = Membership::new(state.clone(), log);

        // 添加新节点
        let result = membership.add_node(4, "127.0.0.1:60054".to_string()).await;
        assert!(result.is_ok());

        // 验证节点已添加
        let state_guard = state.read().await;
        assert!(state_guard.peers.contains(&4));
        assert_eq!(state_guard.peers.len(), 3);
    }

    #[tokio::test]
    async fn test_remove_node_not_leader() {
        let (state, log) = create_test_state();
        let membership = Membership::new(state, log);

        // Follower 尝试移除节点（应该失败）
        let result = membership.remove_node(2).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "not leader");
    }

    #[tokio::test]
    async fn test_remove_node_self() {
        let (state, log) = create_test_state();

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

        let membership = Membership::new(state, log);

        // 尝试移除自己（应该失败）
        let result = membership.remove_node(1).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("self"));
    }

    #[tokio::test]
    async fn test_remove_node_success() {
        let (state, log) = create_test_state();

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

        let membership = Membership::new(state.clone(), log);

        // 移除节点
        let result = membership.remove_node(2).await;
        assert!(result.is_ok());

        // 验证节点已移除
        let state_guard = state.read().await;
        assert!(!state_guard.peers.contains(&2));
        assert_eq!(state_guard.peers.len(), 1);
    }
}
