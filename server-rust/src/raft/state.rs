use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// 节点角色
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::Follower => write!(f, "Follower"),
            NodeRole::Candidate => write!(f, "Candidate"),
            NodeRole::Leader => write!(f, "Leader"),
        }
    }
}

/// 持久化状态（必须在使用前保存）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    /// 当前任期
    pub current_term: u64,
    /// 在当前任期投票给谁 (None = 未投票)
    pub voted_for: Option<u32>,
}

/// 易失性状态（所有节点）
#[derive(Debug, Clone)]
pub struct VolatileState {
    /// 已知的最高已提交日志索引
    pub commit_index: u64,
    /// 最后应用到状态机的日志索引
    pub last_applied: u64,
}

/// Leader 易失性状态（选举后重新初始化）
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// 每个节点的 nextIndex（下一个要发送的日志索引）
    pub next_index: Vec<u64>,
    /// 每个节点的 matchIndex（已知复制的最高日志索引）
    pub match_index: Vec<u64>,
}

/// 完整的 Raft 状态
#[derive(Debug)]
pub struct RaftState {
    /// 本节点 ID
    pub node_id: u32,
    /// 当前角色
    pub role: NodeRole,
    /// 持久化状态
    pub persistent: Arc<RwLock<PersistentState>>,
    /// 易失性状态
    pub volatile: Arc<RwLock<VolatileState>>,
    /// Leader 状态（仅 Leader 有效）
    pub leader_state: Arc<RwLock<Option<LeaderState>>>,
    /// 当前已知 Leader ID
    pub leader_id: Arc<RwLock<Option<u32>>>,
    /// 集群节点列表
    pub peers: Vec<u32>,
}

impl RaftState {
    pub fn new(node_id: u32, peers: Vec<u32>) -> Self {
        Self {
            node_id,
            role: NodeRole::Follower,
            persistent: Arc::new(RwLock::new(PersistentState {
                current_term: 0,
                voted_for: None,
            })),
            volatile: Arc::new(RwLock::new(VolatileState {
                commit_index: 0,
                last_applied: 0,
            })),
            leader_state: Arc::new(RwLock::new(None)),
            leader_id: Arc::new(RwLock::new(None)),
            peers,
        }
    }

    /// 从持久化数据恢复状态
    pub fn from_persistent(
        node_id: u32,
        peers: Vec<u32>,
        current_term: u64,
        voted_for: Option<u32>,
    ) -> Self {
        Self {
            node_id,
            role: NodeRole::Follower,
            persistent: Arc::new(RwLock::new(PersistentState {
                current_term,
                voted_for,
            })),
            volatile: Arc::new(RwLock::new(VolatileState {
                commit_index: 0,
                last_applied: 0,
            })),
            leader_state: Arc::new(RwLock::new(None)),
            leader_id: Arc::new(RwLock::new(None)),
            peers,
        }
    }

    /// 获取多数派节点数
    #[allow(dead_code)]
    pub fn majority(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    /// 转换为 Follower
    pub async fn become_follower(&mut self, term: u64, leader_id: Option<u32>) {
        let mut persistent = self.persistent.write().await;
        if term > persistent.current_term {
            persistent.current_term = term;
            persistent.voted_for = None;
        }
        drop(persistent);

        self.role = NodeRole::Follower;
        let mut leader = self.leader_id.write().await;
        *leader = leader_id;

        tracing::info!("Node {} became Follower for term {}", self.node_id, term);
    }

    /// 转换为 Candidate
    pub async fn become_candidate(&mut self) {
        let mut persistent = self.persistent.write().await;
        persistent.current_term += 1;
        persistent.voted_for = Some(self.node_id);

        self.role = NodeRole::Candidate;
        let mut leader = self.leader_id.write().await;
        *leader = None;

        tracing::info!(
            "Node {} became Candidate for term {}",
            self.node_id,
            persistent.current_term
        );
    }

    /// 转换为 Leader
    pub async fn become_leader(&mut self) {
        self.role = NodeRole::Leader;

        let persistent = self.persistent.read().await;
        let term = persistent.current_term;
        drop(persistent);

        // 初始化 Leader 状态
        let log_len = 1; // TODO: 从 LogStore 获取
        let mut leader_state = self.leader_state.write().await;
        *leader_state = Some(LeaderState {
            next_index: vec![log_len; self.peers.len() + 1],
            match_index: vec![0; self.peers.len() + 1],
        });
        drop(leader_state);

        let mut leader = self.leader_id.write().await;
        *leader = Some(self.node_id);
        drop(leader);

        tracing::info!("Node {} became Leader for term {}", self.node_id, term);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_raft_state() {
        let state = RaftState::new(1, vec![2, 3]);

        assert_eq!(state.node_id, 1);
        assert_eq!(state.role, NodeRole::Follower);
        assert_eq!(state.peers, vec![2, 3]);

        let persistent = state.persistent.read().await;
        assert_eq!(persistent.current_term, 0);
        assert!(persistent.voted_for.is_none());
    }

    #[tokio::test]
    async fn test_become_candidate() {
        let mut state = RaftState::new(1, vec![2, 3]);

        state.become_candidate().await;

        assert_eq!(state.role, NodeRole::Candidate);

        let persistent = state.persistent.read().await;
        assert_eq!(persistent.current_term, 1);
        assert_eq!(persistent.voted_for, Some(1)); // 投给自己

        let leader_id = state.leader_id.read().await;
        assert!(leader_id.is_none());
    }

    #[tokio::test]
    async fn test_become_leader() {
        let mut state = RaftState::new(1, vec![2, 3]);

        // 先成为 Candidate
        state.become_candidate().await;
        state.become_leader().await;

        assert_eq!(state.role, NodeRole::Leader);

        let leader_id = state.leader_id.read().await;
        assert_eq!(*leader_id, Some(1));

        let leader_state = state.leader_state.read().await;
        assert!(leader_state.is_some());
    }

    #[tokio::test]
    async fn test_become_follower_higher_term() {
        let mut state = RaftState::new(1, vec![2, 3]);

        // 收到更高任期的心跳
        state.become_follower(5, Some(2)).await;

        assert_eq!(state.role, NodeRole::Follower);

        let persistent = state.persistent.read().await;
        assert_eq!(persistent.current_term, 5);

        let leader_id = state.leader_id.read().await;
        assert_eq!(*leader_id, Some(2));
    }

    #[tokio::test]
    async fn test_become_follower_same_term() {
        let mut state = RaftState::new(1, vec![2, 3]);

        // 先成为 Candidate
        state.become_candidate().await;
        let term_before = state.persistent.read().await.current_term;

        // 收到同一任期的心跳（其他节点成为 Leader）
        state.become_follower(term_before, Some(2)).await;

        assert_eq!(state.role, NodeRole::Follower);
    }

    #[tokio::test]
    async fn test_majority() {
        // 3 节点集群，多数派 = 2
        let state = RaftState::new(1, vec![2, 3]);
        assert_eq!(state.majority(), 2);

        // 5 节点集群，多数派 = 3
        let state = RaftState::new(1, vec![2, 3, 4, 5]);
        assert_eq!(state.majority(), 3);

        // 单节点，多数派 = 1
        let state = RaftState::new(1, vec![]);
        assert_eq!(state.majority(), 1);
    }

    #[test]
    fn test_node_role_display() {
        assert_eq!(NodeRole::Follower.to_string(), "Follower");
        assert_eq!(NodeRole::Candidate.to_string(), "Candidate");
        assert_eq!(NodeRole::Leader.to_string(), "Leader");
    }
}
