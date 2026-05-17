use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;
use tokio::fs;

use super::{LogEntry, LogStore, PersistentState, RaftState};

/// 持久化数据格式版本
const STORAGE_VERSION: u32 = 1;

/// 持久化错误类型
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// 持久化数据结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentData {
    /// 版本号，便于未来迁移
    pub version: u32,
    /// 当前任期
    pub current_term: u64,
    /// 投票给谁
    pub voted_for: Option<u32>,
    /// 日志条目
    pub log: Vec<LogEntry>,
}

impl PersistentData {
    /// 从 Raft 状态和日志创建持久化数据
    pub fn from_state_and_log(
        persistent_state: &PersistentState,
        log_store: &LogStore,
    ) -> Self {
        Self {
            version: STORAGE_VERSION,
            current_term: persistent_state.current_term,
            voted_for: persistent_state.voted_for,
            log: log_store.entries.clone(),
        }
    }

    /// 恢复 RaftState
    pub fn to_raft_state(&self, node_id: u32, peers: Vec<u32>) -> RaftState {
        RaftState::from_persistent(node_id, peers, self.current_term, self.voted_for)
    }

    /// 恢复 LogStore
    pub fn to_log_store(&self) -> LogStore {
        LogStore::from_entries(self.log.clone())
    }
}

/// 持久化存储
pub struct PersistentStorage {
    /// 存储文件路径
    path: PathBuf,
}

impl PersistentStorage {
    /// 创建持久化存储实例
    pub fn new(data_dir: &str, node_id: u32) -> Self {
        let mut path = PathBuf::from(data_dir);
        path.push(format!("node_{}", node_id));
        path.push("raft_state.json");
        Self { path }
    }

    /// 保存持久化数据（原子写入）
    pub async fn save(&self, data: &PersistentData) -> Result<(), StorageError> {
        // 确保目录存在
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // 先写入临时文件
        let temp_path = self.path.with_extension("json.tmp");
        let content = serde_json::to_string_pretty(data)?;
        fs::write(&temp_path, content).await?;

        // 原子重命名
        fs::rename(&temp_path, &self.path).await?;

        tracing::debug!(
            "Saved persistent data: term={}, voted_for={:?}, log_len={}",
            data.current_term,
            data.voted_for,
            data.log.len()
        );

        Ok(())
    }

    /// 加载持久化数据
    pub async fn load(&self) -> Result<Option<PersistentData>, StorageError> {
        if !self.path.exists() {
            tracing::info!("No persistent data found at {:?}", self.path);
            return Ok(None);
        }

        let content = fs::read_to_string(&self.path).await?;
        let data: PersistentData = serde_json::from_str(&content)?;

        tracing::info!(
            "Loaded persistent data: term={}, voted_for={:?}, log_len={}",
            data.current_term,
            data.voted_for,
            data.log.len()
        );

        Ok(Some(data))
    }

    /// 获取存储路径（用于调试）
    #[allow(dead_code)]
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_save_and_load() {
        let dir = tempdir().unwrap();
        let storage = PersistentStorage::new(dir.path().to_str().unwrap(), 1);

        let data = PersistentData {
            version: 1,
            current_term: 5,
            voted_for: Some(2),
            log: vec![
                LogEntry::new(1, 1, vec![1, 2, 3]),
                LogEntry::new(3, 2, vec![4, 5, 6]),
            ],
        };

        // 保存
        storage.save(&data).await.unwrap();

        // 加载
        let loaded = storage.load().await.unwrap().unwrap();
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.current_term, 5);
        assert_eq!(loaded.voted_for, Some(2));
        assert_eq!(loaded.log.len(), 2);
    }

    #[tokio::test]
    async fn test_load_nonexistent() {
        let dir = tempdir().unwrap();
        let storage = PersistentStorage::new(dir.path().to_str().unwrap(), 1);

        let loaded = storage.load().await.unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_from_state_and_log() {
        let persistent_state = PersistentState {
            current_term: 10,
            voted_for: Some(3),
        };
        let log_store = LogStore::from_entries(vec![
            LogEntry::new(1, 1, vec![1]),
            LogEntry::new(2, 2, vec![2]),
        ]);

        let data = PersistentData::from_state_and_log(&persistent_state, &log_store);
        assert_eq!(data.current_term, 10);
        assert_eq!(data.voted_for, Some(3));
        assert_eq!(data.log.len(), 2);
    }
}
