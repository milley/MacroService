use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 内存 KV 存储 - Raft 状态机
#[derive(Debug)]
pub struct KVStore {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl KVStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let data = self.data.read().await;
        data.get(key).cloned()
    }

    pub async fn put(&self, key: String, value: Vec<u8>) {
        let mut data = self.data.write().await;
        data.insert(key, value);
    }

    pub async fn delete(&self, key: &str) -> bool {
        let mut data = self.data.write().await;
        data.remove(key).is_some()
    }

    pub async fn apply_command(&self, command: &[u8]) -> Result<(), String> {
        let cmd: KVCommand = serde_json::from_slice(command)
            .map_err(|e| format!("Failed to deserialize command: {}", e))?;

        match cmd {
            KVCommand::Put { key, value } => {
                self.put(key, value).await;
            }
            KVCommand::Delete { key } => {
                self.delete(&key).await;
            }
        }

        Ok(())
    }
}

impl Default for KVStore {
    fn default() -> Self {
        Self::new()
    }
}

/// KV 命令 - 序列化后存入 Raft 日志
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum KVCommand {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

impl KVCommand {
    pub fn put(key: String, value: Vec<u8>) -> Self {
        KVCommand::Put { key, value }
    }

    pub fn delete(key: String) -> Self {
        KVCommand::Delete { key }
    }

    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_and_get() {
        let store = KVStore::new();

        // Put
        store.put("key1".to_string(), vec![1, 2, 3]).await;

        // Get existing
        let result = store.get("key1").await;
        assert!(result.is_some());
        assert_eq!(result.unwrap(), vec![1, 2, 3]);

        // Get non-existing
        let result = store.get("key2").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete() {
        let store = KVStore::new();

        store.put("key1".to_string(), vec![1, 2, 3]).await;

        // Delete existing
        let deleted = store.delete("key1").await;
        assert!(deleted);

        // Verify deleted
        let result = store.get("key1").await;
        assert!(result.is_none());

        // Delete non-existing
        let deleted = store.delete("key2").await;
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_apply_command_put() {
        let store = KVStore::new();

        let cmd = KVCommand::put("test_key".to_string(), vec![10, 20, 30]);
        let serialized = cmd.serialize();

        store.apply_command(&serialized).await.unwrap();

        let result = store.get("test_key").await;
        assert!(result.is_some());
        assert_eq!(result.unwrap(), vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn test_apply_command_delete() {
        let store = KVStore::new();

        // 先插入
        store.put("test_key".to_string(), vec![1]).await;

        // 通过 apply_command 删除
        let cmd = KVCommand::delete("test_key".to_string());
        let serialized = cmd.serialize();

        store.apply_command(&serialized).await.unwrap();

        let result = store.get("test_key").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_overwrite() {
        let store = KVStore::new();

        store.put("key".to_string(), vec![1]).await;
        store.put("key".to_string(), vec![2, 3]).await;

        let result = store.get("key").await;
        assert_eq!(result.unwrap(), vec![2, 3]);
    }
}
