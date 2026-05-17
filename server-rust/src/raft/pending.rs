use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

/// 管理所有等待 commit 的请求
pub struct PendingRequests {
    /// log_index -> Notify
    requests: Mutex<HashMap<u64, Arc<Notify>>>,
}

impl PendingRequests {
    pub fn new() -> Self {
        Self {
            requests: Mutex::new(HashMap::new()),
        }
    }

    /// 注册一个等待请求
    pub async fn register(&self, log_index: u64) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        let mut requests = self.requests.lock().await;
        requests.insert(log_index, notify.clone());
        notify
    }

    /// 通知所有已 commit 的请求
    pub async fn notify_committed(&self, commit_index: u64) {
        let mut requests = self.requests.lock().await;
        let to_notify: Vec<_> = requests
            .keys()
            .filter(|&&idx| idx <= commit_index)
            .copied()
            .collect();

        for idx in to_notify {
            if let Some(notify) = requests.remove(&idx) {
                notify.notify_one();
            }
        }
    }
}

impl Default for PendingRequests {
    fn default() -> Self {
        Self::new()
    }
}
