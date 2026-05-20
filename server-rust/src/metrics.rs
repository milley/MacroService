use std::sync::OnceLock;
use prometheus::{
    register_counter, register_int_gauge, Counter, IntGauge,
};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::raft::RaftState;

/// 全局 Metrics 注册表
static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Raft Metrics 指标集合
#[allow(dead_code)]
pub struct Metrics {
    /// 当前任期
    pub current_term: IntGauge,
    /// 节点角色 (0=Follower, 1=Candidate, 2=Leader)
    pub node_role: IntGauge,
    /// 已提交的日志索引
    pub commit_index: IntGauge,
    /// 已应用的日志索引
    pub last_applied: IntGauge,
    /// 最后日志索引
    pub last_log_index: IntGauge,
    /// 日志条目数量
    pub log_entries_count: IntGauge,
    /// 快照最后索引
    pub snapshot_last_index: IntGauge,

    /// 选举次数
    pub elections_started: Counter,
    /// 选举获胜次数
    pub elections_won: Counter,

    /// 心跳发送次数
    pub heartbeats_sent: Counter,
    /// 心跳成功次数
    pub heartbeats_success: Counter,

    /// 快照创建次数
    pub snapshots_created: Counter,

    /// 写请求次数
    pub write_requests: Counter,
    /// 读请求次数
    pub read_requests: Counter,

    /// Leader 租约是否有效
    pub lease_valid: IntGauge,
    /// 集群节点数
    pub cluster_size: IntGauge,
}

impl Metrics {
    fn new() -> Self {
        // Raft 状态指标
        let current_term = register_int_gauge!(
            "raft_current_term",
            "Current term of the Raft node"
        ).unwrap();

        let node_role = register_int_gauge!(
            "raft_node_role",
            "Role of the Raft node (0=Follower, 1=Candidate, 2=Leader)"
        ).unwrap();

        let commit_index = register_int_gauge!(
            "raft_commit_index",
            "Index of the highest log entry known to be committed"
        ).unwrap();

        let last_applied = register_int_gauge!(
            "raft_last_applied",
            "Index of the highest log entry applied to the state machine"
        ).unwrap();

        let last_log_index = register_int_gauge!(
            "raft_last_log_index",
            "Index of the last log entry"
        ).unwrap();

        let log_entries_count = register_int_gauge!(
            "raft_log_entries_count",
            "Number of log entries in memory"
        ).unwrap();

        let snapshot_last_index = register_int_gauge!(
            "raft_snapshot_last_index",
            "Last index included in the snapshot"
        ).unwrap();

        // 选举指标
        let elections_started = register_counter!(
            "raft_elections_started_total",
            "Total number of elections started"
        ).unwrap();

        let elections_won = register_counter!(
            "raft_elections_won_total",
            "Total number of elections won"
        ).unwrap();

        // 心跳指标
        let heartbeats_sent = register_counter!(
            "raft_heartbeats_sent_total",
            "Total number of heartbeats sent"
        ).unwrap();

        let heartbeats_success = register_counter!(
            "raft_heartbeats_success_total",
            "Total number of successful heartbeats"
        ).unwrap();

        // 快照指标
        let snapshots_created = register_counter!(
            "raft_snapshots_created_total",
            "Total number of snapshots created"
        ).unwrap();

        // 请求指标
        let write_requests = register_counter!(
            "raft_write_requests_total",
            "Total number of write requests (Put/Delete)"
        ).unwrap();

        let read_requests = register_counter!(
            "raft_read_requests_total",
            "Total number of read requests (Get)"
        ).unwrap();

        // 其他指标
        let lease_valid = register_int_gauge!(
            "raft_lease_valid",
            "Whether the leader lease is valid (1=valid, 0=invalid)"
        ).unwrap();

        let cluster_size = register_int_gauge!(
            "raft_cluster_size",
            "Number of nodes in the cluster"
        ).unwrap();

        Self {
            current_term,
            node_role,
            commit_index,
            last_applied,
            last_log_index,
            log_entries_count,
            snapshot_last_index,
            elections_started,
            elections_won,
            heartbeats_sent,
            heartbeats_success,
            snapshots_created,
            write_requests,
            read_requests,
            lease_valid,
            cluster_size,
        }
    }

    /// 获取全局 Metrics 实例
    pub fn global() -> &'static Metrics {
        METRICS.get_or_init(Metrics::new)
    }

    /// 从 RaftState 更新指标
    pub async fn update_from_state(&self, state: &Arc<RwLock<RaftState>>) {
        let state_guard = state.read().await;

        // 更新任期
        let term = state_guard.persistent.read().await.current_term;
        self.current_term.set(term as i64);

        // 更新角色
        let role_value = match state_guard.role {
            crate::raft::NodeRole::Follower => 0,
            crate::raft::NodeRole::Candidate => 1,
            crate::raft::NodeRole::Leader => 2,
        };
        self.node_role.set(role_value);

        // 更新索引
        let volatile = state_guard.volatile.read().await;
        self.commit_index.set(volatile.commit_index as i64);
        self.last_applied.set(volatile.last_applied as i64);
        drop(volatile);

        // 更新集群大小
        self.cluster_size.set((state_guard.peers.len() + 1) as i64);

        // 更新租约状态
        if state_guard.role == crate::raft::NodeRole::Leader {
            let leader_state = state_guard.leader_state.read().await;
            if let Some(ls) = leader_state.as_ref() {
                self.lease_valid.set(if ls.lease.is_valid() { 1 } else { 0 });
            } else {
                self.lease_valid.set(0);
            }
        } else {
            self.lease_valid.set(0);
        }
    }

    /// 输出 Prometheus 格式的指标
    pub fn export(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}

/// 启动 metrics HTTP 服务器
pub async fn start_metrics_server(addr: std::net::SocketAddr, state: Arc<RwLock<RaftState>>) {
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use hyper_util::rt::tokio::TokioIo;
    use tokio::net::TcpListener;

    let metrics = Metrics::global();

    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind metrics server to {}: {}", addr, e);
            return;
        }
    };

    tracing::info!("Metrics server listening on http://{}", addr);

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::debug!("Failed to accept connection: {}", e);
                continue;
            }
        };

        let state = state.clone();
        let metrics = metrics as &'static Metrics;

        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                let state = state.clone();
                let metrics = metrics;
                async move {
                    match req.uri().path() {
                        "/metrics" => {
                            // 更新指标
                            metrics.update_from_state(&state).await;

                            // 返回 Prometheus 格式的指标
                            let output = metrics.export();
                            Ok::<_, hyper::Error>(Response::builder()
                                .header("Content-Type", "text/plain; version=0.0.4")
                                .body(Full::new(Bytes::from(output)))
                                .unwrap())
                        }
                        "/health" | "/healthz" => {
                            // 存活检查：服务能响应即健康
                            let state_guard = state.read().await;
                            let role = match state_guard.role {
                                crate::raft::NodeRole::Follower => "follower",
                                crate::raft::NodeRole::Candidate => "candidate",
                                crate::raft::NodeRole::Leader => "leader",
                            };
                            let term = state_guard.persistent.read().await.current_term;
                            let node_id = state_guard.node_id;
                            drop(state_guard);

                            let health_json = serde_json::json!({
                                "status": "healthy",
                                "node_id": node_id,
                                "role": role,
                                "term": term
                            });

                            Ok(Response::builder()
                                .header("Content-Type", "application/json")
                                .body(Full::new(Bytes::from(
                                    serde_json::to_string(&health_json).unwrap()
                                )))
                                .unwrap())
                        }
                        "/ready" | "/readyz" => {
                            // 就绪检查：节点已加入集群
                            let state_guard = state.read().await;
                            let is_ready = state_guard.peers.len() > 0 || state_guard.role == crate::raft::NodeRole::Leader;
                            let role = match state_guard.role {
                                crate::raft::NodeRole::Follower => "follower",
                                crate::raft::NodeRole::Candidate => "candidate",
                                crate::raft::NodeRole::Leader => "leader",
                            };
                            let leader_id = *state_guard.leader_id.read().await;
                            let node_id = state_guard.node_id;
                            drop(state_guard);

                            if is_ready {
                                let ready_json = serde_json::json!({
                                    "status": "ready",
                                    "node_id": node_id,
                                    "role": role,
                                    "leader_id": leader_id
                                });
                                Ok(Response::builder()
                                    .header("Content-Type", "application/json")
                                    .body(Full::new(Bytes::from(
                                        serde_json::to_string(&ready_json).unwrap()
                                    )))
                                    .unwrap())
                            } else {
                                let not_ready_json = serde_json::json!({
                                    "status": "not_ready",
                                    "node_id": node_id,
                                    "reason": "no peers configured"
                                });
                                Ok(Response::builder()
                                    .status(503)
                                    .header("Content-Type", "application/json")
                                    .body(Full::new(Bytes::from(
                                        serde_json::to_string(&not_ready_json).unwrap()
                                    )))
                                    .unwrap())
                            }
                        }
                        "/" => {
                            Ok(Response::builder()
                                .header("Content-Type", "text/html")
                                .body(Full::new(Bytes::from(
                                    "<html><body>\
                                    <h1>Raft KV Store</h1>\
                                    <h2>Endpoints</h2>\
                                    <ul>\
                                    <li><a href=\"/metrics\">/metrics</a> - Prometheus metrics</li>\
                                    <li><a href=\"/health\">/health</a> - Health check (liveness)</li>\
                                    <li><a href=\"/ready\">/ready</a> - Readiness check</li>\
                                    </ul>\
                                    </body></html>"
                                )))
                                .unwrap())
                        }
                        _ => {
                            Ok(Response::builder()
                                .status(404)
                                .body(Full::new(Bytes::from("Not Found")))
                                .unwrap())
                        }
                    }
                }
            });

            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                tracing::debug!("Connection error: {}", e);
            }
        });
    }
}
