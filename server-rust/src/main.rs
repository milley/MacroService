use clap::Parser;
use std::sync::Arc;
use tonic::transport::Server;
use tokio::sync::RwLock;

mod config;
mod kv;
mod proto;
mod raft;

use config::{CliConfig, NodeConfig};
use kv::{KVServiceImpl, KVStore};
use proto::calculator::calculator_server::CalculatorServer;
use proto::kv::kv_service_server::KvServiceServer;
use proto::raft::raft_service_server::RaftServiceServer;
use raft::{Election, ElectionTimer, HeartbeatTimer, LogStore, PendingRequests, PersistentStorage, RaftState, Replication};

/// Calculator 服务实现（保留原有 demo）
mod calculator_service {
    use tonic::{Request, Response, Status};
    use tokio_stream::{wrappers::ReceiverStream, StreamExt};

    use crate::proto::calculator::{
        calculator_server::Calculator, AddRequest, AddResponse, Number, PrimeResponse,
        PrimesRequest, SumResponse,
    };

    #[derive(Debug, Default)]
    pub struct MyCalculator {}

    fn is_prime(n: i32) -> bool {
        if n < 2 {
            return false;
        }
        for i in 2..=((n as f64).sqrt() as i32) {
            if n % i == 0 {
                return false;
            }
        }
        true
    }

    #[tonic::async_trait]
    impl Calculator for MyCalculator {
        async fn add(&self, request: Request<AddRequest>) -> Result<Response<AddResponse>, Status> {
            let req = request.into_inner();
            println!("[Add] {} + {}", req.a, req.b);
            Ok(Response::new(AddResponse {
                result: req.a + req.b,
            }))
        }

        type StreamPrimesStream = ReceiverStream<Result<PrimeResponse, Status>>;

        async fn stream_primes(
            &self,
            request: Request<PrimesRequest>,
        ) -> Result<Response<Self::StreamPrimesStream>, Status> {
            let limit = request.into_inner().limit;
            println!("[StreamPrimes] Generating primes < {}", limit);

            let (tx, rx) = tokio::sync::mpsc::channel(128);

            tokio::spawn(async move {
                for n in 2..limit {
                    if is_prime(n) {
                        tx.send(Ok(PrimeResponse { prime: n })).await.unwrap();
                    }
                }
            });

            Ok(Response::new(ReceiverStream::new(rx)))
        }

        async fn sum_stream(
            &self,
            request: Request<tonic::Streaming<Number>>,
        ) -> Result<Response<SumResponse>, Status> {
            let mut stream = request.into_inner();
            let mut sum = 0i32;
            let mut count = 0i32;

            println!("[SumStream] Receiving numbers...");

            while let Some(number) = stream.next().await {
                let n = number?.value;
                sum += n;
                count += 1;
                println!("  received: {}, running sum: {}", n, sum);
            }

            println!("[SumStream] Done. Total: {} from {} numbers", sum, count);

            Ok(Response::new(SumResponse { sum, count }))
        }
    }
}

use calculator_service::MyCalculator;
use raft::RaftServiceImpl;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    // 解析命令行参数
    let cli = CliConfig::parse();
    let config: NodeConfig = cli.into();

    println!("Starting Raft KV Node {}", config.node_id);
    println!("  Client API: {}", config.client_addr);
    println!("  Raft RPC:   {}", config.raft_addr);
    println!("  Peers:      {} nodes", config.peers.len());
    println!("  Data Dir:   {}", config.data_dir);

    // 创建持久化存储
    let storage = Arc::new(PersistentStorage::new(&config.data_dir, config.node_id));

    // 初始化 Raft 状态（尝试从持久化数据恢复）
    let peers: Vec<u32> = config.peers.iter().map(|p| p.id).collect();
    let (raft_state, log_store) = match storage.load().await {
        Ok(Some(data)) => {
            tracing::info!(
                "Recovered from persistent storage: term={}, log_len={}",
                data.current_term,
                data.log.len()
            );
            (
                Arc::new(RwLock::new(data.to_raft_state(config.node_id, peers.clone()))),
                Arc::new(RwLock::new(data.to_log_store())),
            )
        }
        Ok(None) => {
            tracing::info!("Starting with clean state");
            (
                Arc::new(RwLock::new(RaftState::new(config.node_id, peers.clone()))),
                Arc::new(RwLock::new(LogStore::new())),
            )
        }
        Err(e) => {
            tracing::warn!("Failed to load persistent data: {}, starting fresh", e);
            (
                Arc::new(RwLock::new(RaftState::new(config.node_id, peers.clone()))),
                Arc::new(RwLock::new(LogStore::new())),
            )
        }
    };

    let election_timer = Arc::new(RwLock::new(ElectionTimer::new()));
    let heartbeat_timer = Arc::new(RwLock::new(HeartbeatTimer::new(50)));

    // 创建等待请求管理器
    let pending_requests = Arc::new(PendingRequests::new());

    // 创建 KV 存储
    let kv_store = Arc::new(KVStore::new());

    // 创建选举管理器
    let election = Arc::new(Election::new(
        raft_state.clone(),
        log_store.clone(),
        &config,
        storage.clone(),
    ));

    // 创建复制管理器
    let replication = Arc::new(
        Replication::new(
            raft_state.clone(),
            log_store.clone(),
            &config,
        )
        .with_pending_requests(pending_requests.clone())
        .with_storage(storage.clone()),
    );

    // 启动 Raft 主循环
    let state_for_loop = raft_state.clone();
    let election_for_loop = election.clone();
    let replication_for_loop = replication.clone();
    let timer_for_election = election_timer.clone();
    let heartbeat_timer_for_loop = heartbeat_timer.clone();
    let kv_for_loop = kv_store.clone();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

            let state = state_for_loop.read().await;
            let role = state.role;
            let node_id = state.node_id;
            drop(state);

            match role {
                raft::NodeRole::Follower | raft::NodeRole::Candidate => {
                    let timer = timer_for_election.read().await;
                    if timer.is_expired() {
                        drop(timer);
                        tracing::info!("Node {} election timeout, starting election", node_id);
                        election_for_loop.start_election().await;
                        timer_for_election.write().await.reset();
                    }
                }
                raft::NodeRole::Leader => {
                    // 发送心跳和日志复制
                    let timer = heartbeat_timer_for_loop.read().await;
                    if timer.should_beat() {
                        drop(timer);
                        heartbeat_timer_for_loop.write().await.reset();

                        // 发送心跳
                        replication_for_loop.send_heartbeat().await;

                        // 更新 commit index
                        replication_for_loop.update_commit_index().await;
                    }
                }
            }

            // 应用已提交的日志到状态机（所有节点）
            replication_for_loop.apply_committed_entries(&kv_for_loop).await;

            // 检查是否需要创建快照
            if replication_for_loop.should_snapshot().await {
                if let Err(e) = replication_for_loop.create_snapshot(&kv_for_loop).await {
                    tracing::error!("Failed to create snapshot: {}", e);
                }
            }
        }
    });

    // 创建 Raft gRPC 服务
    let raft_service = RaftServiceImpl::new(
        raft_state.clone(),
        log_store.clone(),
        election.clone(),
        election_timer.clone(),
        storage.clone(),
        kv_store.clone(),
    );

    // 创建 KV gRPC 服务
    let kv_service = KVServiceImpl::new_with_raft(
        kv_store.clone(),
        raft_state.clone(),
        log_store.clone(),
        pending_requests.clone(),
        storage.clone(),
    );

    println!("\nServers starting...");
    println!("  Client API: {}", config.client_addr);
    println!("  Raft RPC:   {}", config.raft_addr);

    // 同时启动两个 gRPC 服务器
    let client_addr = config.client_addr;
    let raft_addr = config.raft_addr;

    let client_server = tokio::spawn(async move {
        Server::builder()
            .add_service(CalculatorServer::new(MyCalculator::default()))
            .add_service(KvServiceServer::new(kv_service))
            .serve(client_addr)
            .await
    });

    let raft_server = tokio::spawn(async move {
        Server::builder()
            .add_service(RaftServiceServer::new(raft_service))
            .serve(raft_addr)
            .await
    });

    // 等待任一服务器完成
    tokio::select! {
        result = client_server => {
            result??;
        }
        result = raft_server => {
            result??;
        }
    }

    Ok(())
}
