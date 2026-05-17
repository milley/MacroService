use clap::Parser;
use serde::Deserialize;
use std::net::SocketAddr;

/// Raft KV 节点配置
#[derive(Parser, Debug, Clone)]
#[command(name = "raft-kv-node")]
pub struct CliConfig {
    /// 节点 ID
    #[arg(short, long)]
    pub node_id: u32,

    /// 客户端服务端口
    #[arg(long, default_value = "50051")]
    pub client_port: u16,

    /// Raft 内部通信端口
    #[arg(long, default_value = "60051")]
    pub raft_port: u16,

    /// 集群节点列表 (格式: id@host:port,id@host:port)
    #[arg(long, default_value = "")]
    pub peers: String,
}

/// 集群节点信息
#[derive(Debug, Clone, Deserialize)]
pub struct Peer {
    pub id: u32,
    pub raft_addr: String,
}

/// 完整节点配置
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: u32,
    pub client_addr: SocketAddr,
    pub raft_addr: SocketAddr,
    pub peers: Vec<Peer>,
}

impl From<CliConfig> for NodeConfig {
    fn from(cli: CliConfig) -> Self {
        let peers: Vec<Peer> = if cli.peers.is_empty() {
            vec![]
        } else {
            cli.peers
                .split(',')
                .filter_map(|s| {
                    let parts: Vec<&str> = s.split('@').collect();
                    if parts.len() == 2 {
                        Some(Peer {
                            id: parts[0].parse().ok()?,
                            raft_addr: parts[1].to_string(),
                        })
                    } else {
                        None
                    }
                })
                .collect()
        };

        Self {
            node_id: cli.node_id,
            client_addr: format!("127.0.0.1:{}", cli.client_port)
                .parse()
                .expect("Invalid client address"),
            raft_addr: format!("127.0.0.1:{}", cli.raft_port)
                .parse()
                .expect("Invalid raft address"),
            peers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_cli(args: &[&str]) -> CliConfig {
        CliConfig::try_parse_from(args).unwrap()
    }

    #[test]
    fn test_cli_config_default() {
        let config = parse_cli(&["test", "--node-id", "1"]);

        assert_eq!(config.node_id, 1);
        assert_eq!(config.client_port, 50051);
        assert_eq!(config.raft_port, 60051);
        assert_eq!(config.peers, "");
    }

    #[test]
    fn test_cli_config_custom_ports() {
        let config = parse_cli(&[
            "test",
            "--node-id", "2",
            "--client-port", "50052",
            "--raft-port", "60052",
        ]);

        assert_eq!(config.node_id, 2);
        assert_eq!(config.client_port, 50052);
        assert_eq!(config.raft_port, 60052);
    }

    #[test]
    fn test_cli_config_with_peers() {
        let config = parse_cli(&[
            "test",
            "--node-id", "1",
            "--peers", "2@127.0.0.1:60052,3@127.0.0.1:60053",
        ]);

        assert_eq!(config.peers, "2@127.0.0.1:60052,3@127.0.0.1:60053");
    }

    #[test]
    fn test_node_config_from_cli() {
        let cli = parse_cli(&[
            "test",
            "--node-id", "1",
            "--client-port", "50051",
            "--raft-port", "60051",
            "--peers", "2@127.0.0.1:60052,3@127.0.0.1:60053",
        ]);

        let config: NodeConfig = cli.into();

        assert_eq!(config.node_id, 1);
        assert_eq!(config.client_addr.to_string(), "127.0.0.1:50051");
        assert_eq!(config.raft_addr.to_string(), "127.0.0.1:60051");
        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.peers[0].id, 2);
        assert_eq!(config.peers[0].raft_addr, "127.0.0.1:60052");
        assert_eq!(config.peers[1].id, 3);
    }

    #[test]
    fn test_node_config_empty_peers() {
        let cli = parse_cli(&["test", "--node-id", "1"]);
        let config: NodeConfig = cli.into();

        assert_eq!(config.peers.len(), 0);
    }

    #[test]
    fn test_peer_deserialize() {
        let peer = Peer {
            id: 2,
            raft_addr: "127.0.0.1:60052".to_string(),
        };

        assert_eq!(peer.id, 2);
        assert_eq!(peer.raft_addr, "127.0.0.1:60052");
    }
}
