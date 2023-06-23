use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use topos_p2p::{Multiaddr, PeerId};

#[derive(Serialize, Deserialize, Debug)]
pub struct TceConfig {
    /// Boot nodes to connect to, pairs of <PeerId> <Multiaddr>, space separated,
    /// quoted list like --boot-peers='a a1,b b1'
    #[serde(default = "default_boot_peers")]
    pub boot_peers: String,

    /// Advertised (externally visible) <host>,
    /// if empty this machine ip address(es) are used
    #[serde(default = "default_tce_ext_host")]
    pub tce_ext_host: String,

    /// Port to listen on (host is 0.0.0.0, should be good for most installations)
    #[serde(default = "default_tce_local_port")]
    pub tce_local_port: u16,

    /// WebAPI external url <host|address:port> (optional)
    pub web_api_ext_url: Option<String>,

    /// WebAPI port
    #[serde(default = "default_web_api_local_port")]
    pub web_api_local_port: u16,

    /// Local peer secret key seed (optional, used for testing)
    pub local_key_seed: Option<String>,

    /// Local peer key-pair (in base64 format)
    pub local_key_pair: Option<String>,

    /// Storage database path, if not set RAM storage is used
    #[serde(default = "default_db_path")]
    pub db_path: String,

    /// gRPC API Addr
    #[serde(default = "default_api_addr")]
    pub api_addr: SocketAddr,

    /// GraphQL API Addr
    #[serde(default = "default_graphql_api_addr")]
    pub graphql_api_addr: SocketAddr,

    /// Socket of the opentelemetry agent endpoint
    /// If not provided open telemetry will not be used
    pub otlp_agent: Option<String>,

    /// Otlp service name
    /// If not provided open telemetry will not be used
    pub otlp_service_name: Option<String>,

    pub minimum_tce_cluster_size: Option<usize>,

    /// Echo threshold
    #[serde(default = "default_echo_threshold")]
    pub echo_threshold: usize,

    /// Ready threshold
    #[serde(default = "default_ready_threshold")]
    pub ready_threshold: usize,

    /// Delivery threshold
    #[serde(default = "default_delivery_threshold")]
    pub delivery_threshold: usize,
}

fn default_boot_peers() -> String {
    "".to_string()
}

fn default_tce_ext_host() -> String {
    "/ip4/0.0.0.0".to_string()
}

fn default_tce_local_port() -> u16 {
    0
}

fn default_web_api_local_port() -> u16 {
    8080
}

fn default_db_path() -> String {
    "./default_db/".to_string()
}

fn default_api_addr() -> SocketAddr {
    "[::1]:1340"
        .parse()
        .expect("Cannot parse address to SocketAddr")
}

fn default_graphql_api_addr() -> SocketAddr {
    "[::1]:4000"
        .parse()
        .expect("Cannot parse address to SocketAddr")
}

fn default_echo_threshold() -> usize {
    1
}

fn default_ready_threshold() -> usize {
    1
}

fn default_delivery_threshold() -> usize {
    1
}

impl TceConfig {
    pub fn parse_boot_peers(&self) -> Vec<(PeerId, Multiaddr)> {
        self.boot_peers
            .split(&[',', ' '])
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
            .chunks(2)
            .filter_map(|pair| {
                if pair.len() > 1 {
                    Some((
                        pair[0].as_str().parse().unwrap(),
                        pair[1].as_str().parse().unwrap(),
                    ))
                } else {
                    None
                }
            })
            .collect()
    }
}

impl Default for TceConfig {
    fn default() -> Self {
        TceConfig {
            boot_peers: default_boot_peers(),
            tce_ext_host: default_tce_ext_host(),
            tce_local_port: default_tce_local_port(),
            api_addr: default_api_addr(),
            graphql_api_addr: default_graphql_api_addr(),
            web_api_ext_url: None,
            web_api_local_port: default_web_api_local_port(),
            local_key_seed: None,
            local_key_pair: None,
            db_path: default_db_path(),
            otlp_agent: None,
            otlp_service_name: None,
            minimum_tce_cluster_size: None,
            echo_threshold: default_echo_threshold(),
            ready_threshold: default_ready_threshold(),
            delivery_threshold: default_delivery_threshold(),
        }
    }
}
