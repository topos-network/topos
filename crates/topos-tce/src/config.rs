use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tce_transport::ReliableBroadcastParams;
use topos_p2p::{Multiaddr, PeerId};

pub use crate::AppContext;

#[derive(Debug)]
pub struct TceConfiguration {
    pub local_key_seed: Option<Vec<u8>>,
    pub tce_params: ReliableBroadcastParams,
    pub boot_peers: Vec<(PeerId, Multiaddr)>,
    pub api_addr: SocketAddr,
    pub graphql_api_addr: SocketAddr,
    pub metrics_api_addr: SocketAddr,
    pub tce_addr: String,
    pub tce_local_port: u16,
    pub storage: StorageConfiguration,
    pub network_bootstrap_timeout: Duration,
    pub minimum_cluster_size: usize,
    pub version: &'static str,
}

#[derive(Debug)]
pub enum StorageConfiguration {
    RAM,
    RocksDB(Option<PathBuf>),
}
