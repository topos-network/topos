use std::{env, time::Duration};

use lazy_static::lazy_static;
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;

lazy_static! {
    /// Metric Registry used to register all the metrics from libp2p::gossipsub
    // NOTE: During tests, if multiple instances are started, they will all point to the same
    // registry.
    pub static ref METRIC_REGISTRY: Mutex<Registry> = Mutex::new(<Registry>::with_prefix("topos"));
    pub static ref EVENT_STREAM_BUFFER: usize = env::var("TCE_EVENT_STREAM_BUFFER")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(2048 * 2);
    pub static ref CAPACITY_EVENT_STREAM_BUFFER: usize = EVENT_STREAM_BUFFER
        .checked_mul(10)
        .map(|v| {
            let r: usize = v.checked_div(100).unwrap_or(*EVENT_STREAM_BUFFER);
            r
        })
        .unwrap_or(*EVENT_STREAM_BUFFER);
    pub static ref COMMAND_STREAM_BUFFER_SIZE: usize = env::var("TCE_COMMAND_STREAM_BUFFER_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(2048);
}

pub const DISCOVERY_PROTOCOL: &str = "/tce-disco/1";
pub const PEER_INFO_PROTOCOL: &str = "/tce-peer-info/1";
pub const GRPC_P2P_TOPOS_PROTOCOL: &str = "/topos-grpc-p2p/1.0";

// FIXME: Considered as constant until customizable and exposed properly in the genesis file
pub const TCE_BOOTNODE_PORT: u16 = 9090;

/// Swarm idle connection timeout
pub const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
