use std::{num::NonZeroUsize, time::Duration};

pub struct NetworkConfig {
    pub minimum_cluster_size: usize,
    pub client_retry_ttl: u64,
    pub discovery: DiscoveryConfig,
    pub yamux_max_buffer_size: usize,
    pub yamux_window_size: Option<u32>,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            minimum_cluster_size: Self::MINIMUM_CLUSTER_SIZE,
            client_retry_ttl: Self::CLIENT_RETRY_TTL,
            discovery: Default::default(),
            yamux_max_buffer_size: usize::MAX,
            yamux_window_size: None,
        }
    }
}

impl NetworkConfig {
    pub const MINIMUM_CLUSTER_SIZE: usize = 5;
    pub const CLIENT_RETRY_TTL: u64 = 200;
}

pub struct DiscoveryConfig {
    pub replication_factor: NonZeroUsize,
    pub replication_interval: Option<Duration>,
    pub publication_interval: Option<Duration>,
    pub provider_publication_interval: Option<Duration>,
    /// Interval at which the node will send bootstrap query to the network
    pub bootstrap_interval: Duration,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            replication_factor: NonZeroUsize::new(4).unwrap(),
            replication_interval: Some(Duration::from_secs(10)),
            publication_interval: Some(Duration::from_secs(10)),
            provider_publication_interval: Some(Duration::from_secs(10)),
            bootstrap_interval: Duration::from_secs(Self::BOOTSTRAP_INTERVAL),
        }
    }
}

impl DiscoveryConfig {
    /// Default bootstrap interval in seconds
    pub const BOOTSTRAP_INTERVAL: u64 = 60;

    pub fn with_replication_factor(mut self, replication_factor: NonZeroUsize) -> Self {
        self.replication_factor = replication_factor;

        self
    }
}
