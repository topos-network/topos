use serde::{Deserialize, Serialize};

/// Configuration for the TCE synchronization
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct SynchronizationConfig {
    /// Interval in seconds to synchronize the TCE
    #[serde(default = "SynchronizationConfig::default_interval_seconds")]
    pub interval_seconds: u64,

    /// Maximum number of Proof of delivery per query per subnet
    #[serde(default = "SynchronizationConfig::default_limit_per_subnet")]
    pub limit_per_subnet: usize,
}

impl Default for SynchronizationConfig {
    fn default() -> Self {
        Self {
            interval_seconds: SynchronizationConfig::INTERVAL_SECONDS,
            limit_per_subnet: SynchronizationConfig::LIMIT_PER_SUBNET,
        }
    }
}

impl SynchronizationConfig {
    pub const INTERVAL_SECONDS: u64 = 60;
    pub const LIMIT_PER_SUBNET: usize = 100;

    const fn default_interval_seconds() -> u64 {
        Self::INTERVAL_SECONDS
    }

    const fn default_limit_per_subnet() -> usize {
        Self::LIMIT_PER_SUBNET
    }
}
