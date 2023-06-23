use std::path::PathBuf;

use clap::Parser;
use serde::Serialize;

#[derive(Parser, Debug, Clone, Serialize)]
#[command(
    about = "Run a test topos certificate spammer to send test certificates to the network, generating randomly among \
the `nb_subnets` subnets the batch of `cert_per_batch` certificates at every `batch-interval`"
)]
pub struct Spam {
    /// The target node api endpoint.
    /// Multiple nodes could be specified as comma separated list
    /// e.g. `--target-nodes=http://[::1]:1340,http://[::1]:1341`
    #[clap(
        long,
        env = "TOPOS_NETWORK_SPAMMER_TARGET_NODES",
        value_delimiter = ','
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_nodes: Option<Vec<String>>,
    /// Path to json file with list of target nodes as alternative to `--target-nodes`
    #[clap(long, env = "TOPOS_NETWORK_SPAMMER_TARGET_NODES_PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_nodes_path: Option<String>,

    /// Seed for generation of local private signing keys and corresponding subnet ids.
    #[arg(long, env = "TOPOS_NETWORK_SPAMMER_LOCAL_KEY_SEED")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_key_seed: Option<u64>,

    /// Certificates generated in one batch. Batch is generated every `batch-interval` milliseconds.
    #[arg(long, env = "TOPOS_NETWORK_SPAMMER_CERT_PER_BATCH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_per_batch: Option<u8>,

    /// Number of subnets to use for certificate generation. For every certificate subnet id will be picked randomly.
    #[arg(long, env = "TOPOS_NETWORK_SPAMMER_NUMBER_OF_SUBNETS")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nb_subnets: Option<u8>,

    /// Number of batches to generate before finishing execution.
    /// If not specified, batches will be generated indefinitely.
    #[arg(long, env = "TOPOS_NETWORK_SPAMMER_NUMBER_OF_BATCHES")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nb_batches: Option<u64>,

    /// Time interval in milliseconds between generated batches of certificates
    #[arg(long, env = "TOPOS_NETWORK_SPAMMER_BATCH_INTERVAL")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_interval: Option<u64>,

    /// List of generated certificate target subnets. No target subnets by default.
    /// For example `--target-subnets=0x3bc19e36ff1673910575b6727a974a9abd80c9a875d41ab3e2648dbfb9e4b518,0xa00d60b2b408c2a14c5d70cdd2c205db8985ef737a7e55ad20ea32cc9e7c417c`
    #[arg(
        long,
        env = "TOPOS_NETWORK_SPAMMER_TARGET_SUBNETS",
        value_delimiter = ','
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_subnets: Option<Vec<String>>,

    /// Socket of the opentelemetry agent endpoint.
    /// If not provided open telemetry will not be used
    #[arg(long, env = "TOPOS_OTLP_AGENT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_agent: Option<String>,

    /// Otlp service name.
    /// If not provided open telemetry will not be used
    #[arg(long, env = "TOPOS_OTLP_SERVICE_NAME")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_service_name: Option<String>,
}
