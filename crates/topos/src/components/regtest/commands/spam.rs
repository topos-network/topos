use clap::Args;

#[derive(Args, Debug)]
#[command(
    about = "Run a test topos certificate spammer to send test certificates to the network, \
             generating randomly among the `nb_subnets` subnets the batch of `cert_per_batch` \
             certificates at every `batch-interval`"
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
    pub target_nodes: Option<Vec<String>>,
    /// Path to json file with list of target nodes as alternative to `--target-nodes`
    #[clap(long, env = "TOPOS_NETWORK_SPAMMER_TARGET_NODES_PATH")]
    pub target_nodes_path: Option<String>,
    /// Seed for generation of local private signing keys and corresponding subnet ids.
    #[arg(
        long,
        env = "TOPOS_NETWORK_SPAMMER_LOCAL_KEY_SEED",
        default_value = "1"
    )]
    pub local_key_seed: u64,
    /// Certificates generated in one batch. Batch is generated every `batch-interval` milliseconds.
    #[arg(
        long,
        env = "TOPOS_NETWORK_SPAMMER_CERT_PER_BATCH",
        default_value = "1"
    )]
    pub cert_per_batch: u64,
    /// Number of subnets to use for certificate generation. For every certificate subnet id will be picked randomly.
    #[arg(
        long,
        env = "TOPOS_NETWORK_SPAMMER_NUMBER_OF_SUBNETS",
        default_value = "10"
    )]
    pub nb_subnets: u8,
    /// Number of batches to generate before finishing execution.
    /// If not specified, batches will be generated indefinitely.
    #[arg(long, env = "TOPOS_NETWORK_SPAMMER_NUMBER_OF_BATCHES")]
    pub nb_batches: Option<u64>,
    /// Time interval in milliseconds between generated batches of certificates
    #[arg(
        long,
        env = "TOPOS_NETWORK_SPAMMER_BATCH_INTERVAL",
        default_value = "2000"
    )]
    pub batch_interval: u64,
    /// List of generated certificate target subnets. No target subnets by default.
    /// For example `--target-subnets=0x3bc19e36ff1673910575b6727a974a9abd80c9a875d41ab3e2648dbfb9e4b518,0xa00d60b2b408c2a14c5d70cdd2c205db8985ef737a7e55ad20ea32cc9e7c417c`
    #[arg(
        long,
        env = "TOPOS_NETWORK_SPAMMER_TARGET_SUBNETS",
        value_delimiter = ','
    )]
    pub target_subnets: Option<Vec<String>>,
    /// Socket of the opentelemetry agent endpoint.
    /// If not provided open telemetry will not be used
    #[arg(long, env = "TOPOS_OTLP_AGENT")]
    pub otlp_agent: Option<String>,
    /// Otlp service name.
    /// If not provided open telemetry will not be used
    #[arg(long, env = "TOPOS_OTLP_SERVICE_NAME")]
    pub otlp_service_name: Option<String>,
    /// Flag to indicate usage of Kubernetes.
    #[arg(
        long,
        env = "TOPOS_NETWORK_SPAMMER_BENCHMARK",
        requires = "target_hosts",
        requires = "number"
    )]
    pub benchmark: bool,
    /// Template for generating target node entrypoints.
    /// e.g. `--hosts="http://validator-{N}:1340"`
    #[arg(
        long,
        env = "TOPOS_NETWORK_SPAMMER_TARGET_HOSTS",
        requires = "benchmark"
    )]
    pub target_hosts: Option<String>,
    /// Number of nodes to generate based on the DNS template.
    #[arg(long, env = "TOPOS_NETWORK_SPAMMER_NUMBER", requires = "benchmark")]
    pub number: Option<u32>,
}

impl Spam {}
