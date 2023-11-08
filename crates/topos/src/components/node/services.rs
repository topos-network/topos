use crate::config::sequencer::SequencerConfig;
use crate::config::tce::TceConfig;
use crate::edge::CommandConfig;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;
use tokio::{spawn, sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use topos_p2p::config::NetworkConfig;
use topos_sequencer::SequencerConfiguration;
use topos_tce::config::{AuthKey, StorageConfiguration, TceConfiguration};
use topos_tce_transport::ReliableBroadcastParams;
use topos_wallet::SecretManager;
use tracing::{debug, error, info};

use crate::config::genesis::Genesis;

#[derive(Error, Debug)]
pub enum Errors {
    #[error("Failure on the TCE")]
    TceFailure,
    #[error("Failure on the Sequencer")]
    SequencerFailure,
    #[error("Failure on the Edge")]
    EdgeTerminated(#[from] std::io::Error),
}

pub fn generate_edge_config(
    edge_path: PathBuf,
    config_path: PathBuf,
) -> JoinHandle<Result<(), Errors>> {
    // Create the Polygon Edge config
    info!("Generating the configuration at {config_path:?}");
    info!("Polygon-edge binary located at: {edge_path:?}");
    spawn(async move {
        match CommandConfig::new(edge_path)
            .init(&config_path)
            .spawn()
            .await
        {
            Ok(status) => {
                info!("Edge process terminated: {status:?}");
                Ok(())
            }
            Err(e) => {
                println!("Failed to run the edge binary: {e:?}");
                Err(Errors::EdgeTerminated(e))
            }
        }
    })
}

pub(crate) fn spawn_sequencer_process(
    config: SequencerConfig,
    keys: &SecretManager,
    shutdown: (CancellationToken, mpsc::Sender<()>),
) -> JoinHandle<Result<(), Errors>> {
    let config = SequencerConfiguration {
        subnet_id: config.subnet_id,
        public_key: keys.validator_pubkey(),
        subnet_jsonrpc_http: config.subnet_jsonrpc_http,
        subnet_jsonrpc_ws: config.subnet_jsonrpc_ws,
        subnet_contract_address: config.subnet_contract_address,
        tce_grpc_endpoint: config.tce_grpc_endpoint,
        signing_key: keys.validator.clone().unwrap(),
        verifier: 0,
        start_block: config.start_block,
    };

    debug!("Sequencer args: {config:?}");
    spawn(async move {
        topos_sequencer::run(config, shutdown).await.map_err(|e| {
            error!("Failure on the Sequencer: {e:?}");
            Errors::SequencerFailure
        })
    })
}

pub(crate) fn spawn_tce_process(
    config: TceConfig,
    keys: SecretManager,
    genesis: Genesis,
    shutdown: (CancellationToken, mpsc::Sender<()>),
) -> JoinHandle<Result<(), Errors>> {
    let tce_config = TceConfiguration {
        boot_peers: genesis
            .boot_peers(Some(topos_p2p::constants::TCE_BOOTNODE_PORT))
            .into_iter()
            .chain(config.parse_boot_peers())
            .collect::<Vec<_>>(),
        validators: genesis.validators().expect("Cannot parse validators"),
        auth_key: keys.network.map(AuthKey::PrivateKey),
        signing_key: keys.validator.map(AuthKey::PrivateKey),
        tce_addr: format!("/ip4/{}", config.libp2p_api_addr.ip()),
        tce_local_port: config.libp2p_api_addr.port(),
        tce_params: ReliableBroadcastParams::new(genesis.validator_count()),
        api_addr: config.grpc_api_addr,
        graphql_api_addr: config.graphql_api_addr,
        metrics_api_addr: config.metrics_api_addr,
        storage: StorageConfiguration::RocksDB(Some(config.db_path)),
        network_bootstrap_timeout: Duration::from_secs(180),
        minimum_cluster_size: config
            .minimum_tce_cluster_size
            .unwrap_or(NetworkConfig::MINIMUM_CLUSTER_SIZE),
        version: env!("TOPOS_VERSION"),
    };

    debug!("TCE args: {tce_config:?}");
    spawn(async move {
        topos_tce::run(&tce_config, shutdown).await.map_err(|e| {
            error!("TCE process terminated: {e:?}");
            Errors::TceFailure
        })
    })
}

pub fn spawn_edge_process(
    edge_path: PathBuf,
    data_dir: PathBuf,
    genesis_path: PathBuf,
    edge_args: HashMap<String, String>,
) -> JoinHandle<Result<(), Errors>> {
    spawn(async move {
        match CommandConfig::new(edge_path)
            .server(&data_dir, &genesis_path, edge_args)
            .spawn()
            .await
        {
            Ok(status) => {
                info!("Edge process terminated: {status:?}");
                Ok(())
            }
            Err(e) => Err(Errors::EdgeTerminated(e)),
        }
    })
}
