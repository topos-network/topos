use futures::{Future, StreamExt};
use opentelemetry::global;
use std::process::ExitStatus;
use std::{future::IntoFuture, sync::Arc};
use tokio::{
    spawn,
    sync::{broadcast, mpsc},
};
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;
use topos_config::tce::TceConfig;
use topos_core::api::grpc::tce::v1::synchronizer_service_server::SynchronizerServiceServer;
use topos_crypto::{messages::MessageSigner, validator_id::ValidatorId};
use topos_p2p::{
    utils::{local_key_pair, local_key_pair_from_slice},
    GrpcContext, GrpcRouter,
};
use topos_tce_broadcast::{ReliableBroadcastClient, ReliableBroadcastConfig};
use topos_tce_storage::{store::ReadStore, validator::ValidatorStore, StorageClient};
use topos_tce_synchronizer::SynchronizerService;
use tracing::{debug, info, warn};

mod app_context;
pub mod events;
#[cfg(test)]
mod tests;

pub use app_context::AppContext;

use topos_config::tce::{AuthKey, StorageConfiguration};

// TODO: Estimate on the max broadcast throughput, could need to be override by config
const BROADCAST_CHANNEL_SIZE: usize = 10_000;

pub async fn launch(
    config: &TceConfig,
    shutdown: (CancellationToken, mpsc::Sender<()>),
) -> Result<ExitStatus, Box<dyn std::error::Error>> {
    let cancel = shutdown.0.clone();
    let run_fut = run(config, shutdown);
    let app_context_run = tokio::select! {
            _ = cancel.cancelled() => {
                return Err(Box::from("Killed before readiness".to_string()));
            }

            result = run_fut => {
                match result {
                    Ok(app_context_run)=> app_context_run,
                    Err(error) => return Err(error)
                }
            }
    };

    app_context_run.await;

    global::shutdown_tracer_provider();
    Ok(ExitStatus::default())
}

pub async fn run(
    config: &TceConfig,
    shutdown: (CancellationToken, mpsc::Sender<()>),
) -> Result<impl Future<Output = ()>, Box<dyn std::error::Error>> {
    // Preboot phase - start
    topos_metrics::init_metrics();

    let key = match config.auth_key.as_ref() {
        Some(AuthKey::Seed(seed)) => local_key_pair_from_slice(&seed[..]),
        Some(AuthKey::PrivateKey(pk)) => topos_p2p::utils::keypair_from_protobuf_encoding(&pk[..]),
        None => local_key_pair(None),
    };

    let message_signer = match &config.signing_key {
        Some(AuthKey::PrivateKey(pk)) => Arc::new(MessageSigner::new(&pk[..])?),
        _ => return Err(Box::from("Error, no singing key".to_string())),
    };

    let validator_id: ValidatorId = message_signer.public_address.into();
    let public_address = validator_id.to_string();

    warn!("Public node address: {public_address}");

    let peer_id = key.public().to_peer_id();

    warn!("I am {peer_id}");

    tracing::Span::current().record("peer_id", &peer_id.to_string());

    let mut boot_peers = config.boot_peers.clone();

    // Remove myself from the bootnode list
    boot_peers.retain(|(p, _)| *p != peer_id);
    let is_validator = config.validators.contains(&validator_id);

    // Preboot phase - stop
    // Healthiness phase - start
    debug!("Starting the Storage");
    let path = if let StorageConfiguration::RocksDB(Some(ref path)) = config.storage {
        path
    } else {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Unsupported storage type {:?}", config.storage),
        )));
    };

    let validator_store = ValidatorStore::new(path)
        .map_err(|error| format!("Unable to create validator store: {error}"))?;

    let fullnode_store = validator_store.fullnode_store();

    let storage_client = StorageClient::new(validator_store.clone());

    let certificates_synced = fullnode_store
        .count_certificates_delivered()
        .map_err(|error| format!("Unable to count certificates delivered: {error}"))?;

    let pending_certificates = validator_store
        .pending_pool_size()
        .map_err(|error| format!("Unable to count pending certificates: {error}"))?;

    let precedence_pool_certificates = validator_store
        .precedence_pool_size()
        .map_err(|error| format!("Unable to count precedence pool certificates: {error}"))?;

    info!(
        "Storage initialized with {} certificates delivered, {} pending certificates and {} \
         certificates in the precedence pool",
        certificates_synced, pending_certificates, precedence_pool_certificates
    );

    let grpc_context = GrpcContext::default().with_router(
        GrpcRouter::new(tonic::transport::Server::builder()).add_service(
            SynchronizerServiceServer::new(SynchronizerService {
                validator_store: validator_store.clone(),
            }),
        ),
    );

    let (network_client, mut event_stream, network_runtime) = topos_p2p::network::builder()
        .peer_key(key)
        .listen_addresses(config.p2p.listen_addresses.clone())
        .minimum_cluster_size(config.minimum_tce_cluster_size)
        .public_addresses(config.p2p.public_addresses.clone())
        .known_peers(&boot_peers)
        .grpc_context(grpc_context)
        .build()
        .await?;

    debug!("Starting the p2p network");
    let _network_handle = network_runtime.bootstrap(&mut event_stream).await?;
    debug!("P2P layer bootstrapped");

    debug!("Creating the Synchronizer");
    let (synchronizer_runtime, synchronizer_stream) =
        topos_tce_synchronizer::Synchronizer::builder()
            .with_config(config.synchronization.clone())
            .with_shutdown(shutdown.0.child_token())
            .with_store(validator_store.clone())
            .with_network_client(network_client.clone())
            .build()?;

    debug!("Synchronizer created");

    debug!("Starting gRPC api");
    let (broadcast_sender, broadcast_receiver) = broadcast::channel(BROADCAST_CHANNEL_SIZE);

    let (api_client, api_stream, ctx) = topos_tce_api::Runtime::builder()
        .with_peer_id(peer_id.to_string())
        .with_broadcast_stream(broadcast_receiver.resubscribe())
        .serve_grpc_addr(config.grpc_api_addr)
        .serve_graphql_addr(config.graphql_api_addr)
        .serve_metrics_addr(config.metrics_api_addr)
        .store(validator_store.clone())
        .storage(storage_client.clone())
        .build_and_launch()
        .await;
    debug!("gRPC api started");

    // Healthiness phase - stop

    debug!("Starting the gatekeeper");
    let (gatekeeper_client, gatekeeper_runtime) =
        topos_tce_gatekeeper::Gatekeeper::builder().await?;

    spawn(gatekeeper_runtime.into_future());
    debug!("Gatekeeper started");

    debug!("Starting reliable broadcast");

    let (tce_cli, tce_stream) = ReliableBroadcastClient::new(
        ReliableBroadcastConfig {
            tce_params: config.tce_params.clone(),
            validator_id,
            validators: config.validators.clone(),
            message_signer,
        },
        validator_store.clone(),
        broadcast_sender,
    )
    .await;

    debug!("Reliable broadcast started");

    spawn(synchronizer_runtime.into_future());
    // setup transport-tce-storage-api connector
    let (app_context, _tce_stream) = AppContext::new(
        is_validator,
        storage_client,
        tce_cli,
        network_client,
        api_client,
        gatekeeper_client,
        validator_store,
        ctx,
    );

    Ok(app_context.run(
        event_stream,
        tce_stream,
        api_stream,
        synchronizer_stream,
        BroadcastStream::new(broadcast_receiver).filter_map(|v| futures::future::ready(v.ok())),
        shutdown,
    ))
}
