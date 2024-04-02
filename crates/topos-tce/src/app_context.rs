//!
//! Application logic glue
//!
use futures::{Stream, StreamExt};
use prometheus::HistogramTimer;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use topos_core::uci::CertificateId;
use topos_metrics::CERTIFICATE_DELIVERED_TOTAL;
use topos_p2p::{Event as NetEvent, NetworkClient};
use topos_tce_api::RuntimeClient as ApiClient;
use topos_tce_api::RuntimeContext;
use topos_tce_api::RuntimeEvent as ApiEvent;
use topos_tce_broadcast::event::ProtocolEvents;
use topos_tce_broadcast::ReliableBroadcastClient;
use topos_tce_gatekeeper::GatekeeperClient;
use topos_tce_storage::store::ReadStore;
use topos_tce_storage::types::CertificateDeliveredWithPositions;
use topos_tce_storage::validator::ValidatorStore;
use topos_tce_storage::StorageClient;
use topos_tce_synchronizer::SynchronizerEvent;
use tracing::{error, info, warn};

mod api;
mod network;
pub(crate) mod protocol;

/// Top-level transducer main app context & driver (alike)
///
/// Implements <...Host> traits for network and Api, listens for protocol events in events
/// (store is not active component).
///
/// In the end we shall come to design where this struct receives
/// config+data as input and runs app returning data as output
///
pub struct AppContext {
    pub(crate) is_validator: bool,
    pub(crate) tce_cli: ReliableBroadcastClient,
    pub(crate) network_client: NetworkClient,
    pub(crate) api_client: ApiClient,
    pub(crate) pending_storage: StorageClient,
    pub(crate) gatekeeper: GatekeeperClient,

    pub(crate) delivery_latency: HashMap<CertificateId, HistogramTimer>,

    pub(crate) validator_store: Arc<ValidatorStore>,
    // Hold the api context, cleaning up when Drop
    #[allow(unused)]
    pub(crate) api_context: RuntimeContext,
}

impl AppContext {
    // Default previous certificate id for first certificate in the subnet
    // TODO: Remove, it will be genesis certificate id retrieved from Topos Subnet
    const DUMMY_INITIAL_CERTIFICATE_ID: CertificateId =
        CertificateId::from_array([0u8; topos_core::uci::CERTIFICATE_ID_LENGTH]);

    /// Factory
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        is_validator: bool,
        pending_storage: StorageClient,
        tce_cli: ReliableBroadcastClient,
        network_client: NetworkClient,
        api_client: ApiClient,
        gatekeeper: GatekeeperClient,
        validator_store: Arc<ValidatorStore>,
        api_context: RuntimeContext,
    ) -> Self {
        Self {
            is_validator,
            tce_cli,
            network_client,
            api_client,
            pending_storage,
            gatekeeper,
            delivery_latency: Default::default(),
            validator_store,
            api_context,
        }
    }

    /// Main processing loop
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        mut self,
        mut network_stream: impl Stream<Item = NetEvent> + Unpin,
        mut tce_stream: impl Stream<Item = ProtocolEvents> + Unpin,
        mut api_stream: impl Stream<Item = ApiEvent> + Unpin,
        mut synchronizer_stream: impl Stream<Item = SynchronizerEvent> + Unpin,
        mut broadcast_stream: impl Stream<Item = CertificateDeliveredWithPositions> + Unpin,
        shutdown: (CancellationToken, mpsc::Sender<()>),
    ) {
        loop {
            tokio::select! {

                Some(delivery) = broadcast_stream.next() => {
                    let certificate_id = delivery.0.certificate.id;
                    CERTIFICATE_DELIVERED_TOTAL.inc();

                    if let Some(timer) = self.delivery_latency.remove(&certificate_id) {
                        let duration = timer.stop_and_record();
                        info!("Certificate {} delivered with total latency: {}s", certificate_id, duration);
                    }
                }

                // protocol
                Some(evt) = tce_stream.next() => {
                    self.on_protocol_event(evt).await;
                },

                // network
                Some(net_evt) = network_stream.next() => {
                    self.on_net_event(net_evt).await;
                }

                // api events
                Some(event) = api_stream.next() => {
                    self.on_api_event(event).await;
                }

                // Synchronizer events
                Some(_event) = synchronizer_stream.next() => {
                }

                // Shutdown signal
                _ = shutdown.0.cancelled() => {
                    info!("Shutting down TCE app context...");

                    if let Err(e) = self.shutdown().await {
                        error!("Failed to shutdown the TCE app context: {e}");
                    }
                    // Drop the sender to notify the TCE termination
                    drop(shutdown.1);
                    break;
                }
            }
        }
        warn!("Exiting main TCE app processing loop")
    }

    pub async fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Shutting down the TCE client...");

        self.api_client.shutdown().await?;
        self.tce_cli.shutdown().await?;
        self.gatekeeper.shutdown().await?;
        self.network_client.shutdown().await?;

        let certificates_synced = self
            .validator_store
            .count_certificates_delivered()
            .map_err(|error| format!("Unable to count certificates delivered: {error}"))
            .unwrap();

        let pending_certificates = self
            .validator_store
            .pending_pool_size()
            .map_err(|error| format!("Unable to count pending certificates: {error}"))
            .unwrap();

        let precedence_pool_certificates = self
            .validator_store
            .precedence_pool_size()
            .map_err(|error| format!("Unable to count precedence pool certificates: {error}"))
            .unwrap();

        info!(
            "Stopping with {} certificates delivered, {} pending certificates and {} certificates \
             in the precedence pool",
            certificates_synced, pending_certificates, precedence_pool_certificates
        );
        Ok(())
    }
}
