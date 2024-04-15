use std::{future::IntoFuture, sync::Arc};

use tokio::{spawn, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use topos_p2p::NetworkClient;
use topos_tce_storage::validator::ValidatorStore;
use tracing::Instrument;

use crate::{
    checkpoints_collector::{CheckpointSynchronizer, CheckpointsCollectorError},
    Synchronizer, SynchronizerError, SynchronizerEvent,
};
use topos_config::tce::synchronization::SynchronizationConfig;

pub struct SynchronizerBuilder {
    network_client: Option<NetworkClient>,
    store: Option<Arc<ValidatorStore>>,
    config: SynchronizationConfig,
    /// Size of the channel producing events (default: 100)
    event_channel_size: usize,
    /// CancellationToken used to trigger shutdown of the Synchronizer
    shutdown: Option<CancellationToken>,
}

impl Default for SynchronizerBuilder {
    fn default() -> Self {
        Self {
            network_client: None,
            store: None,
            config: SynchronizationConfig::default(),
            event_channel_size: 10_000,
            shutdown: None,
        }
    }
}

impl SynchronizerBuilder {
    pub fn build(
        mut self,
    ) -> Result<(Synchronizer, ReceiverStream<SynchronizerEvent>), SynchronizerError> {
        let shutdown = if let Some(shutdown) = self.shutdown.take() {
            shutdown
        } else {
            return Err(SynchronizerError::CheckpointsCollectorError(
                CheckpointsCollectorError::NoStore,
            ))?;
        };
        let (events, events_recv) = mpsc::channel(self.event_channel_size);
        let (sync_events, checkpoints_collector_stream) = mpsc::channel(self.event_channel_size);

        let checkpoints_collector_stream = ReceiverStream::new(checkpoints_collector_stream);

        spawn(
            CheckpointSynchronizer {
                config: self.config,
                network: if let Some(network) = self.network_client {
                    network
                } else {
                    return Err(SynchronizerError::CheckpointsCollectorError(
                        CheckpointsCollectorError::NoNetworkClient,
                    ))?;
                },

                store: if let Some(store) = self.store {
                    store
                } else {
                    return Err(SynchronizerError::CheckpointsCollectorError(
                        CheckpointsCollectorError::NoStore,
                    ))?;
                },
                current_request_id: None,
                shutdown: shutdown.child_token(),
                events: sync_events,
            }
            .into_future()
            .in_current_span(),
        );

        Ok((
            Synchronizer {
                shutdown,
                events,
                checkpoints_collector_stream,
            },
            ReceiverStream::new(events_recv),
        ))
    }
}

impl SynchronizerBuilder {
    pub fn with_store(mut self, store: Arc<ValidatorStore>) -> Self {
        self.store = Some(store);

        self
    }

    pub fn with_network_client(mut self, network_client: NetworkClient) -> Self {
        self.network_client = Some(network_client);

        self
    }

    pub fn with_config(mut self, config: SynchronizationConfig) -> Self {
        self.config = config;

        self
    }

    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = Some(shutdown);

        self
    }
}
