use futures::{stream::FuturesUnordered, FutureExt, StreamExt, TryFutureExt};
use std::future::{Future, IntoFuture};
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    sync::{broadcast, oneshot},
};
use tokio_util::sync::CancellationToken;
use tonic_health::server::HealthReporter;
use topos_core::api::grpc::checkpoints::TargetStreamPosition;
use topos_core::api::grpc::tce::v1::api_service_server::ApiServiceServer;
use topos_core::types::CertificateDelivered;
use topos_core::uci::SubnetId;
use topos_tce_storage::{types::CertificateDeliveredWithPositions, StorageClient};

use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{
    constants::TRANSIENT_STREAM_CHANNEL_SIZE,
    grpc::TceGrpcService,
    stream::{StreamCommand, StreamError, StreamErrorKind, TransientStream},
};

pub mod builder;
pub use builder::RuntimeContext;
mod client;
mod commands;
pub mod error;
mod events;

mod sync_task;
#[cfg(test)]
mod tests;

pub use client::RuntimeClient;

use self::builder::RuntimeBuilder;
pub(crate) use self::commands::InternalRuntimeCommand;

pub use self::commands::RuntimeCommand;
pub use self::events::RuntimeEvent;

use crate::runtime::sync_task::{RunningTasks, SyncTask};

pub(crate) type Streams =
    FuturesUnordered<Pin<Box<dyn Future<Output = Result<Uuid, StreamError>> + Send>>>;

pub struct Runtime {
    /// Map of sync tasks and their stream id, so we can cancel them when a new stream
    /// with the same id is registered
    pub(crate) sync_tasks: HashMap<Uuid, CancellationToken>,
    /// Sync tasks that were registered for this node.
    pub(crate) running_sync_tasks: RunningTasks,

    pub(crate) broadcast_stream: broadcast::Receiver<CertificateDeliveredWithPositions>,

    pub(crate) storage: StorageClient,
    pub(crate) transient_streams: HashMap<Uuid, Sender<Arc<CertificateDelivered>>>,
    /// Streams that are currently active (with a valid handshake)
    pub(crate) active_streams: HashMap<Uuid, Sender<StreamCommand>>,
    /// Streams that are currently in negotiation
    pub(crate) pending_streams: HashMap<Uuid, Sender<StreamCommand>>,
    /// Mapping between a subnet_id and streams that are subscribed to it
    pub(crate) subnet_subscriptions: HashMap<SubnetId, HashSet<Uuid>>,
    /// Receiver for Internal API command
    pub(crate) internal_runtime_command_receiver: Receiver<InternalRuntimeCommand>,
    /// Receiver for Outside API command
    pub(crate) runtime_command_receiver: Receiver<RuntimeCommand>,
    /// HealthCheck reporter for gRPC
    pub(crate) health_reporter: HealthReporter,
    /// Sender that forward Event to the rest of the system
    pub(crate) api_event_sender: Sender<RuntimeEvent>,
    /// Shutdown signal receiver
    pub(crate) shutdown: mpsc::Receiver<oneshot::Sender<()>>,
    /// Spawned stream that manage a gRPC stream
    pub(crate) streams: Streams,
}

impl Runtime {
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::default()
    }

    pub async fn launch(mut self) {
        let mut health_update = tokio::time::interval(Duration::from_secs(10));
        let shutdowned: Option<oneshot::Sender<()>> = loop {
            tokio::select! {
                shutdown = self.shutdown.recv() => {
                    break shutdown;
                },

                _ = health_update.tick() => {
                    self.health_reporter.set_serving::<ApiServiceServer<TceGrpcService>>().await;
                }

                Ok(certificate_delivered) = self.broadcast_stream.recv() => {
                    let certificate = certificate_delivered.0;
                    let certificate_id = certificate.certificate.id;
                    let positions = certificate_delivered.1;
                    let cmd = RuntimeCommand::DispatchCertificate {
                        certificate,
                        positions: positions
                            .targets
                            .into_iter()
                            .map(|(subnet_id, certificate_target_stream_position)| {
                                (
                                    subnet_id,
                                    TargetStreamPosition {
                                        target_subnet_id:
                                            certificate_target_stream_position.target_subnet_id,
                                        source_subnet_id:
                                            certificate_target_stream_position.source_subnet_id,
                                        position: *certificate_target_stream_position.position,
                                        certificate_id: Some(certificate_id),
                                    },
                                )
                            })
                        .collect::<HashMap<SubnetId, TargetStreamPosition>>()
                    };

                    self.handle_runtime_command(cmd).await;
                }

                Some(result) = self.streams.next() => {
                    self.handle_stream_termination(result).await;
                }

                Some(internal_command) = self.internal_runtime_command_receiver.recv() => {
                    self.handle_internal_command(internal_command).await;
                }

                Some(command) = self.runtime_command_receiver.recv() => {
                    self.handle_runtime_command(command).await;
                }

                Some(result) = self.running_sync_tasks.next() => {
                    debug!("SyncTask with StreamId: {:?} resulted in {:?}", result.0, result.1);
                }
            }
        };

        if let Some(sender) = shutdowned {
            info!("Shutting down the TCE API service...");
            _ = sender.send(());
        }
    }

    async fn handle_stream_termination(&mut self, stream_result: Result<Uuid, StreamError>) {
        match stream_result {
            Ok(stream_id) => {
                info!("Stream {stream_id} terminated gracefully");

                self.active_streams.remove(&stream_id);
                self.pending_streams.remove(&stream_id);
            }
            Err(StreamError { stream_id, kind }) => match kind {
                StreamErrorKind::HandshakeFailed(_)
                | StreamErrorKind::InvalidCommand
                | StreamErrorKind::MalformedTargetCheckpoint
                | StreamErrorKind::Transport(_)
                | StreamErrorKind::PreStartError
                | StreamErrorKind::StreamClosed
                | StreamErrorKind::Timeout => {
                    error!("Stream {stream_id} error: {kind:?}");

                    self.active_streams.remove(&stream_id);
                    self.pending_streams.remove(&stream_id);
                }
            },
        }
    }

    async fn handle_runtime_command(&mut self, command: RuntimeCommand) {
        match command {
            RuntimeCommand::DispatchCertificate {
                certificate,
                mut positions,
            } => {
                info!(
                    "Received DispatchCertificate for certificate cert_id: {:?}",
                    certificate.certificate.id
                );
                // Collect target subnets from certificate cross chain transaction list
                let target_subnets = certificate
                    .certificate
                    .target_subnets
                    .iter()
                    .collect::<HashSet<_>>();
                debug!(
                    "Dispatching certificate cert_id: {:?} to target subnets: {:?}",
                    &certificate.certificate.id, target_subnets
                );

                // Notify all the transient streams that a new certificate is available
                // To avoid double allocation for each stream, we clone an Arc of the certificate.
                // Each stream will convert the UCI certificate into a GraphQL one and send it to the transient stream.
                let shared_certificate = Arc::new(certificate.clone());
                for transient in self.transient_streams.values() {
                    let sender = transient.clone();
                    let shared_certificate = shared_certificate.clone();
                    tokio::spawn(async move {
                        _ = sender.send(shared_certificate).await;
                    });
                }

                for target_subnet_id in target_subnets {
                    let target_subnet_id = *target_subnet_id;
                    let target_position = positions.remove(&target_subnet_id);
                    if let Some(stream_list) = self.subnet_subscriptions.get(&target_subnet_id) {
                        let uuids: Vec<&Uuid> = stream_list.iter().collect();
                        for uuid in uuids {
                            if let Some(sender) = self.active_streams.get(uuid) {
                                let sender = sender.clone();
                                let certificate = certificate.clone();
                                info!("Sending certificate to {uuid}");
                                if let Some(target_position) = target_position.clone() {
                                    if let Err(error) = sender
                                        .send(StreamCommand::PushCertificate {
                                            certificate,
                                            positions: vec![target_position],
                                        })
                                        .await
                                    {
                                        error!(%error, "Can't push certificate because the receiver is dropped");
                                    }
                                } else {
                                    error!(
                                        "Invalid target stream position for cert id {}, target \
                                         subnet id {target_subnet_id}, dispatch failed",
                                        &certificate.certificate.id
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn handle_internal_command(&mut self, command: InternalRuntimeCommand) {
        match command {
            InternalRuntimeCommand::NewTransientStream { sender } => {
                let stream_id = Uuid::new_v4();
                info!("Opening a new transient stream with UUID {stream_id}");

                let (stream, receiver) = mpsc::channel(TRANSIENT_STREAM_CHANNEL_SIZE);
                let (shutdown, shutdown_recv) = oneshot::channel();
                self.transient_streams.insert(stream_id, stream);

                self.streams.push(
                    shutdown_recv
                        .map_err(move |_| StreamError {
                            stream_id,
                            kind: StreamErrorKind::StreamClosed,
                        })
                        .boxed(),
                );

                if sender
                    .send(Ok(TransientStream {
                        stream_id,
                        inner: receiver,
                        notifier: Some(shutdown),
                    }))
                    .is_err()
                {
                    error!("Unable to send new TransientStream");
                    _ = self.transient_streams.remove(&stream_id);
                }
            }

            InternalRuntimeCommand::NewStream {
                stream,
                command_sender,
            } => {
                let stream_id = stream.stream_id;
                info!("Opening a new stream with UUID {stream_id}");

                self.pending_streams.insert(stream_id, command_sender);

                self.streams.push(Box::pin(stream.run()));
            }

            InternalRuntimeCommand::Handshaked { stream_id } => {
                if let Some(sender) = self.pending_streams.remove(&stream_id) {
                    self.active_streams.insert(stream_id, sender);
                    info!("Stream {stream_id} has successfully handshake");
                }
            }

            InternalRuntimeCommand::Register {
                stream_id,
                sender,
                target_subnet_stream_positions,
            } => {
                info!("Stream {stream_id} is registered as subscriber");

                if let Some(cancel_token) = self.sync_tasks.remove(&stream_id) {
                    // Cancel the previous task
                    cancel_token.cancel();
                }

                let storage = self.storage.clone();
                let notifier = self
                    .active_streams
                    .get(&stream_id)
                    .or_else(|| self.pending_streams.get(&stream_id))
                    .cloned();

                if let Err(error) = sender.send(Ok(())) {
                    error!(
                        ?error,
                        "Failed to send response to the Stream, receiver is dropped"
                    );
                }

                if let Some(notifier) = notifier {
                    // TODO: Rework to remove old subscriptions
                    for target_subnet_id in target_subnet_stream_positions.keys() {
                        self.subnet_subscriptions
                            .entry(*target_subnet_id)
                            .or_default()
                            .insert(stream_id);
                    }

                    let cancel_token = CancellationToken::new();

                    let cloned_cancel_token = cancel_token.clone();

                    let task = SyncTask::new(
                        stream_id,
                        target_subnet_stream_positions,
                        storage,
                        notifier,
                        cancel_token,
                    );

                    self.running_sync_tasks.push(task.into_future());

                    self.sync_tasks.insert(stream_id, cloned_cancel_token);
                }
            }

            InternalRuntimeCommand::CertificateSubmitted {
                certificate,
                sender,
            } => {
                async move {
                    info!(
                        "A certificate has been submitted to the TCE {}",
                        certificate.id
                    );
                    if let Err(error) = self
                        .api_event_sender
                        .send(RuntimeEvent::CertificateSubmitted {
                            certificate,
                            sender,
                        })
                        .await
                    {
                        error!(
                            %error,
                            "Can't send certificate submission to runtime, receiver is dropped"
                        );
                    }
                }
                .await
            }

            InternalRuntimeCommand::GetSourceHead { subnet_id, sender } => {
                info!("Source head certificate has been requested for subnet id: {subnet_id}");

                if let Err(error) = self
                    .api_event_sender
                    .send(RuntimeEvent::GetSourceHead { subnet_id, sender })
                    .await
                {
                    error!(
                        %error,
                        "Can't request source head certificate, receiver is dropped"
                    );
                }
            }
        }
    }
}
