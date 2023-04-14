use futures::{stream::FuturesUnordered, StreamExt};
use opentelemetry::{
    trace::{FutureExt, TraceContextExt},
    Context,
};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    future,
    pin::Pin,
    time::Duration,
};
use tokio::{
    spawn,
    sync::mpsc::{self, Receiver, Sender},
    sync::oneshot,
    task::JoinHandle,
};
use tonic_health::server::HealthReporter;
use topos_core::api::checkpoints::TargetStreamPosition;
use topos_core::api::tce::v1::api_service_server::ApiServiceServer;
use topos_core::uci::{Certificate, SubnetId};
use topos_tce_storage::{
    CertificateTargetStreamPosition, FetchCertificatesFilter, FetchCertificatesPosition,
    StorageClient,
};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use tracing::{debug, error, info, info_span, Instrument, Span};
use uuid::Uuid;

use crate::{
    grpc::TceGrpcService,
    stream::{StreamCommand, StreamError, StreamErrorKind},
};

pub(crate) mod builder;
mod client;
mod commands;
pub mod error;
mod events;

#[cfg(test)]
mod tests;

pub use client::RuntimeClient;

use self::builder::RuntimeBuilder;
pub(crate) use self::commands::InternalRuntimeCommand;

pub use self::commands::RuntimeCommand;
pub use self::events::RuntimeEvent;

pub(crate) type Streams =
    FuturesUnordered<Pin<Box<dyn future::Future<Output = Result<Uuid, StreamError>> + Send>>>;

pub struct Runtime {
    pub(crate) sync_tasks: HashMap<Uuid, JoinHandle<()>>,

    pub(crate) storage: StorageClient,
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
        let mut health_update = tokio::time::interval(Duration::from_secs(1));
        let shutdowned: Option<oneshot::Sender<()>> = loop {
            tokio::select! {
                shutdown = self.shutdown.recv() => {
                    break shutdown;
                },
                _ = health_update.tick() => {
                    self.health_reporter.set_serving::<ApiServiceServer<TceGrpcService>>().await;
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
                    certificate.id
                );
                // Collect target subnets from certificate cross chain transaction list
                let target_subnets = certificate.target_subnets.iter().collect::<HashSet<_>>();
                debug!(
                    "Dispatching certificate cert_id: {:?} to target subnets: {:?}",
                    &certificate.id, target_subnets
                );
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
                                        "Invalid target stream position for cert id {}, \
                                        target subnet id {target_subnet_id}, dispatch failed",
                                        &certificate.id
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

                if let Some(task) = self.sync_tasks.get(&stream_id) {
                    task.abort();
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

                    // TODO: Refactor this using a better handle, FuturesUnordered + Killswitch
                    let task = spawn(async move {
                        info!("Sync task started for sequencer stream {}", stream_id);
                        let mut collector: Vec<(Certificate, FetchCertificatesPosition)> =
                            Vec::new();

                        for (target_subnet_id, mut source) in target_subnet_stream_positions {
                            let source_subnet_list = storage.targeted_by(target_subnet_id).await;

                            info!(
                                "Sequencer sync task detected {:?} as source list",
                                source_subnet_list
                            );
                            if let Ok(source_subnet_list) = source_subnet_list {
                                for source_subnet_id in source_subnet_list {
                                    if let Entry::Vacant(entry) = source.entry(source_subnet_id) {
                                        entry.insert(TargetStreamPosition {
                                            target_subnet_id,
                                            source_subnet_id,
                                            position: 0,
                                            certificate_id: None,
                                        });
                                    }
                                }
                            }

                            for (
                                _,
                                TargetStreamPosition {
                                    target_subnet_id,
                                    source_subnet_id,
                                    position,
                                    ..
                                },
                            ) in source
                            {
                                if let Ok(certificates_with_positions) = storage
                                    .fetch_certificates(FetchCertificatesFilter::Target {
                                        target_stream_position: CertificateTargetStreamPosition {
                                            target_subnet_id,
                                            source_subnet_id,
                                            position: topos_tce_storage::Position(position),
                                        },
                                        limit: 100,
                                    })
                                    .await
                                {
                                    collector.extend(certificates_with_positions)
                                }
                            }
                        }

                        for (certificate, position) in collector {
                            info!(
                                "Sequencer sync task for {} is sending {}",
                                stream_id, certificate.id
                            );
                            // TODO: catch error on send
                            if let FetchCertificatesPosition::Target(
                                CertificateTargetStreamPosition {
                                    target_subnet_id,
                                    source_subnet_id,
                                    position,
                                },
                            ) = position
                            {
                                _ = notifier
                                    .send(StreamCommand::PushCertificate {
                                        positions: vec![TargetStreamPosition {
                                            target_subnet_id,
                                            source_subnet_id,
                                            position: position.0,
                                            certificate_id: Some(certificate.id),
                                        }],
                                        certificate,
                                    })
                                    .await;
                            } else {
                                error!("Invalid certificate position fetched");
                            }
                        }
                    });

                    self.sync_tasks.insert(stream_id, task);
                }
            }

            InternalRuntimeCommand::CertificateSubmitted {
                certificate,
                sender,
                ctx,
            } => {
                let span = info_span!("TCE API Runtime",);
                span.set_parent(ctx);

                async move {
                    tracing::warn!(span_span_id = ?Span::current().context().span().span_context().span_id());
                    tracing::warn!(cx_span_id = ?Context::current().span().span_context().span_id());

                    info!(
                        "A certificate has been submitted to the TCE {}",
                        certificate.id
                    );
                    if let Err(error) = self
                        .api_event_sender
                        .send(RuntimeEvent::CertificateSubmitted {
                            certificate,
                            sender,
                            ctx: Span::current().context(),
                        })
                        .with_current_context()
                        .instrument(Span::current())
                        .await
                    {
                        error!(
                            %error,
                            "Can't send certificate submission to runtime, receiver is dropped"
                        );
                    }
                }
                .with_context(span.context())
                .instrument(span)
                .await
            }

            InternalRuntimeCommand::PushPeerList { peers, sender } => {
                // debug!("A peer list has been pushed {:?}", peers);

                if let Err(error) = self
                    .api_event_sender
                    .send(RuntimeEvent::PeerListPushed { peers, sender })
                    .await
                {
                    error!(
                        %error,
                        "Can't send new peer list to runtime, receiver is dropped"
                    );
                }
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

            InternalRuntimeCommand::GetLastPendingCertificates { subnet_ids, sender } => {
                info!("Last pending certificate has been requested for subnet ids: {subnet_ids:?}");

                let subnet_ids: HashSet<SubnetId> = subnet_ids.into_iter().collect();
                if let Err(error) = self
                    .api_event_sender
                    .send(RuntimeEvent::GetLastPendingCertificates { subnet_ids, sender })
                    .await
                {
                    error!(
                        %error,
                        "Can't request last pending certificates, receiver is dropped"
                    );
                }
            }
        }
    }
}
