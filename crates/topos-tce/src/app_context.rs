#![allow(unused_variables)]
//!
//! Application logic glue
//!
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::{Stream, StreamExt};
use opentelemetry::trace::{FutureExt as TraceFutureExt, TraceContextExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tce_transport::{ProtocolEvents, TceCommands};
use tokio::spawn;
use tokio::sync::{mpsc, oneshot};
use topos_core::api::checkpoints::TargetStreamPosition;
use topos_core::uci::{Certificate, CertificateId, SubnetId};
use topos_p2p::{Client as NetworkClient, Event as NetEvent, RetryPolicy};
use topos_tce_api::RuntimeEvent as ApiEvent;
use topos_tce_api::{RuntimeClient as ApiClient, RuntimeError};
use topos_tce_broadcast::sampler::SampleType;
use topos_tce_broadcast::DoubleEchoCommand;
use topos_tce_broadcast::{ReliableBroadcastClient, SamplerCommand};
use topos_tce_gatekeeper::{GatekeeperClient, GatekeeperError};
use topos_tce_storage::errors::{InternalStorageError, StorageError};
use topos_tce_storage::events::StorageEvent;
use topos_tce_storage::StorageClient;
use topos_tce_synchronizer::{SynchronizerClient, SynchronizerEvent};
use topos_telemetry::PropagationContext;
use tracing::{debug, error, info, info_span, trace, warn, warn_span, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::events::Events;

/// Top-level transducer main app context & driver (alike)
///
/// Implements <...Host> traits for network and Api, listens for protocol events in events
/// (store is not active component).
///
/// In the end we shall come to design where this struct receives
/// config+data as input and runs app returning data as output
///
pub struct AppContext {
    pub events: mpsc::Sender<Events>,
    pub tce_cli: ReliableBroadcastClient,
    pub network_client: NetworkClient,
    pub api_client: ApiClient,
    pub pending_storage: StorageClient,
    pub gatekeeper: GatekeeperClient,
    pub synchronizer: SynchronizerClient,
}

impl AppContext {
    // Default previous certificate id for first certificate in the subnet
    // TODO: Remove, it will be genesis certificate id retrieved from Topos Subnet
    const DUMMY_INITIAL_CERTIFICATE_ID: CertificateId =
        CertificateId::from_array([0u8; topos_core::uci::CERTIFICATE_ID_LENGTH]);

    /// Factory
    pub fn new(
        pending_storage: StorageClient,
        tce_cli: ReliableBroadcastClient,
        network_client: NetworkClient,
        api_client: ApiClient,
        gatekeeper: GatekeeperClient,
        synchronizer: SynchronizerClient,
    ) -> (Self, mpsc::Receiver<Events>) {
        let (events, receiver) = mpsc::channel(100);
        (
            Self {
                events,
                tce_cli,
                network_client,
                api_client,
                pending_storage,
                gatekeeper,
                synchronizer,
            },
            receiver,
        )
    }

    /// Main processing loop
    pub async fn run(
        mut self,
        mut network_stream: impl Stream<Item = NetEvent> + Unpin,
        mut tce_stream: impl Stream<Item = Result<ProtocolEvents, ()>> + Unpin,
        mut api_stream: impl Stream<Item = ApiEvent> + Unpin,
        mut storage_stream: impl Stream<Item = StorageEvent> + Unpin,
        mut synchronizer_stream: impl Stream<Item = SynchronizerEvent> + Unpin,
        mut shutdown: mpsc::Receiver<oneshot::Sender<()>>,
    ) {
        loop {
            tokio::select! {

                // protocol
                Some(Ok(evt)) = tce_stream.next() => {
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

                // Storage events
                Some(_event) = storage_stream.next() => {
                }

                // Synchronizer events
                Some(_event) = synchronizer_stream.next() => {
                }

                // Shutdown signal
                Some(sender) = shutdown.recv() => {
                    info!("Shutting down TCE app context...");
                    if let Err(e) = self.shutdown().await {
                        error!("Error shutting down TCE app context: {e}");
                    }
                    // Send feedback that shutdown has been finished
                    _ = sender.send(());
                    break;
                }
            }
        }
        warn!("Exiting main TCE app processing loop")
    }

    async fn on_api_event(&mut self, event: ApiEvent) {
        match event {
            ApiEvent::CertificateSubmitted {
                certificate,
                sender,
                ctx,
            } => {
                let span = info_span!(parent: &ctx, "TCE Runtime");

                _ = self
                    .tce_cli
                    .broadcast_new_certificate(*certificate)
                    .with_context(span.context())
                    .instrument(span)
                    .await;

                _ = sender.send(Ok(()));
            }

            ApiEvent::PeerListPushed { peers, sender } => {
                let sampler = self.tce_cli.clone();
                let gatekeeper = self.gatekeeper.clone();

                spawn(async move {
                    match gatekeeper.push_peer_list(peers).await {
                        Ok(peers) => {
                            info!("Gatekeeper has detected changes on the peer list, new sample in creation");
                            if sampler.peer_changed(peers).await.is_err() {
                                _ = sender.send(Err(RuntimeError::UnableToPushPeerList));
                            } else {
                                _ = sender.send(Ok(()));
                            }
                        }
                        Err(GatekeeperError::NoUpdate) => {
                            _ = sender.send(Ok(()));
                        }
                        Err(_) => {
                            _ = sender.send(Err(RuntimeError::UnableToPushPeerList));
                        }
                    }
                });
            }

            ApiEvent::GetSourceHead { subnet_id, sender } => {
                // Get source head certificate
                let mut result = self
                    .pending_storage
                    .get_source_head(subnet_id)
                    .await
                    .map_err(|e| match e {
                        StorageError::InternalStorage(internal) => {
                            if let InternalStorageError::MissingHeadForSubnet(subnet_id) = internal
                            {
                                RuntimeError::UnknownSubnet(subnet_id)
                            } else {
                                RuntimeError::UnableToGetSourceHead(subnet_id, internal.to_string())
                            }
                        }
                        e => RuntimeError::UnableToGetSourceHead(subnet_id, e.to_string()),
                    });

                // TODO: Initial genesis certificate eventually will be fetched from the topos subnet
                // Currently, for subnet starting from scratch there are no certificates in the database
                // So for MissingHeadForSubnet error we will return some default dummy certificate
                if let Err(RuntimeError::UnknownSubnet(subnet_id)) = result {
                    warn!("Returning dummy certificate as head certificate, to be fixed...");
                    result = Ok((
                        0,
                        topos_core::uci::Certificate {
                            prev_id: AppContext::DUMMY_INITIAL_CERTIFICATE_ID,
                            source_subnet_id: subnet_id,
                            state_root: Default::default(),
                            tx_root_hash: Default::default(),
                            target_subnets: vec![],
                            verifier: 0,
                            id: AppContext::DUMMY_INITIAL_CERTIFICATE_ID,
                            proof: Default::default(),
                            signature: Default::default(),
                        },
                    ));
                };

                _ = sender.send(result);
            }

            ApiEvent::GetLastPendingCertificates {
                mut subnet_ids,
                sender,
            } => {
                let mut last_pending_certificates: HashMap<SubnetId, Option<Certificate>> =
                    subnet_ids
                        .iter()
                        .map(|subnet_id| (*subnet_id, None))
                        .collect();

                if let Ok(pending_certificates) =
                    self.pending_storage.get_pending_certificates().await
                {
                    // Iterate through pending certificates and determine last one for every subnet
                    // Last certificate in the subnet should be one with the highest index
                    for (pending_certificate_id, cert) in pending_certificates.into_iter().rev() {
                        if let Some(subnet_id) = subnet_ids.take(&cert.source_subnet_id) {
                            *last_pending_certificates.entry(subnet_id).or_insert(None) =
                                Some(cert);
                        }
                        if subnet_ids.is_empty() {
                            break;
                        }
                    }
                }

                // Add None pending certificate for any other requested subnet_id
                subnet_ids.iter().for_each(|subnet_id| {
                    last_pending_certificates.insert(*subnet_id, None);
                });

                _ = sender.send(Ok(last_pending_certificates));
            }
        }
    }

    async fn on_protocol_event(&mut self, evt: ProtocolEvents) {
        match evt {
            ProtocolEvents::StableSample(peers) => {
                info!("Stable Sample detected");
                self.api_client.set_active_sample(true).await;
                if self.events.send(Events::StableSample(peers)).await.is_err() {
                    error!("Unable to send StableSample event");
                }
            }

            ProtocolEvents::Broadcast { certificate_id } => {
                info!("Broadcasting certificate {}", certificate_id);
            }

            ProtocolEvents::CertificateDelivered { certificate } => {
                warn!("Certificate delivered {}", certificate.id);
                let storage = self.pending_storage.clone();
                let api_client = self.api_client.clone();

                spawn(async move {
                    match storage.certificate_delivered(certificate.id).await {
                        Ok(positions) => {
                            let certificate_id = certificate.id;
                            api_client
                                .dispatch_certificate(
                                    certificate,
                                    positions
                                        .targets
                                        .into_iter()
                                        .map(|(subnet_id, certificate_target_stream_position)| {
                                            (
                                                subnet_id,
                                                TargetStreamPosition {
                                                    target_subnet_id:
                                                        certificate_target_stream_position
                                                            .target_subnet_id,
                                                    source_subnet_id:
                                                        certificate_target_stream_position
                                                            .source_subnet_id,
                                                    position: certificate_target_stream_position
                                                        .position
                                                        .0,
                                                    certificate_id: Some(certificate_id),
                                                },
                                            )
                                        })
                                        .collect::<HashMap<SubnetId, TargetStreamPosition>>(),
                                )
                                .await;
                        }
                        Err(StorageError::InternalStorage(
                            InternalStorageError::CertificateNotFound(_),
                        )) => {}
                        Err(e) => {
                            error!("Pending storage error while delivering certificate: {e}");
                        }
                    };
                });
            }

            ProtocolEvents::EchoSubscribeReq { peers } => {
                // Preparing echo subscribe message
                let my_peer_id = self.network_client.local_peer_id;
                let data = NetworkMessage::from(TceCommands::OnEchoSubscribeReq {
                    from_peer: self.network_client.local_peer_id,
                });
                let command_sender = self.tce_cli.get_sampler_channel();

                // Sending echo subscribe message to all remote peers
                let future_pool: FuturesUnordered<_> = peers
                    .iter()
                    .map(|peer_id| {
                        debug!(
                            "peer_id: {} sending echo subscribe to {}",
                            &my_peer_id, &peer_id
                        );
                        let peer_id = *peer_id;
                        self.network_client
                            .send_request::<_, NetworkMessage>(
                                peer_id,
                                data.clone(),
                                RetryPolicy::N(3),
                            )
                            .map(move |result| (peer_id, result))
                    })
                    .collect();

                spawn(async move {
                    // Waiting for all responses from remote peers on our echo subscription request
                    let results: Vec<_> = future_pool.collect().await;

                    // Process responses
                    for (peer_id, result) in results {
                        match result {
                            Ok(message) => match message {
                                // Remote peer has replied us that he is accepting us as echo subscriber
                                NetworkMessage::Cmd(TceCommands::OnEchoSubscribeOk {
                                    from_peer,
                                }) => {
                                    debug!("Receive response to EchoSubscribe",);
                                    let (sender, receiver) = oneshot::channel();
                                    let _ = command_sender
                                        .send(SamplerCommand::ConfirmPeer {
                                            peer: from_peer,
                                            sample_type: SampleType::EchoSubscription,
                                            sender,
                                        })
                                        .await;

                                    let _ = receiver.await.expect("Sender was dropped");
                                }
                                msg => {
                                    error!("Receive an unexpected message as a response {msg:?}");
                                }
                            },
                            Err(error) => {
                                error!("An error occurred when sending EchoSubscribe {error:?} for peer {peer_id}");
                            }
                        }
                    }
                });
            }
            ProtocolEvents::ReadySubscribeReq { peers } => {
                // Preparing ready subscribe message
                let my_peer_id = self.network_client.local_peer_id;
                let data = NetworkMessage::from(TceCommands::OnReadySubscribeReq {
                    from_peer: self.network_client.local_peer_id,
                });
                let command_sender = self.tce_cli.get_sampler_channel();
                // Sending ready subscribe message to send to a number of remote peers
                let future_pool: FuturesUnordered<_> = peers
                    .into_iter()
                    .map(|peer_id| {
                        debug!(
                            "peer_id: {} sending ready subscribe to {}",
                            &my_peer_id, &peer_id
                        );
                        self.network_client
                            .send_request::<_, NetworkMessage>(
                                peer_id,
                                data.clone(),
                                RetryPolicy::N(3),
                            )
                            .map(move |result| (peer_id, result))
                    })
                    .collect();

                spawn(async move {
                    // Waiting for all responses from remote peers on our ready subscription request
                    let results: Vec<_> = future_pool.collect().await;

                    // Process responses from remote peers
                    for (peer_id, result) in results {
                        match result {
                            Ok(message) => match message {
                                // Remote peer has replied us that he is accepting us as ready subscriber
                                NetworkMessage::Cmd(TceCommands::OnReadySubscribeOk {
                                    from_peer,
                                }) => {
                                    debug!("Receive response to ReadySubscribe");
                                    let (sender_ready, receiver_ready) = oneshot::channel();
                                    let _ = command_sender
                                        .send(SamplerCommand::ConfirmPeer {
                                            peer: from_peer,
                                            sample_type: SampleType::ReadySubscription,
                                            sender: sender_ready,
                                        })
                                        .await;
                                    let (sender_delivery, receiver_delivery) = oneshot::channel();
                                    let _ = command_sender
                                        .send(SamplerCommand::ConfirmPeer {
                                            peer: from_peer,
                                            sample_type: SampleType::DeliverySubscription,
                                            sender: sender_delivery,
                                        })
                                        .await;

                                    join_all(vec![receiver_ready, receiver_delivery]).await;
                                }
                                msg => {
                                    error!("Receive an unexpected message as a response {msg:?}");
                                }
                            },
                            Err(error) => {
                                error!("An error occurred when sending ReadySubscribe {error:?} for peer {peer_id}");
                            }
                        }
                    }
                });
            }

            ProtocolEvents::Gossip { peers, cert, ctx } => {
                let span = info_span!(
                    parent: &ctx,
                    "SEND Outbound Gossip",
                    peer_id = self.network_client.local_peer_id.to_string(),
                    "otel.kind" = "producer",
                );
                let cert_id = cert.id;

                let data = NetworkMessage::from(TceCommands::OnGossip {
                    cert,
                    ctx: PropagationContext::inject(&span.context()),
                });

                let future_pool: FuturesUnordered<_> = peers
                    .iter()
                    .map(|peer_id| {
                        debug!(
                            "peer_id: {} sending gossip cert id: {} to peer {}",
                            &self.network_client.local_peer_id, &cert_id, &peer_id
                        );
                        self.network_client
                            .send_request::<_, NetworkMessage>(
                                *peer_id,
                                data.clone(),
                                RetryPolicy::N(3),
                            )
                            .instrument(span.clone())
                            .with_context(span.context())
                    })
                    .collect();

                spawn(async move {
                    let results: Vec<_> = future_pool
                        .collect()
                        .instrument(span.clone())
                        .with_context(span.context())
                        .await;
                });
            }

            ProtocolEvents::Echo {
                peers,
                certificate_id,
                ctx,
            } => {
                let span = info_span!(
                    parent: &ctx,
                    "SEND Outbound Echo",
                    peer_id = self.network_client.local_peer_id.to_string(),
                    "otel.kind" = "producer",
                );
                let my_peer_id = self.network_client.local_peer_id;
                // Send echo message
                let data = NetworkMessage::from(TceCommands::OnEcho {
                    from_peer: self.network_client.local_peer_id,
                    certificate_id,
                    ctx: PropagationContext::inject(&span.context()),
                });

                let future_pool: FuturesUnordered<_> = peers
                    .iter()
                    .map(|peer_id| {
                        debug!("Peer {} is sending Echo to {}", &my_peer_id, &peer_id);
                        self.network_client
                            .send_request::<_, NetworkMessage>(
                                *peer_id,
                                data.clone(),
                                RetryPolicy::N(3),
                            )
                            .instrument(span.clone())
                            .with_context(span.context())
                    })
                    .collect();

                spawn(async move {
                    let _results: Vec<_> = future_pool
                        .collect()
                        .instrument(span.clone())
                        .with_context(span.context())
                        .await;
                });
            }

            ProtocolEvents::Ready {
                peers,
                certificate_id,
                ctx,
            } => {
                let span = info_span!(
                    parent: &ctx,
                    "SEND Outbound Ready",
                    peer_id = self.network_client.local_peer_id.to_string(),
                    "otel.kind" = "producer",
                );
                let my_peer_id = self.network_client.local_peer_id;
                let data = NetworkMessage::from(TceCommands::OnReady {
                    from_peer: self.network_client.local_peer_id,
                    certificate_id,
                    ctx: PropagationContext::inject(&span.context()),
                });

                let future_pool: FuturesUnordered<_> = peers
                    .iter()
                    .map(|peer_id| {
                        debug!("Peer {} is sending Ready to {}", &my_peer_id, &peer_id);
                        self.network_client
                            .send_request::<_, NetworkMessage>(
                                *peer_id,
                                data.clone(),
                                RetryPolicy::N(3),
                            )
                            .instrument(span.clone())
                            .with_context(span.context())
                    })
                    .collect();

                spawn(async move {
                    let _results: Vec<_> = future_pool
                        .collect()
                        .instrument(span.clone())
                        .with_context(span.context().clone())
                        .await;
                });
            }

            evt => {
                debug!("Unhandled event: {:?}", evt);
            }
        }
    }

    async fn on_net_event(&mut self, evt: NetEvent) {
        trace!(
            "on_net_event: peer: {} event {:?}",
            &self.network_client.local_peer_id,
            &evt
        );

        match evt {
            NetEvent::PeersChanged { .. } => {}

            NetEvent::TransmissionOnReq {
                from,
                data,
                channel,
                ..
            } => {
                let my_peer = self.network_client.local_peer_id;
                let msg: NetworkMessage = data.into();
                match msg {
                    NetworkMessage::NotReady(_) => {}
                    NetworkMessage::Cmd(cmd) => {
                        match cmd {
                            // We received echo subscription request from external peer
                            TceCommands::OnEchoSubscribeReq { from_peer } => {
                                debug!(
                                    sender = from.to_string(),
                                    "on_net_event peer {} TceCommands::OnEchoSubscribeReq from_peer: {}",
                                    &self.network_client.local_peer_id, &from_peer
                                );
                                self.tce_cli
                                    .add_confirmed_peer_to_sample(
                                        SampleType::EchoSubscriber,
                                        from_peer,
                                    )
                                    .await;

                                // We are responding that we are accepting echo subscriber
                                spawn(self.network_client.respond_to_request(
                                    NetworkMessage::from(TceCommands::OnEchoSubscribeOk {
                                        from_peer: my_peer,
                                    }),
                                    channel,
                                ));
                            }

                            // We received ready subscription request from external peer
                            TceCommands::OnReadySubscribeReq { from_peer } => {
                                debug!(
                                    sender = from.to_string(),
                                    "peer_id {} on_net_event TceCommands::OnReadySubscribeReq from_peer: {}",
                                    &self.network_client.local_peer_id, &from_peer,
                                );
                                self.tce_cli
                                    .add_confirmed_peer_to_sample(
                                        SampleType::ReadySubscriber,
                                        from_peer,
                                    )
                                    .await;
                                // We are responding that we are accepting ready subscriber
                                spawn(self.network_client.respond_to_request(
                                    NetworkMessage::from(TceCommands::OnReadySubscribeOk {
                                        from_peer: my_peer,
                                    }),
                                    channel,
                                ));
                            }

                            TceCommands::OnGossip {
                                cert,
                                ctx,
                                // root_ctx,
                            } => {
                                // let network_span_ctx = SpanToContext::to_context(
                                let span = warn_span!(
                                    "RECV Outbound Gossip",
                                    peer_id = self.network_client.local_peer_id.to_string(),
                                    "otel.kind" = "consumer",
                                    sender = from.to_string()
                                );
                                let parent = ctx.extract();
                                span.add_link(parent.span().span_context().clone());

                                // &ctx,
                                // );

                                _ = self.tce_cli.broadcast_new_certificate(cert.clone()).await;

                                _ = self
                                    .network_client
                                    .respond_to_request(
                                        NetworkMessage::from(TceCommands::OnDoubleEchoOk {
                                            from_peer: my_peer,
                                        }),
                                        channel,
                                    )
                                    .await;
                            }
                            TceCommands::OnEcho {
                                from_peer,
                                certificate_id,
                                ctx,
                            } => {
                                let span = warn_span!(
                                    "RECV Outbound Echo",
                                    peer_id = self.network_client.local_peer_id.to_string(),
                                    "otel.kind" = "consumer",
                                    sender = from.to_string()
                                );
                                let context = ctx.extract();
                                span.add_link(context.span().span_context().clone());

                                _ = self
                                    .network_client
                                    .respond_to_request(
                                        NetworkMessage::from(TceCommands::OnDoubleEchoOk {
                                            from_peer: my_peer,
                                        }),
                                        channel,
                                    )
                                    .await;
                                self.tce_cli
                                    .get_double_echo_channel()
                                    .send(DoubleEchoCommand::Echo {
                                        from_peer,
                                        certificate_id,
                                        ctx: span.clone(),
                                    })
                                    .with_context(span.context().clone())
                                    .instrument(span)
                                    .await
                                    .expect("Receive the Echo");
                            }
                            TceCommands::OnReady {
                                from_peer,
                                certificate_id,
                                ctx,
                            } => {
                                let span = warn_span!(
                                    "RECV Outbound Ready",
                                    peer_id = self.network_client.local_peer_id.to_string(),
                                    "otel.kind" = "consumer",
                                    sender = from.to_string()
                                );
                                let context = ctx.extract();
                                span.add_link(context.span().span_context().clone());

                                _ = self
                                    .network_client
                                    .respond_to_request(
                                        NetworkMessage::from(TceCommands::OnDoubleEchoOk {
                                            from_peer: my_peer,
                                        }),
                                        channel,
                                    )
                                    .await;
                                self.tce_cli
                                    .get_double_echo_channel()
                                    .send(DoubleEchoCommand::Ready {
                                        from_peer,
                                        certificate_id,
                                        ctx: span.clone(),
                                    })
                                    .with_context(context)
                                    .instrument(span)
                                    .await
                                    .expect("Receive the Ready");
                            }
                            _ => todo!(),
                        }
                    }
                }
            }
            _ => {}
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Shutting down the TCE client...");
        self.api_client.shutdown().await?;
        self.synchronizer.shutdown().await?;
        self.pending_storage.shutdown().await?;
        self.tce_cli.shutdown().await?;
        self.gatekeeper.shutdown().await?;
        self.network_client.shutdown().await?;

        Ok(())
    }
}

/// Definition of networking payload.
///
/// We assume that only Commands will go through the network,
/// [Response] is used to allow reporting of logic errors to the caller.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
enum NetworkMessage {
    Cmd(TceCommands),

    NotReady(topos_p2p::NotReadyMessage),
}

// deserializer
impl From<Vec<u8>> for NetworkMessage {
    fn from(data: Vec<u8>) -> Self {
        bincode::deserialize::<NetworkMessage>(data.as_ref()).expect("msg deser")
    }
}

// serializer
impl From<NetworkMessage> for Vec<u8> {
    fn from(msg: NetworkMessage) -> Self {
        bincode::serialize::<NetworkMessage>(&msg).expect("msg ser")
    }
}

// transformer of protocol commands into network commands
impl From<TceCommands> for NetworkMessage {
    fn from(cmd: TceCommands) -> Self {
        Self::Cmd(cmd)
    }
}
