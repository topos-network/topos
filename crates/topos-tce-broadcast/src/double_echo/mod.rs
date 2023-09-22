use crate::TaskStatus;
use crate::{DoubleEchoCommand, SubscriptionsView};
use std::collections::HashSet;
use std::sync::Arc;
use tce_transport::{ProtocolEvents, ReliableBroadcastParams, ValidatorId};
use tokio::sync::{broadcast, mpsc, oneshot};
use topos_core::uci::{Certificate, CertificateId};
use topos_crypto::messages::{MessageSigner, Signature};
use topos_p2p::PeerId;
use topos_tce_storage::types::CertificateDeliveredWithPositions;
use topos_tce_storage::validator::ValidatorStore;
use tracing::{error, info, warn};

pub mod broadcast_state;

pub struct DoubleEcho {
    /// Channel to receive commands
    command_receiver: mpsc::Receiver<DoubleEchoCommand>,
    /// Channel to send events
    event_sender: mpsc::Sender<ProtocolEvents>,
    /// Channel to receive shutdown signal
    pub(crate) shutdown: mpsc::Receiver<oneshot::Sender<()>>,
    /// Delivered certificate ids to avoid processing twice the same certificate
    delivered_certificates: HashSet<CertificateId>,
    /// The threshold parameters for the double echo
    pub params: ReliableBroadcastParams,
    /// The connection to the TaskManager to forward DoubleEchoCommand messages
    task_manager_message_sender: mpsc::Sender<DoubleEchoCommand>,
    /// The overview of the network, which holds echo and ready subscriptions and the network size
    pub subscriptions: SubscriptionsView,
    /// Local node ValidatorId
    pub validator_id: ValidatorId,
    /// Keypair to sign and verify ECHO and READY messages
    pub message_signer: Arc<MessageSigner>,
    /// List of approved validators through smart contract and/or genesis
    pub validators: HashSet<ValidatorId>,
    pub validator_store: Arc<ValidatorStore>,
    pub broadcast_sender: broadcast::Sender<CertificateDeliveredWithPositions>,
}

impl DoubleEcho {
    pub const MAX_BUFFER_SIZE: usize = 2048;

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        params: ReliableBroadcastParams,
        validator_id: ValidatorId,
        message_signer: Arc<MessageSigner>,
        validators: HashSet<ValidatorId>,
        task_manager_message_sender: mpsc::Sender<DoubleEchoCommand>,
        command_receiver: mpsc::Receiver<DoubleEchoCommand>,
        event_sender: mpsc::Sender<ProtocolEvents>,
        shutdown: mpsc::Receiver<oneshot::Sender<()>>,
        validator_store: Arc<ValidatorStore>,
        broadcast_sender: broadcast::Sender<CertificateDeliveredWithPositions>,
    ) -> Self {
        Self {
            params,
            validator_id,
            message_signer,
            validators,
            task_manager_message_sender,
            command_receiver,
            event_sender,
            subscriptions: SubscriptionsView::default(),
            shutdown,
            delivered_certificates: Default::default(),
            validator_store,
            broadcast_sender,
        }
    }

    #[cfg(not(feature = "task-manager-channels"))]
    pub fn spawn_task_manager(
        &mut self,
        subscriptions_view_receiver: mpsc::Receiver<SubscriptionsView>,
        task_manager_message_receiver: mpsc::Receiver<DoubleEchoCommand>,
    ) -> mpsc::Receiver<(CertificateId, TaskStatus)> {
        let (task_completion_sender, task_completion_receiver) = mpsc::channel(2048);

        let (task_manager, shutdown_receiver) = crate::task_manager_futures::TaskManager::new(
            task_manager_message_receiver,
            task_completion_sender,
            subscriptions_view_receiver,
            self.event_sender.clone(),
            self.validator_id,
            self.params.clone(),
            self.message_signer.clone(),
            self.validator_store.clone(),
            self.broadcast_sender.clone(),
        );

        tokio::spawn(task_manager.run(shutdown_receiver));

        task_completion_receiver
    }

    #[cfg(feature = "task-manager-channels")]
    pub fn spawn_task_manager(
        &mut self,
        subscriptions_view_receiver: mpsc::Receiver<SubscriptionsView>,
        task_manager_message_receiver: mpsc::Receiver<DoubleEchoCommand>,
    ) -> mpsc::Receiver<(CertificateId, TaskStatus)> {
        let (task_completion_sender, task_completion_receiver) = mpsc::channel(2048);

        let (task_manager, shutdown_receiver) = crate::task_manager_channels::TaskManager::new(
            task_manager_message_receiver,
            task_completion_sender,
            subscriptions_view_receiver,
            self.event_sender.clone(),
            self.validator_id,
            self.message_signer.clone(),
            self.params.clone(),
            self.validator_store.clone(),
        );

        tokio::spawn(task_manager.run(shutdown_receiver));

        task_completion_receiver
    }

    /// DoubleEcho main loop
    ///   - Listen for shutdown signal
    ///   - Read new messages from command_receiver
    ///      - If a new certificate is received, add it to the buffer
    ///      - If a new subscription view is received, update the subscriptions
    ///      - If a new Echo/Ready is received, update the state of the certificate or buffer
    ///      the message
    pub(crate) async fn run(
        mut self,
        mut subscriptions_view_receiver: mpsc::Receiver<SubscriptionsView>,
        task_manager_message_receiver: mpsc::Receiver<DoubleEchoCommand>,
    ) {
        let (forwarding_subscriptions_sender, forwarding_subscriptions_receiver) =
            mpsc::channel(2048);
        let mut task_completion = self.spawn_task_manager(
            forwarding_subscriptions_receiver,
            task_manager_message_receiver,
        );

        info!("DoubleEcho started");

        let shutdowned: Option<oneshot::Sender<()>> = loop {
            tokio::select! {
                biased;

                Some(new_subscriptions_view) = subscriptions_view_receiver.recv() => {
                    forwarding_subscriptions_sender.send(new_subscriptions_view.clone()).await.unwrap();
                    self.subscriptions = new_subscriptions_view;
                }

                shutdown = self.shutdown.recv() => {
                        warn!("Double echo shutdown signal received {:?}", shutdown);
                        break shutdown;
                },
                Some(command) = self.command_receiver.recv() => {
                    match command {

                        DoubleEchoCommand::Broadcast { need_gossip, cert } => self.broadcast(cert, need_gossip).await,

                        command if self.subscriptions.is_some() => {
                            match command {
                                DoubleEchoCommand::Echo { from_peer, certificate_id, validator_id, signature } => {
                                    // Check if source is part of known_validators
                                    if !self.validators.contains(&validator_id) {
                                        return error!("ECHO message comes from non-validator: {}", validator_id);
                                    }

                                    let mut payload = Vec::new();
                                    payload.extend_from_slice(certificate_id.as_array());
                                    payload.extend_from_slice(validator_id.as_bytes());

                                    if let Err(e) = self.message_signer.verify_signature(signature, &payload, validator_id.address()) {
                                        return error!("ECHO messag signature cannot be verified from: {}", e);
                                    }

                                    self.handle_echo(from_peer, certificate_id, validator_id, signature).await
                                },
                                DoubleEchoCommand::Ready { from_peer, certificate_id, validator_id, signature } => {
                                    // Check if source is part of known_validators
                                    if !self.validators.contains(&validator_id) {
                                        return error!("READY message comes from non-validator: {}", validator_id);
                                    }

                                    let mut payload = Vec::new();
                                    payload.extend_from_slice(certificate_id.as_array());
                                    payload.extend_from_slice(validator_id.as_bytes());

                                    if let Err(e) = self.message_signer.verify_signature(signature, &payload, validator_id.address()) {
                                        return error!("READY message signature cannot be verified from: {}", e);
                                    }

                                    self.handle_ready(from_peer, certificate_id, validator_id, signature).await
                                },
                                _ => {}
                            }

                        },
                        command => {
                            warn!("Received a command {command:?} while not having a complete sampling");
                        }
                    }
                }

                Some((certificate_id, status)) = task_completion.recv() => {
                    if let TaskStatus::Success = status {
                        self.delivered_certificates.insert(certificate_id);
                    }
                }

                else => {
                    warn!("Break the tokio loop for the double echo");
                    break None;
                }
            }
        };

        if let Some(sender) = shutdowned {
            info!("Shutting down p2p double echo...");
            _ = sender.send(());
        } else {
            warn!("Shutting down p2p double echo due to error...");
        }
    }
}

impl DoubleEcho {
    /// Called to process potentially new certificate:
    /// - either submitted from API ( [tce_transport::TceCommands::Broadcast] command)
    /// - or received through the gossip (first step of protocol exchange)
    pub async fn broadcast(&mut self, cert: Certificate, origin: bool) {
        info!("🙌 Starting broadcasting the Certificate {}", &cert.id);
        if self.cert_pre_broadcast_check(&cert).is_err() {
            error!("Failure on the pre-check for the Certificate {}", &cert.id);
            self.event_sender
                .try_send(ProtocolEvents::BroadcastFailed {
                    certificate_id: cert.id,
                })
                .unwrap();
            return;
        }

        if self.delivered_certificates.get(&cert.id).is_some() {
            self.event_sender
                .try_send(ProtocolEvents::AlreadyDelivered {
                    certificate_id: cert.id,
                })
                .unwrap();

            return;
        }

        if self
            .delivery_state_for_new_cert(cert, origin)
            .await
            .is_none()
        {
            error!("Ill-formed samples");
            _ = self.event_sender.try_send(ProtocolEvents::Die);
        }
    }

    /// Build initial delivery state
    async fn delivery_state_for_new_cert(
        &mut self,
        certificate: Certificate,
        origin: bool,
    ) -> Option<bool> {
        let subscriptions = self.subscriptions.clone();

        // Check whether inbound sets are empty
        if subscriptions.echo.is_empty() || subscriptions.ready.is_empty() {
            error!(
                "One Subscription sample is empty: Echo({}), Ready({})",
                subscriptions.echo.is_empty(),
                subscriptions.ready.is_empty(),
            );
            None
        } else {
            _ = self
                .task_manager_message_sender
                .send(DoubleEchoCommand::Broadcast {
                    need_gossip: origin,
                    cert: certificate,
                })
                .await;

            Some(true)
        }
    }

    /// Checks done before starting to broadcast
    fn cert_pre_broadcast_check(&self, cert: &Certificate) -> Result<(), ()> {
        if cert.check_signature().is_err() {
            error!("Error on the signature");
        }

        if cert.check_proof().is_err() {
            error!("Error on the proof");
        }

        Ok(())
    }
}

impl DoubleEcho {
    pub async fn handle_echo(
        &mut self,
        from_peer: PeerId,
        certificate_id: CertificateId,
        validator_id: ValidatorId,
        signature: Signature,
    ) {
        if self.delivered_certificates.get(&certificate_id).is_none() {
            let _ = self
                .task_manager_message_sender
                .send(DoubleEchoCommand::Echo {
                    from_peer,
                    validator_id,
                    certificate_id,
                    signature,
                })
                .await;
        }
    }

    pub async fn handle_ready(
        &mut self,
        from_peer: PeerId,
        certificate_id: CertificateId,
        validator_id: ValidatorId,
        signature: Signature,
    ) {
        if self.delivered_certificates.get(&certificate_id).is_none() {
            let _ = self
                .task_manager_message_sender
                .send(DoubleEchoCommand::Ready {
                    from_peer,
                    validator_id,
                    certificate_id,
                    signature,
                })
                .await;
        }
    }
}
