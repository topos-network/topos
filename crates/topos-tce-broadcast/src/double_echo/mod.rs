//! Everything related to the double_echo implementation
//!
//! ## Messages and roles
//!
//! In order to prevent many non validator's messages to be published on the
//! gossip topics, messages are filtered when the [`DoubleEcho`] is producing events.
//!
//! For `validator` nothing changed, for `fullnode` and `sentry` node, their `Echo` and
//! `Ready` messages are filtered, they still produce `Gossip` messages tho.
//!
//! It doesn't mean that a `fullnode` will stop propagate messages from
//! `validators`, it only prevents a non validator to publish messages that will
//! be ignored by others. `fullnode` still consumes Echo and Ready coming from
//! validators and use those messages to build their state.

use crate::event::ProtocolEvents;
use crate::{DoubleEchoCommand, SubscriptionsView};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use topos_config::tce::broadcast::ReliableBroadcastParams;
use topos_core::{
    types::ValidatorId,
    uci::{Certificate, CertificateId},
};
use topos_crypto::messages::{MessageSigner, Signature};
use topos_tce_storage::store::ReadStore;
use topos_tce_storage::types::CertificateDeliveredWithPositions;
use topos_tce_storage::validator::ValidatorStore;
use tracing::{debug, error, info, warn};

pub mod broadcast_state;

pub struct DoubleEcho {
    /// Channel to receive commands
    command_receiver: mpsc::Receiver<DoubleEchoCommand>,
    /// Channel to send events
    event_sender: mpsc::Sender<ProtocolEvents>,
    /// Channel to receive shutdown signal
    pub(crate) shutdown: mpsc::Receiver<oneshot::Sender<()>>,
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

    pub task_manager_cancellation: CancellationToken,
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
            validators: validators.clone(),
            task_manager_message_sender,
            command_receiver,
            event_sender,
            subscriptions: SubscriptionsView {
                echo: validators.clone(),
                ready: validators.clone(),
                network_size: validators.len(),
            },
            shutdown,
            validator_store,
            broadcast_sender,
            task_manager_cancellation: CancellationToken::new(),
        }
    }

    pub fn spawn_task_manager(
        &mut self,
        task_manager_message_receiver: mpsc::Receiver<DoubleEchoCommand>,
    ) {
        let task_manager = crate::task_manager::TaskManager::new(
            task_manager_message_receiver,
            self.subscriptions.clone(),
            self.event_sender.clone(),
            self.validator_id,
            self.params.clone(),
            self.message_signer.clone(),
            self.validator_store.clone(),
            self.broadcast_sender.clone(),
        );

        tokio::spawn(task_manager.run(self.task_manager_cancellation.child_token()));
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
        task_manager_message_receiver: mpsc::Receiver<DoubleEchoCommand>,
    ) {
        self.spawn_task_manager(task_manager_message_receiver);

        info!("DoubleEcho started");

        let shutdowned: Option<oneshot::Sender<()>> = loop {
            tokio::select! {
                biased;

                shutdown = self.shutdown.recv() => {
                        warn!("Double echo shutdown signal received {:?}", shutdown);
                        self.task_manager_cancellation.cancel();
                        break shutdown;
                },
                Some(command) = self.command_receiver.recv() => {
                    match command {

                        DoubleEchoCommand::Broadcast { need_gossip, cert } => self.broadcast(cert, need_gossip).await,

                        command if self.subscriptions.is_some() => {
                            match command {
                                DoubleEchoCommand::Echo { certificate_id, validator_id, signature } => {
                                    // Check if source is part of known_validators
                                    if !self.validators.contains(&validator_id) {
                                        debug!("ECHO message comes from non-validator: {}", validator_id);
                                        continue;
                                    }

                                    let mut payload = Vec::new();
                                    payload.extend_from_slice(certificate_id.as_array());
                                    payload.extend_from_slice(validator_id.as_bytes());

                                    if let Err(e) = self.message_signer.verify_signature(signature, &payload, validator_id.address()) {
                                        debug!("ECHO message signature cannot be verified from: {}", e);
                                        continue;
                                    }

                                    self.handle_echo(certificate_id, validator_id, signature).await
                                },
                                DoubleEchoCommand::Ready { certificate_id, validator_id, signature } => {
                                    // Check if source is part of known_validators
                                    if !self.validators.contains(&validator_id) {
                                        debug!("READY message comes from non-validator: {}", validator_id);
                                        continue;
                                    }

                                    let mut payload = Vec::new();
                                    payload.extend_from_slice(certificate_id.as_array());
                                    payload.extend_from_slice(validator_id.as_bytes());

                                    if let Err(e) = self.message_signer.verify_signature(signature, &payload, validator_id.address()) {
                                        debug!("READY message signature cannot be verified from: {}", e);
                                        continue;
                                    }

                                    self.handle_ready(certificate_id, validator_id, signature).await
                                },
                                _ => {}
                            }

                        },
                        command => {
                            warn!("Received a command {command:?} while not having a complete sampling");
                        }
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
    /// Called to process new certificate submitted from the API or received on
    /// the gossip p2p layer
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

        match self.validator_store.get_certificate(&cert.id) {
            Ok(Some(_)) => {
                _ = self
                    .event_sender
                    .send(ProtocolEvents::AlreadyDelivered {
                        certificate_id: cert.id,
                    })
                    .await;
            }
            Ok(None) => {
                if self
                    .delivery_state_for_new_cert(cert, origin)
                    .await
                    .is_none()
                {
                    error!("Ill-formed samples");
                    _ = self.event_sender.try_send(ProtocolEvents::Die);
                }
            }
            Err(storage_error) => {
                error!(
                    "Unable to broadcast the Certificate {} due to {:?}",
                    &cert.id, storage_error
                );
            }
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
        certificate_id: CertificateId,
        validator_id: ValidatorId,
        signature: Signature,
    ) {
        match self.validator_store.get_certificate(&certificate_id) {
            Err(storage_error) => error!(
                "Unable to get the Certificate {} due to {:?}",
                &certificate_id, storage_error
            ),
            Ok(Some(_)) => debug!(
                "Certificate {} already delivered, ignoring echo",
                &certificate_id
            ),
            Ok(None) => {
                let _ = self
                    .task_manager_message_sender
                    .send(DoubleEchoCommand::Echo {
                        validator_id,
                        certificate_id,
                        signature,
                    })
                    .await;
            }
        }
    }

    pub async fn handle_ready(
        &mut self,
        certificate_id: CertificateId,
        validator_id: ValidatorId,
        signature: Signature,
    ) {
        match self.validator_store.get_certificate(&certificate_id) {
            Err(storage_error) => error!(
                "Unable to get the Certificate {} due to {:?}",
                &certificate_id, storage_error
            ),
            Ok(Some(_)) => debug!(
                "Certificate {} already delivered, ignoring echo",
                &certificate_id
            ),
            Ok(None) => {
                let _ = self
                    .task_manager_message_sender
                    .send(DoubleEchoCommand::Ready {
                        validator_id,
                        certificate_id,
                        signature,
                    })
                    .await;
            }
        }
    }
}
