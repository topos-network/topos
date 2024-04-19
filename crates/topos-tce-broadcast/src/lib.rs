//! Implementation of Topos Reliable Broadcast to be used in the Transmission Control Engine (TCE)
//!
//! This crate is designed to be used as a library in the TCE implementation.
//! It covers the Reliable Broadcast part of the TCE, which is the core of the TCE.
//! It doesn't handle how messages are sent or received, nor how the certificates are stored.
//! It is designed to be used with any transport and storage implementation, relying on the
//! `ProtocolEvents` and `DoubleEchoCommand` to communicate with the transport and storage.
//!
//! The reliable broadcast allows a set of validators to agree on a set of messages in order to
//! reach agreement about the delivery of a certificate.
//!
//! Each certificates need to be broadcast to the network, and each validator needs to
//! receive a threshold of messages from the other validators.
//! The thresholds are defined by the `ReliableBroadcastParams` and correspond to the minimum number of
//! validators who need to agree on one certificate in order to consider it delivered.
//!
//! This crate is responsible for validating and driving the broadcast of every certificates.
//!
//! ## Input
//!
//! The input of the broadcast is a certificate to be broadcast. It can be received from
//! the transport layer, or from the storage layer (from the pending tables).
//!
//! The transport layer can be anything from p2p network to API calls.
//!
//! Other inputs are the messages received from the transport layer, coming from other validators.
//! They're `Echo` and `Ready` signed messages.
//!
//! ## Output
//!
//! The outcome of the broadcast is either a certificate delivered or a failure on the delivery.
//!
//! The implementation is based on the paper: [Topos: A Secure, Trustless, and Decentralized Interoperability Protocol](https://arxiv.org/pdf/2206.03481.pdf)
//!
use crate::event::ProtocolEvents;
use double_echo::DoubleEcho;
use futures::Stream;
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;
use tokio::spawn;
use topos_metrics::{
    DOUBLE_CLIENT_TO_DOUBLE_ECHO_CHANNEL, DOUBLE_ECHO_EVENT_CHANNEL,
    DOUBLE_ECHO_TO_TASK_MANAGER_CHANNEL,
};
// use tokio::sync::mpsc::Sender;
use topos_metrics::channels::mpsc;
use topos_metrics::channels::mpsc::Sender;

use tokio::sync::{broadcast, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use topos_config::tce::broadcast::ReliableBroadcastParams;
use topos_core::types::ValidatorId;
use topos_core::uci::{Certificate, CertificateId};
use topos_crypto::messages::{MessageSigner, Signature};
use topos_tce_storage::types::CertificateDeliveredWithPositions;
use topos_tce_storage::validator::ValidatorStore;
use tracing::{debug, error, Instrument};

pub use topos_core::uci;

pub type Peer = String;

mod constant;
pub mod double_echo;
pub mod event;
pub mod sampler;

pub mod task_manager;

#[cfg(test)]
mod tests;

use crate::sampler::SubscriptionsView;

#[derive(Debug)]
pub enum TaskStatus {
    /// The task finished successfully and broadcast the certificate + received ready
    Success,
    /// The task did not finish successfully and stopped.
    Failure,
}

/// Configuration of TCE implementation
pub struct ReliableBroadcastConfig {
    pub tce_params: ReliableBroadcastParams,
    pub validator_id: ValidatorId,
    pub validators: HashSet<ValidatorId>,
    pub message_signer: Arc<MessageSigner>,
}

#[derive(Debug, Clone)]
pub enum DoubleEchoCommand {
    /// Entry point for new certificate to submit as initial sender
    Broadcast {
        need_gossip: bool,
        cert: Certificate,
        pending_id: u64,
    },

    /// When echo reply received
    Echo {
        validator_id: ValidatorId,
        certificate_id: CertificateId,
        signature: Signature,
    },

    /// When ready reply received
    Ready {
        validator_id: ValidatorId,
        certificate_id: CertificateId,
        signature: Signature,
    },
}

/// Thread safe client to the protocol aggregate
#[derive(Clone, Debug)]
pub struct ReliableBroadcastClient {
    command_sender: Sender<DoubleEchoCommand>,
    pub(crate) double_echo_shutdown_channel: tokio::sync::mpsc::Sender<oneshot::Sender<()>>,
}

impl ReliableBroadcastClient {
    /// Creates new instance of the aggregate and returns proxy to it.
    ///
    /// New client instances to the same aggregate can be cloned from the returned one.
    /// Aggregate is spawned as new task.
    pub async fn new(
        config: ReliableBroadcastConfig,
        validator_store: Arc<ValidatorStore>,
        broadcast_sender: broadcast::Sender<CertificateDeliveredWithPositions>,
    ) -> (Self, impl Stream<Item = ProtocolEvents>) {
        let (event_sender, event_receiver) =
            mpsc::channel(*constant::PROTOCOL_CHANNEL_SIZE, &DOUBLE_ECHO_EVENT_CHANNEL);
        let (command_sender, command_receiver) = mpsc::channel(
            *constant::COMMAND_CHANNEL_SIZE,
            &DOUBLE_CLIENT_TO_DOUBLE_ECHO_CHANNEL,
        );
        let (double_echo_shutdown_channel, double_echo_shutdown_receiver) =
            tokio::sync::mpsc::channel::<oneshot::Sender<()>>(1);

        let (task_manager_message_sender, task_manager_message_receiver) = mpsc::channel(
            *constant::BROADCAST_TASK_MANAGER_CHANNEL_SIZE,
            &DOUBLE_ECHO_TO_TASK_MANAGER_CHANNEL,
        );

        let double_echo = DoubleEcho::new(
            config.tce_params,
            config.validator_id,
            config.message_signer,
            config.validators,
            task_manager_message_sender,
            command_receiver,
            event_sender,
            double_echo_shutdown_receiver,
            validator_store,
            broadcast_sender,
        );

        spawn(
            double_echo
                .run(task_manager_message_receiver)
                .in_current_span(),
        );

        (
            Self {
                command_sender,
                double_echo_shutdown_channel,
            },
            ReceiverStream::new(event_receiver.inner()),
        )
    }

    pub fn get_double_echo_channel(&self) -> Sender<DoubleEchoCommand> {
        self.command_sender.clone()
    }

    pub async fn shutdown(&self) -> Result<(), Errors> {
        debug!("Shutting down reliable broadcast client");
        let (double_echo_sender, double_echo_receiver) = oneshot::channel();
        self.double_echo_shutdown_channel
            .send(double_echo_sender)
            .await
            .map_err(Errors::ShutdownCommunication)?;
        double_echo_receiver.await?;

        Ok(())
    }
}

/// Protocol and technical errors
#[derive(Error, Debug)]
pub enum Errors {
    #[error("Error while sending a DoubleEchoCommand to DoubleEcho: {0:?}")]
    DoubleEchoSend(#[from] Box<mpsc::error::SendError<DoubleEchoCommand>>),

    #[error("Error while waiting for a DoubleEchoCommand response: {0:?}")]
    DoubleEchoRecv(#[from] oneshot::error::RecvError),

    #[error("Requested certificate not found")]
    CertificateNotFound,

    #[error("Requested digest not found for certificate {0:?}")]
    DigestNotFound(CertificateId),

    #[error("Cannot create public address from private key")]
    ProducePublicAddress,

    #[error("Unable to execute shutdown for the reliable broadcast: {0}")]
    ShutdownCommunication(mpsc::error::SendError<oneshot::Sender<()>>),
}
