//! implementation of Topos Reliable Broadcast to be used in the Transmission Control Engine (TCE)
//!
//! Abstracted from actual transport implementation.
//! Abstracted from actual storage implementation.
//!
use sampler::SampleType;
use thiserror::Error;
use tokio::spawn;
use tokio_stream::wrappers::BroadcastStream;

use futures::{Future, Stream, TryStreamExt};
#[allow(unused)]
use opentelemetry::global;
use tokio::sync::mpsc::Sender;
use tokio::sync::{broadcast, mpsc, oneshot};

use double_echo::DoubleEcho;
use tce_transport::{ReliableBroadcastParams, TrbpEvents};

use topos_core::uci::{Certificate, CertificateId, DigestCompressed, SubnetId};
use tracing::info;

use crate::mem_store::TrbMemStore;
use crate::sampler::Sampler;
use crate::trb_store::TrbStore;
pub use topos_core::uci;

pub type Peer = String;

pub mod double_echo;
pub mod mem_store;
pub mod mock;
pub mod sampler;
pub mod trb_store;

/// Configuration of TRB implementation
pub struct ReliableBroadcastConfig {
    pub store: Box<dyn TrbStore + Send>,
    pub trbp_params: ReliableBroadcastParams,
    pub my_peer_id: Peer,
}

#[derive(Debug)]
pub enum SamplerCommand {
    PeersChanged {
        peers: Vec<String>,
    },
    ConfirmPeer {
        peer: String,
        sample_type: SampleType,
        sender: oneshot::Sender<Result<(), ()>>,
    },
}

#[derive(Debug)]
pub enum DoubleEchoCommand {
    /// Received G-set message
    Deliver {
        cert: Certificate,
        digest: DigestCompressed,
    },

    /// Entry point for new certificate to submit as initial sender
    Broadcast {
        cert: Certificate,
    },

    // Entry point to broadcast many Certificates
    BroadcastMany {
        certificates: Vec<Certificate>,
    },

    /// When echo reply received
    Echo {
        from_peer: String,
        cert: Certificate,
    },

    /// When ready reply received
    Ready {
        from_peer: String,
        cert: Certificate,
    },
    DeliveredCerts {
        subnet_id: SubnetId,
        limit: u64,
        sender: oneshot::Sender<Result<Vec<Certificate>, Errors>>,
    },
}

/// Thread safe client to the protocol aggregate
#[derive(Clone, Debug)]
pub struct ReliableBroadcastClient {
    #[allow(dead_code)]
    peer_id: String,
    broadcast_commands: mpsc::Sender<DoubleEchoCommand>,
    sampling_commands: mpsc::Sender<SamplerCommand>,
}

impl ReliableBroadcastClient {
    /// Creates new instance of the aggregate and returns proxy to it.
    ///
    /// New client instances to the same aggregate can be cloned from the returned one.
    /// Aggregate is spawned as new task.
    pub fn new(
        config: ReliableBroadcastConfig,
    ) -> (Self, impl Stream<Item = Result<TrbpEvents, ()>>) {
        info!("new(trbp_params: {:?})", &config.trbp_params);

        let peer_id = config.my_peer_id.clone();

        let (sample_view_sender, sample_view_receiver) = mpsc::channel(2048);
        let (sampler_command_sender, command_receiver) = mpsc::channel(2048);
        let (event_sender, event_receiver) = broadcast::channel(2048);

        let sampler = Sampler::new(
            config.trbp_params.clone(),
            command_receiver,
            event_sender.clone(),
            sample_view_sender,
        );

        let (broadcast_commands, command_receiver) = mpsc::channel(2048);

        let double_echo = DoubleEcho::new(
            peer_id.clone(),
            config.trbp_params,
            command_receiver,
            sample_view_receiver,
            event_sender,
            Box::new(TrbMemStore::default()),
        );

        spawn(sampler.run());
        spawn(double_echo.run());

        (
            Self {
                peer_id,
                broadcast_commands,
                sampling_commands: sampler_command_sender,
            },
            BroadcastStream::new(event_receiver).map_err(|_| ()),
        )
    }

    pub fn peer_changed(
        &self,
        peers: Vec<String>,
    ) -> impl Future<Output = Result<(), ()>> + 'static + Send {
        let command_channel = self.get_sampler_channel();
        async move {
            command_channel
                .send(SamplerCommand::PeersChanged { peers })
                .await
                .expect("Unable to send peer changed to sampler");
            Ok(())
        }
    }

    pub async fn add_confirmed_peer_to_sample(&self, sample_type: SampleType, peer: Peer) {
        let (sender, receiver) = oneshot::channel();

        self.sampling_commands
            .send(SamplerCommand::ConfirmPeer {
                peer,
                sample_type,
                sender,
            })
            .await
            .expect("Unable to send confirmation to sample");

        let _ = receiver.await.expect("Sender was dropped");
    }

    /// known peers
    /// todo: move it out somewhere out of here, use DHT to advertise urls of API nodes
    pub async fn known_peers_api_addrs(&self) -> Result<Vec<String>, Errors> {
        // todo
        Ok(vec![])
    }

    /// delivered certificates for given terminal chain after the given certificate
    pub fn delivered_certs(
        &self,
        subnet_id: SubnetId,
        _from_cert_id: CertificateId,
    ) -> impl Future<Output = Result<Vec<Certificate>, Errors>> + 'static + Send {
        let (sender, receiver) = oneshot::channel();

        let broadcast_commands = self.broadcast_commands.clone();

        async move {
            let _ = broadcast_commands
                .send(DoubleEchoCommand::DeliveredCerts {
                    subnet_id,
                    limit: 10,
                    sender,
                })
                .await;

            receiver.await.expect("Sender to be alive")
        }
    }

    pub async fn delivered_certs_ids(
        &self,
        subnet_id: SubnetId,
        from_cert_id: CertificateId,
    ) -> Result<Vec<CertificateId>, Errors> {
        self.delivered_certs(subnet_id, from_cert_id)
            .await
            .map(|mut v| v.iter_mut().map(|c| c.cert_id.clone()).collect())
    }

    pub fn get_sampler_channel(&self) -> Sender<SamplerCommand> {
        self.sampling_commands.clone()
    }

    pub fn get_double_echo_channel(&self) -> Sender<DoubleEchoCommand> {
        self.broadcast_commands.clone()
    }

    pub fn get_command_channels(&self) -> (Sender<SamplerCommand>, Sender<DoubleEchoCommand>) {
        (
            self.sampling_commands.clone(),
            self.broadcast_commands.clone(),
        )
    }

    /// Use to broadcast new certificate to the TCE network
    pub fn broadcast_new_certificate(
        &self,
        certificate: Certificate,
    ) -> impl Future<Output = Result<(), ()>> + 'static + Send {
        let broadcast_commands = self.broadcast_commands.clone();

        async move {
            _ = broadcast_commands
                .send(DoubleEchoCommand::Broadcast { cert: certificate })
                .await;

            Ok(())
        }
    }
}

/// Protocol and technical errors
#[derive(Error, Debug)]
pub enum Errors {
    #[error("Error while sending a DoubleEchoCommand to DoubleEcho: {0:?}")]
    DoubleEchoSend(#[from] mpsc::error::SendError<DoubleEchoCommand>),

    #[error("Error while sending a SamplerCommand to Sampler: {0:?}")]
    SamplerSend(#[from] mpsc::error::SendError<SamplerCommand>),

    #[error("Requested certificate not found")]
    CertificateNotFound,
}
