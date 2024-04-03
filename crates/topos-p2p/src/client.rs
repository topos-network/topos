use futures::TryFutureExt;
use libp2p::PeerId;
use tokio::sync::{
    mpsc::{self, error::SendError},
    oneshot,
};
use tonic::server::NamedService;
use topos_core::api::grpc::GrpcClient;

use crate::{
    error::{CommandExecutionError, P2PError},
    utils::GrpcOverP2P,
    Command,
};

#[derive(Clone)]
pub struct NetworkClient {
    pub retry_ttl: u64,
    pub local_peer_id: PeerId,
    pub sender: mpsc::Sender<Command>,
    pub grpc_over_p2p: GrpcOverP2P,
    pub shutdown_channel: mpsc::Sender<oneshot::Sender<()>>,
}

impl NetworkClient {
    pub async fn connected_peers(&self) -> Result<Vec<PeerId>, P2PError> {
        let (sender, receiver) = oneshot::channel();
        Self::send_command_with_receiver(&self.sender, Command::ConnectedPeers { sender }, receiver)
            .await
    }

    pub async fn random_known_peer(&self) -> Result<PeerId, P2PError> {
        let (sender, receiver) = oneshot::channel();
        Self::send_command_with_receiver(
            &self.sender,
            Command::RandomKnownPeer { sender },
            receiver,
        )
        .await
    }

    pub async fn publish<T: std::fmt::Debug + prost::Message + 'static>(
        &self,
        topic: &'static str,
        message: T,
    ) -> Result<String, P2PError> {
        let network = self.sender.clone();
        let (sender, receiver) = oneshot::channel();

        network
            .send(Command::Gossip {
                topic,
                data: message.encode_to_vec(),
                sender,
            })
            .map_err(CommandExecutionError::from)
            .await?;

        receiver.await?.map(|id| id.to_string())
    }

    async fn send_command_with_receiver<
        T,
        E: From<oneshot::error::RecvError> + From<CommandExecutionError>,
    >(
        sender: &mpsc::Sender<Command>,
        command: Command,
        receiver: oneshot::Receiver<Result<T, E>>,
    ) -> Result<T, E> {
        if let Err(SendError(command)) = sender.send(command).await {
            return Err(CommandExecutionError::UnableToSendCommand(command).into());
        }

        receiver.await.unwrap_or_else(|error| Err(error.into()))
    }

    pub async fn shutdown(&self) -> Result<(), P2PError> {
        let (sender, receiver) = oneshot::channel();
        self.shutdown_channel
            .send(sender)
            .await
            .map_err(P2PError::ShutdownCommunication)?;

        Ok(receiver.await?)
    }

    /// Creates a new gRPC client for the given peer.
    pub async fn new_grpc_client<C, S>(&self, peer: PeerId) -> Result<C, P2PError>
    where
        C: GrpcClient<Output = C>,
        S: NamedService,
    {
        self.grpc_over_p2p.create::<C, S>(peer).await
    }
}

pub enum RetryPolicy {
    NoRetry,
    N(usize),
}
