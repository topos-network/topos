use std::io;

use libp2p::{
    gossipsub::{PublishError, SubscriptionError},
    kad::NoKnownPeers,
    noise::Error as NoiseError,
    request_response::OutboundFailure,
    TransportError,
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::{behaviour::grpc::error::OutboundConnectionError, command::Command};

#[derive(Error, Debug)]
pub enum P2PError {
    #[error("Unable build a network: peer_key missing")]
    MissingPeerKey,

    #[error("Unable to reach any bootnode")]
    UnableToReachBootnode,

    #[error("The handle on the runtime failed")]
    JoinHandleFailure,

    #[error(transparent)]
    CommandError(#[from] CommandExecutionError),

    #[error("An error occurred on the Transport layer: {0}")]
    TransportError(#[from] TransportError<io::Error>),

    #[error("An error occured trying to subscribe to gossip topic: {0}")]
    SubscriptionError(#[from] SubscriptionError),

    #[error("Unable to receive expected response of a oneshot channel")]
    OneshotReceiveError(#[from] oneshot::error::RecvError),

    #[error("An error occurred on the Noise protocol: {0}")]
    NoiseProtocolError(#[from] NoiseError),

    #[error("Error during bootstrap phase: {0}")]
    BootstrapError(&'static str),

    #[error("Kademlia bootstrap query error: {0}")]
    KademliaBootstrapError(#[from] NoKnownPeers),

    #[error("Unable to execute shutdown on the p2p runtime: {0}")]
    ShutdownCommunication(mpsc::error::SendError<oneshot::Sender<()>>),

    #[error("Unable to create gRPC client")]
    UnableToCreateGrpcClient(#[from] OutboundConnectionError),

    #[error("Gossip topics subscription failed")]
    GossipTopicSubscriptionFailure,

    #[error("Gossipsub publish failure: {0}")]
    GossipsubPublishFailure(#[from] PublishError),

    #[error("Invalid gossipsub topics: {0}")]
    InvalidGossipTopic(&'static str),
}

#[derive(Error, Debug)]
pub enum CommandExecutionError {
    #[error("Unable to parse message")]
    ParsingError,

    #[error("Unable to send command {0}")]
    UnableToSendCommand(Command),

    #[error("Unable to perform query: {0}")]
    RequestOutboundFailure(#[from] OutboundFailure),

    #[error("Unable to receive expected response of a oneshot channel")]
    UnableToReceiveCommandResponse(#[from] oneshot::error::RecvError),

    #[error("Unable to send a command: {0}")]
    SendError(#[from] mpsc::error::SendError<Command>),

    #[error("Failed to fetch Record from DHT")]
    DHTGetRecordFailed,

    #[error("Connection with a peer has failed")]
    ConnectionClosed,

    #[error("No known peer in the peer set")]
    NoKnownPeer,
}
