use std::fmt::Display;

use libp2p::{gossipsub::MessageId, PeerId};
use tokio::sync::oneshot;

use crate::{behaviour::grpc::connection::OutboundConnection, error::P2PError};

#[derive(Debug)]
pub enum Command {
    /// Command to ask for the current connected peer id list
    ConnectedPeers {
        sender: oneshot::Sender<Result<Vec<PeerId>, P2PError>>,
    },

    Gossip {
        topic: &'static str,
        data: Vec<u8>,
        sender: oneshot::Sender<Result<MessageId, P2PError>>,
    },

    /// Ask for the creation of a new proxy connection for a gRPC query.
    /// The response will be sent to the sender of the command once the connection is established.
    /// The response will be a `OutboundConnection` that can be used to create a gRPC client.
    /// A connection is established if needed with the peer.
    NewProxiedQuery {
        protocol: &'static str,
        peer: PeerId,
        id: uuid::Uuid,
        response: oneshot::Sender<OutboundConnection>,
    },

    /// Ask for a random known peer
    RandomKnownPeer {
        sender: oneshot::Sender<Result<PeerId, P2PError>>,
    },
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::ConnectedPeers { .. } => write!(f, "ConnectedPeers"),
            Command::RandomKnownPeer { .. } => write!(f, "RandomKnownPeer"),
            Command::Gossip { .. } => write!(f, "GossipMessage"),
            Command::NewProxiedQuery { .. } => write!(f, "NewProxiedQuery"),
        }
    }
}
