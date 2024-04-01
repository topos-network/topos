use libp2p::{gossipsub::MessageId, identify, kad, PeerId};

use crate::behaviour::{grpc, HealthStatus};

/// Represents the events that the Gossip protocol can emit
#[derive(Debug)]
pub enum GossipEvent {
    /// A message has been received from a peer on one of the subscribed topics
    Message {
        propagated_by: PeerId,
        source: Option<PeerId>,
        topic: &'static str,
        message: Vec<u8>,
        id: MessageId,
    },
}

#[derive(Debug)]
pub enum ComposedEvent {
    Kademlia(Box<kad::Event>),
    PeerInfo(Box<identify::Event>),
    Gossipsub(Box<GossipEvent>),
    Grpc(grpc::Event),
    Void,
}

impl From<grpc::Event> for ComposedEvent {
    fn from(event: grpc::Event) -> Self {
        ComposedEvent::Grpc(event)
    }
}

impl From<kad::Event> for ComposedEvent {
    fn from(event: kad::Event) -> Self {
        ComposedEvent::Kademlia(Box::new(event))
    }
}

impl From<identify::Event> for ComposedEvent {
    fn from(event: identify::Event) -> Self {
        ComposedEvent::PeerInfo(Box::new(event))
    }
}

impl From<void::Void> for ComposedEvent {
    fn from(_: void::Void) -> Self {
        Self::Void
    }
}

/// Represents the events that the p2p layer can emit
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    /// An event emitted when a gossip message is received
    Gossip {
        propagated_by: PeerId,
        from: PeerId,
        data: Vec<u8>,
        id: String,
    },
    /// An event emitted when the p2p layer becomes healthy
    Healthy,
    /// An event emitted when the p2p layer becomes unhealthy
    Unhealthy,
    /// An event emitted when the p2p layer is shutting down
    Killing,
}

impl From<&HealthStatus> for Event {
    fn from(value: &HealthStatus) -> Self {
        match value {
            HealthStatus::Healthy => Event::Healthy,
            HealthStatus::Killing => Event::Killing,
            _ => Event::Unhealthy,
        }
    }
}
