use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::{
    collections::{HashMap, VecDeque},
    env,
    task::Poll,
    time::Duration,
};

use libp2p::swarm::{ConnectionClosed, FromSwarm};
use libp2p::PeerId;
use libp2p::{
    gossipsub::{self, IdentTopic, Message, MessageAuthenticity},
    identity::Keypair,
    swarm::{NetworkBehaviour, THandlerInEvent, ToSwarm},
};
use prost::Message as ProstMessage;
use topos_core::api::grpc::tce::v1::Batch;
use topos_metrics::P2P_GOSSIP_BATCH_SIZE;
use tracing::{debug, error, warn};

use crate::error::P2PError;
use crate::{constants, event::ComposedEvent, TOPOS_ECHO, TOPOS_GOSSIP, TOPOS_READY};

use super::HealthStatus;

const MAX_BATCH_SIZE: usize = 1024;

pub struct Behaviour {
    batch_size: usize,
    gossipsub: gossipsub::Behaviour,
    pending: HashMap<&'static str, VecDeque<Vec<u8>>>,
    tick: tokio::time::Interval,
    /// List of connected peers per topic.
    connected_peer: HashMap<&'static str, HashSet<PeerId>>,
    /// The health status of the gossip behaviour
    pub(crate) health_status: HealthStatus,
}

impl Behaviour {
    pub fn publish(
        &mut self,
        topic: &'static str,
        message: Vec<u8>,
    ) -> Result<usize, &'static str> {
        match topic {
            TOPOS_GOSSIP => {
                if let Ok(msg_id) = self.gossipsub.publish(IdentTopic::new(topic), message) {
                    debug!("Published on topos_gossip: {:?}", msg_id);
                }
            }
            TOPOS_ECHO | TOPOS_READY => self.pending.entry(topic).or_default().push_back(message),
            _ => return Err("Invalid topic"),
        }

        Ok(0)
    }

    pub fn subscribe(&mut self) -> Result<(), P2PError> {
        self.gossipsub
            .subscribe(&gossipsub::IdentTopic::new(TOPOS_GOSSIP))?;

        self.gossipsub
            .subscribe(&gossipsub::IdentTopic::new(TOPOS_ECHO))?;

        self.gossipsub
            .subscribe(&gossipsub::IdentTopic::new(TOPOS_READY))?;

        Ok(())
    }

    pub async fn new(peer_key: Keypair) -> Self {
        let batch_size = env::var("TOPOS_GOSSIP_BATCH_SIZE")
            .map(|v| v.parse::<usize>())
            .unwrap_or(Ok(MAX_BATCH_SIZE))
            .unwrap();
        let gossipsub = gossipsub::ConfigBuilder::default()
            .max_transmit_size(2 * 2048 * 2048)
            .validation_mode(gossipsub::ValidationMode::Strict)
            .message_id_fn(|msg_id| {
                // Content based id
                let mut s = DefaultHasher::new();
                msg_id.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_be_bytes())
            })
            .build()
            .unwrap();

        let gossipsub = gossipsub::Behaviour::new_with_metrics(
            MessageAuthenticity::Signed(peer_key),
            gossipsub,
            constants::METRIC_REGISTRY
                .lock()
                .await
                .sub_registry_with_prefix("libp2p_gossipsub"),
            Default::default(),
        )
        .unwrap();

        Self {
            batch_size,
            gossipsub,
            pending: [
                (TOPOS_ECHO, VecDeque::new()),
                (TOPOS_READY, VecDeque::new()),
            ]
            .into_iter()
            .collect(),
            tick: tokio::time::interval(Duration::from_millis(
                env::var("TOPOS_GOSSIP_INTERVAL")
                    .map(|v| v.parse::<u64>())
                    .unwrap_or(Ok(100))
                    .unwrap(),
            )),

            connected_peer: Default::default(),
            health_status: Default::default(),
        }
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = <gossipsub::Behaviour as NetworkBehaviour>::ConnectionHandler;

    type ToSwarm = ComposedEvent;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: libp2p::swarm::ConnectionId,
        peer: libp2p::PeerId,
        local_addr: &libp2p::Multiaddr,
        remote_addr: &libp2p::Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        self.gossipsub.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: libp2p::swarm::ConnectionId,
        peer: libp2p::PeerId,
        addr: &libp2p::Multiaddr,
        role_override: libp2p::core::Endpoint,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        self.gossipsub.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
        )
    }

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm) {
        if let FromSwarm::ConnectionClosed(ConnectionClosed {
            peer_id,
            connection_id,
            endpoint,
            remaining_established,
            ..
        }) = &event
        {
            debug!(
                "Connection closed: {:?} {:?} {:?} {:?}",
                peer_id, connection_id, endpoint, remaining_established
            );

            for (_, topic) in self.connected_peer.iter_mut() {
                topic.remove(peer_id);
            }
        }

        self.gossipsub.on_swarm_event(event)
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: libp2p::PeerId,
        connection_id: libp2p::swarm::ConnectionId,
        event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        self.gossipsub
            .on_connection_handler_event(peer_id, connection_id, event)
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if self.tick.poll_tick(cx).is_ready() {
            // Publish batch
            for (topic, queue) in self.pending.iter_mut() {
                if !queue.is_empty() {
                    let num_of_message = queue.len().min(self.batch_size);
                    let batch = Batch {
                        messages: queue.drain(0..num_of_message).collect(),
                    };

                    debug!("Publishing {} {}", batch.messages.len(), topic);
                    let msg = batch.encode_to_vec();
                    P2P_GOSSIP_BATCH_SIZE.observe(batch.messages.len() as f64);
                    match self.gossipsub.publish(IdentTopic::new(*topic), msg) {
                        Ok(message_id) => debug!("Published {} {}", topic, message_id),
                        Err(error) => error!("Failed to publish {}: {}", topic, error),
                    }
                }
            }
        }

        match self.gossipsub.poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(ToSwarm::GenerateEvent(event)) => match event {
                gossipsub::Event::Message {
                    propagation_source,
                    message_id,
                    message:
                        Message {
                            source,
                            data,
                            topic,
                            ..
                        },
                } => match topic.as_str() {
                    TOPOS_GOSSIP => {
                        return Poll::Ready(ToSwarm::GenerateEvent(ComposedEvent::Gossipsub(
                            crate::event::GossipEvent::Message {
                                topic: TOPOS_GOSSIP,
                                message: data,
                                source,
                            },
                        )))
                    }
                    TOPOS_ECHO => {
                        return Poll::Ready(ToSwarm::GenerateEvent(ComposedEvent::Gossipsub(
                            crate::event::GossipEvent::Message {
                                topic: TOPOS_ECHO,
                                message: data,
                                source,
                            },
                        )))
                    }
                    TOPOS_READY => {
                        return Poll::Ready(ToSwarm::GenerateEvent(ComposedEvent::Gossipsub(
                            crate::event::GossipEvent::Message {
                                topic: TOPOS_READY,
                                message: data,
                                source,
                            },
                        )))
                    }
                    _ => {}
                },
                gossipsub::Event::Subscribed { peer_id, topic } => {
                    debug!("{peer_id} subscribed to {:?}", topic);

                    // If the behaviour isn't already healthy we check if this event
                    // triggers a switch to healthy
                    if self.health_status != HealthStatus::Healthy
                        && self.gossipsub.topics().all(|topic| {
                            self.gossipsub.mesh_peers(topic).peekable().peek().is_some()
                        })
                    {
                        self.health_status = HealthStatus::Healthy;
                    }
                }
                gossipsub::Event::Unsubscribed { peer_id, topic } => {
                    debug!("{peer_id} unsubscribed from {:?}", topic);
                }
                gossipsub::Event::GossipsubNotSupported { peer_id } => {
                    debug!("Gossipsub not supported by {:?}", peer_id);
                }
            },
            Poll::Ready(ToSwarm::ListenOn { opts }) => {
                return Poll::Ready(ToSwarm::ListenOn { opts })
            }
            Poll::Ready(ToSwarm::RemoveListener { id }) => {
                return Poll::Ready(ToSwarm::RemoveListener { id })
            }
            Poll::Ready(ToSwarm::Dial { opts }) => return Poll::Ready(ToSwarm::Dial { opts }),
            Poll::Ready(ToSwarm::NotifyHandler {
                peer_id,
                handler,
                event,
            }) => {
                return Poll::Ready(ToSwarm::NotifyHandler {
                    peer_id,
                    handler,
                    event,
                })
            }
            Poll::Ready(ToSwarm::CloseConnection {
                peer_id,
                connection,
            }) => {
                return Poll::Ready(ToSwarm::CloseConnection {
                    peer_id,
                    connection,
                })
            }
            Poll::Ready(ToSwarm::ExternalAddrExpired(addr)) => {
                return Poll::Ready(ToSwarm::ExternalAddrExpired(addr))
            }
            Poll::Ready(ToSwarm::ExternalAddrConfirmed(addr)) => {
                return Poll::Ready(ToSwarm::ExternalAddrConfirmed(addr))
            }
            Poll::Ready(ToSwarm::NewExternalAddrCandidate(addr)) => {
                return Poll::Ready(ToSwarm::NewExternalAddrCandidate(addr))
            }
            Poll::Ready(event) => {
                warn!("Unhandled event in gossip behaviour: {:?}", event);
            }
        }

        Poll::Pending
    }
}
