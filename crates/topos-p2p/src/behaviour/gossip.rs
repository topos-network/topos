use std::{
    collections::{HashSet, VecDeque},
    env,
    task::Poll,
    time::Duration,
};

use libp2p::{
    gossipsub::{self, IdentTopic, Message, MessageAuthenticity, MessageId, Topic, TopicHash},
    identity::Keypair,
    swarm::{NetworkBehaviour, THandlerInEvent, ToSwarm},
};
use serde::{Deserialize, Serialize};
use topos_metrics::{
    P2P_DUPLICATE_MESSAGE_ID_RECEIVED_TOTAL, P2P_GOSSIP_BATCH_SIZE,
    P2P_MESSAGE_SERIALIZE_FAILURE_TOTAL,
};
use tracing::{debug, error, info};

use crate::{constant, event::ComposedEvent, TOPOS_ECHO, TOPOS_GOSSIP, TOPOS_READY};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Batch {
    pub(crate) data: Vec<Vec<u8>>,
}

pub struct Behaviour {
    batch_size: usize,
    gossipsub: gossipsub::Behaviour,
    echo_queue: VecDeque<Vec<u8>>,
    ready_queue: VecDeque<Vec<u8>>,
    tick: tokio::time::Interval,
    cache: HashSet<MessageId>,
}

impl Behaviour {
    pub fn publish(&mut self, topic: &'static str, data: Vec<u8>) -> Result<usize, &'static str> {
        match topic {
            TOPOS_GOSSIP => {
                _ = self.gossipsub.publish(IdentTopic::new(topic), data);
            }
            TOPOS_ECHO => self.echo_queue.push_back(data),
            TOPOS_READY => self.ready_queue.push_back(data),
            _ => return Err("Invalid topic"),
        }

        Ok(0)
    }

    pub fn subscribe(&mut self) -> Result<(), &'static str> {
        self.gossipsub
            .subscribe(&gossipsub::IdentTopic::new(TOPOS_GOSSIP))
            .unwrap();

        self.gossipsub
            .subscribe(&gossipsub::IdentTopic::new(TOPOS_ECHO))
            .unwrap();

        self.gossipsub
            .subscribe(&gossipsub::IdentTopic::new(TOPOS_READY))
            .unwrap();

        Ok(())
    }

    pub fn new(peer_key: Keypair) -> Self {
        let batch_size = env::var("TOPOS_GOSSIP_BATCH_SIZE")
            .map(|v| v.parse::<usize>())
            .unwrap_or(Ok(10))
            .unwrap();
        let gossipsub = gossipsub::ConfigBuilder::default()
            .max_transmit_size(2 * 1024 * 1024)
            .validation_mode(gossipsub::ValidationMode::Strict)
            .build()
            .unwrap();

        let gossipsub = gossipsub::Behaviour::new_with_metrics(
            MessageAuthenticity::Signed(peer_key),
            gossipsub,
            constant::METRIC_REGISTRY
                .try_lock()
                .expect("Failed to lock metric registry during gossipsub creation")
                .sub_registry_with_prefix("libp2p_gossipsub"),
            Default::default(),
        )
        .unwrap();

        Self {
            batch_size,
            gossipsub,
            echo_queue: VecDeque::new(),
            ready_queue: VecDeque::new(),
            tick: tokio::time::interval(Duration::from_millis(
                env::var("TOPOS_GOSSIP_INTERVAL")
                    .map(|v| v.parse::<u64>())
                    .unwrap_or(Ok(100))
                    .unwrap(),
            )),
            cache: HashSet::new(),
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

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm<Self::ConnectionHandler>) {
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
        params: &mut impl libp2p::swarm::PollParameters,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if self.tick.poll_tick(cx).is_ready() {
            // Publish batch
            if !self.echo_queue.is_empty() {
                let mut echos = Batch { data: Vec::new() };
                for _ in 0..self.batch_size {
                    if let Some(data) = self.echo_queue.pop_front() {
                        echos.data.push(data);
                    } else {
                        break;
                    }
                }

                debug!("Publishing {} echos", echos.data.len());
                if let Ok(msg) = bincode::serialize::<Batch>(&echos) {
                    P2P_GOSSIP_BATCH_SIZE.observe(echos.data.len() as f64);

                    match self.gossipsub.publish(IdentTopic::new(TOPOS_ECHO), msg) {
                        Ok(message_id) => debug!("Published echo {}", message_id),
                        Err(error) => error!("Failed to publish echo: {}", error),
                    }
                } else {
                    P2P_MESSAGE_SERIALIZE_FAILURE_TOTAL
                        .with_label_values(&["echo"])
                        .inc();
                }
            }

            if !self.ready_queue.is_empty() {
                let mut readies = Batch { data: Vec::new() };
                for _ in 0..self.batch_size {
                    if let Some(data) = self.ready_queue.pop_front() {
                        readies.data.push(data);
                    } else {
                        break;
                    }
                }

                debug!("Publishing {} readies", readies.data.len());
                if let Ok(msg) = bincode::serialize::<Batch>(&readies) {
                    P2P_GOSSIP_BATCH_SIZE.observe(readies.data.len() as f64);
                    _ = self.gossipsub.publish(IdentTopic::new(TOPOS_READY), msg);
                } else {
                    P2P_MESSAGE_SERIALIZE_FAILURE_TOTAL
                        .with_label_values(&["ready"])
                        .inc();
                }
            }
        }

        let event = match self.gossipsub.poll(cx, params) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(ToSwarm::GenerateEvent(event)) => Some(event),
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
        };

        if let Some(gossipsub::Event::Message { ref message_id, .. }) = event {
            if self.cache.contains(message_id) {
                P2P_DUPLICATE_MESSAGE_ID_RECEIVED_TOTAL.inc();
            }
        }

        if let Some(gossipsub::Event::Message {
            propagation_source,
            message_id,
            message:
                Message {
                    source,
                    data,
                    sequence_number,
                    topic,
                },
        }) = event
        {
            match topic.as_str() {
                TOPOS_GOSSIP => {
                    return Poll::Ready(ToSwarm::GenerateEvent(ComposedEvent::Gossipsub(
                        crate::event::GossipEvent {
                            topic: TOPOS_GOSSIP,
                            message: data,
                            source,
                        },
                    )))
                }
                TOPOS_ECHO => {
                    return Poll::Ready(ToSwarm::GenerateEvent(ComposedEvent::Gossipsub(
                        crate::event::GossipEvent {
                            topic: TOPOS_ECHO,
                            message: data,
                            source,
                        },
                    )))
                }
                TOPOS_READY => {
                    return Poll::Ready(ToSwarm::GenerateEvent(ComposedEvent::Gossipsub(
                        crate::event::GossipEvent {
                            topic: TOPOS_READY,
                            message: data,
                            source,
                        },
                    )))
                }
                _ => {}
            }
        }

        Poll::Pending
    }
}
