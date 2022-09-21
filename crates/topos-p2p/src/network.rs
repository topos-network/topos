use super::{Behaviour, Client, Event, Runtime};
use crate::{
    behaviour::{
        discovery::DiscoveryBehaviour, peer_info::PeerInfoBehaviour,
        transmission::TransmissionBehaviour,
    },
    constant::{
        COMMAND_STREAM_BUFFER, DISCOVERY_PROTOCOL, EVENT_STREAM_BUFFER, TRANSMISSION_PROTOCOL,
    },
};
use futures::Stream;
use libp2p::{
    core::upgrade,
    identity::Keypair,
    kad::store::MemoryStore,
    mplex, noise,
    swarm::SwarmBuilder,
    tcp::{GenTcpConfig, TokioTcpTransport},
    Multiaddr, PeerId, Swarm, Transport,
};
use std::{collections::VecDeque, error::Error, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

pub fn builder() -> NetworkBuilder {
    NetworkBuilder::default()
}

const TWO_HOURS: Duration = Duration::from_secs(60 * 60 * 2);

#[derive(Default)]
pub struct NetworkBuilder {
    discovery_protocol: Option<&'static str>,
    transmission_protocol: Option<&'static str>,
    peer_key: Option<Keypair>,
    listen_addr: Option<Multiaddr>,
    store: Option<MemoryStore>,
    known_peers: Vec<(PeerId, Multiaddr)>,
    local_port: Option<u8>,
}

impl NetworkBuilder {
    pub fn peer_key(mut self, peer_key: Keypair) -> Self {
        self.peer_key = Some(peer_key);

        self
    }

    pub fn listen_addr(mut self, addr: Multiaddr) -> Self {
        self.listen_addr = Some(addr);

        self
    }
    pub fn store(mut self, store: MemoryStore) -> Self {
        self.store = Some(store);

        self
    }

    pub fn known_peers(mut self, known_peers: Vec<(PeerId, Multiaddr)>) -> Self {
        self.known_peers = known_peers;

        self
    }

    pub fn local_port(mut self, port: u8) -> Self {
        self.local_port = Some(port);

        self
    }

    pub fn transmission_protocol(mut self, protocol: &'static str) -> Self {
        self.transmission_protocol = Some(protocol);

        self
    }

    pub fn discovery_protocol(mut self, protocol: &'static str) -> Self {
        self.discovery_protocol = Some(protocol);

        self
    }

    pub async fn build(
        self,
    ) -> Result<(Client, impl Stream<Item = Event>, Runtime), Box<dyn Error>> {
        let peer_key = self.peer_key.expect("peer_key not defined");
        let peer_id = peer_key.public().to_peer_id();

        let noise_keys = noise::Keypair::<noise::X25519Spec>::new().into_authentic(&peer_key)?;

        let (command_sender, command_receiver) = mpsc::channel(COMMAND_STREAM_BUFFER);
        let (event_sender, event_receiver) = mpsc::channel(EVENT_STREAM_BUFFER);

        let behaviour = Behaviour {
            peer_info: PeerInfoBehaviour::new(
                self.transmission_protocol.unwrap_or(TRANSMISSION_PROTOCOL),
                &peer_key,
            ),

            discovery: DiscoveryBehaviour::new(
                peer_key.clone(),
                self.discovery_protocol.unwrap_or(DISCOVERY_PROTOCOL),
                &self.known_peers[..],
                false,
                // TODO: better handle the listening part
                self.listen_addr.clone(),
            ),
            transmission: TransmissionBehaviour::new(),
            events: VecDeque::new(),
            peer_id,
            addresses: self.listen_addr.clone().unwrap(),
        };

        let transport = TokioTcpTransport::new(GenTcpConfig::default().nodelay(true))
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
            .multiplex(mplex::MplexConfig::new())
            .timeout(TWO_HOURS)
            .boxed();

        let mut swarm = SwarmBuilder::new(transport, behaviour, peer_id)
            .executor(Box::new(|future| {
                let _ = tokio::spawn(future);
            }))
            .build();

        if let Some(addr) = self.listen_addr {
            if let Err(error) = Swarm::listen_on(&mut swarm, addr.clone()) {
                warn!("Couldn't start listening on {addr} because of {error:?}");
            }
        }

        Ok((
            Client {
                local_peer_id: peer_id,
                sender: command_sender,
            },
            ReceiverStream::new(event_receiver),
            Runtime::new(swarm, command_receiver, event_sender, peer_id),
        ))
    }
}
