use crate::{
    error::{CommandExecutionError, P2PError},
    protocol_name, Command, Runtime,
};

use rand::{thread_rng, Rng};
use topos_metrics::P2P_MESSAGE_SENT_ON_GOSSIPSUB_TOTAL;
use tracing::{error, trace, warn};

impl Runtime {
    pub(crate) async fn handle_command(&mut self, command: Command) {
        match command {
            Command::NewProxiedQuery {
                peer,
                id,
                response,
                protocol,
            } => {
                let connection = self
                    .swarm
                    .behaviour_mut()
                    .grpc
                    .open_outbound_connection(&peer, protocol_name!(protocol));

                _ = response.send(connection);
            }

            Command::ConnectedPeers { sender } => {
                if sender
                    .send(Ok(self
                        .swarm
                        .connected_peers()
                        .cloned()
                        .collect::<Vec<_>>()))
                    .is_err()
                {
                    warn!("Unable to notify ConnectedPeers response: initiator is dropped");
                }
            }
            Command::RandomKnownPeer { sender } => {
                if self.peer_set.is_empty() {
                    let _ = sender.send(Err(P2PError::CommandError(
                        CommandExecutionError::NoKnownPeer,
                    )));

                    return;
                }

                let selected_peer: usize = thread_rng().gen_range(0..(self.peer_set.len()));
                if sender
                    .send(
                        self.peer_set
                            .iter()
                            .nth(selected_peer)
                            .cloned()
                            .ok_or(P2PError::CommandError(CommandExecutionError::NoKnownPeer)),
                    )
                    .is_err()
                {
                    warn!("Unable to notify RandomKnownPeer response: initiator is dropped");
                }
            }

            Command::Gossip {
                topic,
                data: message,
                sender,
            } => match self.swarm.behaviour_mut().gossipsub.publish(topic, message) {
                Ok(message_id) => {
                    trace!("Published message to {topic}");
                    P2P_MESSAGE_SENT_ON_GOSSIPSUB_TOTAL.inc();
                    _ = sender.send(Ok(message_id));
                }
                Err(err) => {
                    error!("Failed to publish message to {topic}: {err}");
                    _ = sender.send(Err(err));
                }
            },
        }
    }
}
