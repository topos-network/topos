use topos_metrics::{
    P2P_EVENT_STREAM_CAPACITY_TOTAL, P2P_MESSAGE_RECEIVED_ON_ECHO_TOTAL,
    P2P_MESSAGE_RECEIVED_ON_GOSSIP_TOTAL, P2P_MESSAGE_RECEIVED_ON_READY_TOTAL,
};
use tracing::{debug, error};

use crate::{constants, event::GossipEvent, Event, Runtime, TOPOS_ECHO, TOPOS_GOSSIP, TOPOS_READY};

use super::{EventHandler, EventResult};

#[async_trait::async_trait]
impl EventHandler<GossipEvent> for Runtime {
    async fn handle(&mut self, event: GossipEvent) -> EventResult {
        if let GossipEvent::Message {
            source: Some(source),
            message,
            topic,
            id,
        } = event
        {
            if self.event_sender.capacity() < *constants::CAPACITY_EVENT_STREAM_BUFFER {
                P2P_EVENT_STREAM_CAPACITY_TOTAL.inc();
            }

            debug!("Received message from {:?} on topic {:?}", source, topic);

            match topic {
                TOPOS_GOSSIP => P2P_MESSAGE_RECEIVED_ON_GOSSIP_TOTAL.inc(),
                TOPOS_ECHO => P2P_MESSAGE_RECEIVED_ON_ECHO_TOTAL.inc(),
                TOPOS_READY => P2P_MESSAGE_RECEIVED_ON_READY_TOTAL.inc(),
                _ => {
                    error!("Received message on unknown topic {:?}", topic);
                    return Ok(());
                }
            }

            if let Err(e) = self
                .event_sender
                .send(Event::Gossip {
                    from: source,
                    data: message,
                    id: id.to_string(),
                })
                .await
            {
                error!("Failed to send gossip event to runtime: {:?}", e);
            }
        }

        Ok(())
    }
}
