use topos_core::api::grpc::tce::v1::{double_echo_request, DoubleEchoRequest, Echo, Gossip, Ready};
use topos_tce_broadcast::event::ProtocolEvents;
use tracing::{debug, error, info, warn};

use crate::AppContext;

impl AppContext {
    pub async fn on_protocol_event(&mut self, evt: ProtocolEvents) {
        match evt {
            ProtocolEvents::Broadcast { certificate_id } => {
                info!("Broadcasting certificate {}", certificate_id);
            }

            ProtocolEvents::Gossip { cert } => {
                let cert_id = cert.id;

                let request = DoubleEchoRequest {
                    request: Some(double_echo_request::Request::Gossip(Gossip {
                        certificate: Some(cert.into()),
                    })),
                };

                info!("Sending Gossip for certificate {}", cert_id);
                match self
                    .network_client
                    .publish(topos_p2p::TOPOS_GOSSIP, request)
                    .await
                {
                    Ok(id) => debug!(
                        message_id = id,
                        "Sent Gossip message({id}) for certificate {cert_id}"
                    ),
                    Err(e) => error!("Unable to send Gossip: {e}"),
                }
            }

            ProtocolEvents::Echo {
                certificate_id,
                signature,
                validator_id,
            } if self.is_validator => {
                // Send echo message
                let request = DoubleEchoRequest {
                    request: Some(double_echo_request::Request::Echo(Echo {
                        certificate_id: Some(certificate_id.into()),
                        signature: Some(signature.into()),
                        validator_id: Some(validator_id.into()),
                    })),
                };

                match self
                    .network_client
                    .publish(topos_p2p::TOPOS_ECHO, request)
                    .await
                {
                    Ok(id) => debug!(
                        message_id = id,
                        "Sent Echo message({id}) for certificate {certificate_id}"
                    ),
                    Err(e) => error!("Unable to send Echo: {e}"),
                }
            }

            ProtocolEvents::Ready {
                certificate_id,
                signature,
                validator_id,
            } if self.is_validator => {
                let request = DoubleEchoRequest {
                    request: Some(double_echo_request::Request::Ready(Ready {
                        certificate_id: Some(certificate_id.into()),
                        signature: Some(signature.into()),
                        validator_id: Some(validator_id.into()),
                    })),
                };

                match self
                    .network_client
                    .publish(topos_p2p::TOPOS_READY, request)
                    .await
                {
                    Ok(id) => debug!(
                        message_id = id,
                        "Sent Ready message({id}) for certificate {certificate_id}"
                    ),
                    Err(e) => error!("Unable to send Ready: {e}"),
                }
            }
            ProtocolEvents::BroadcastFailed { certificate_id } => {
                warn!("Broadcast failed for certificate {certificate_id}")
            }
            ProtocolEvents::AlreadyDelivered { certificate_id } => {
                info!("Certificate {certificate_id} already delivered")
            }
            _ => {}
        }
    }
}
