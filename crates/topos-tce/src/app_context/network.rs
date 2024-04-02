use prost::Message;
use std::collections::hash_map;
use topos_tce_storage::errors::{InternalStorageError, StorageError};

use tokio::spawn;

use topos_metrics::CERTIFICATE_DELIVERY_LATENCY;
use topos_p2p::Event as NetEvent;
use topos_tce_broadcast::DoubleEchoCommand;
use tracing::{debug, error, info, trace};

use topos_core::api::grpc::tce::v1::{double_echo_request, DoubleEchoRequest, Echo, Gossip, Ready};
use topos_core::uci;

use crate::AppContext;

impl AppContext {
    pub(crate) async fn on_net_event(&mut self, evt: NetEvent) {
        trace!(
            "on_net_event: peer: {} event {:?}",
            &self.network_client.local_peer_id,
            &evt
        );

        if let NetEvent::Gossip { data, from } = evt {
            if let Ok(DoubleEchoRequest {
                request: Some(double_echo_request),
            }) = DoubleEchoRequest::decode(&data[..])
            {
                match double_echo_request {
                    double_echo_request::Request::Gossip(Gossip {
                        certificate: Some(certificate),
                    }) => match uci::Certificate::try_from(certificate) {
                        Ok(cert) => {
                            if let hash_map::Entry::Vacant(entry) =
                                self.delivery_latency.entry(cert.id)
                            {
                                entry.insert(CERTIFICATE_DELIVERY_LATENCY.start_timer());
                            }
                            info!(
                                "Received certificate {} from GossipSub from {}",
                                cert.id, from
                            );

                            match self.validator_store.insert_pending_certificate(&cert).await {
                                Ok(Some(pending_id)) => {
                                    let certificate_id = cert.id;
                                    debug!(
                                        "Certificate {} has been inserted into pending pool",
                                        certificate_id
                                    );

                                    if self
                                        .tce_cli
                                        .get_double_echo_channel()
                                        .send(DoubleEchoCommand::Broadcast {
                                            need_gossip: false,
                                            cert,
                                            pending_id,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        error!(
                                            "Unable to send DoubleEchoCommand::Broadcast command \
                                             to double echo for {}",
                                            certificate_id
                                        );
                                    }
                                }

                                Ok(None) => {
                                    debug!(
                                        "Certificate {} from subnet {} has been inserted into \
                                         precedence pool waiting for {}",
                                        cert.id, cert.source_subnet_id, cert.prev_id
                                    );
                                }
                                Err(StorageError::InternalStorage(
                                    InternalStorageError::CertificateAlreadyPending,
                                )) => {
                                    debug!(
                                        "Certificate {} has been already added to the pending \
                                         pool, skipping",
                                        cert.id
                                    );
                                }
                                Err(StorageError::InternalStorage(
                                    InternalStorageError::CertificateAlreadyExists,
                                )) => {
                                    debug!(
                                        "Certificate {} has been already delivered, skipping",
                                        cert.id
                                    );
                                }
                                Err(error) => {
                                    error!(
                                        "Unable to insert pending certificate {}: {}",
                                        cert.id, error
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to parse the received Certificate: {e}");
                        }
                    },
                    double_echo_request::Request::Echo(Echo {
                        certificate_id: Some(certificate_id),
                        signature: Some(signature),
                        validator_id: Some(validator_id),
                    }) => {
                        let channel = self.tce_cli.get_double_echo_channel();
                        spawn(async move {
                            let certificate_id = certificate_id.clone().try_into().map_err(|e| {
                                error!(
                                    "Failed to parse the CertificateId {certificate_id} from \
                                     Echo: {e}"
                                );
                                e
                            });
                            let validator_id = validator_id.clone().try_into().map_err(|e| {
                                error!(
                                    "Failed to parse the ValidatorId {validator_id} from Echo: {e}"
                                );
                                e
                            });

                            if let (Ok(certificate_id), Ok(validator_id)) =
                                (certificate_id, validator_id)
                            {
                                trace!(
                                    "Received Echo message, certificate_id: {certificate_id}, \
                                     validator_id: {validator_id} from: {from}",
                                    certificate_id = certificate_id,
                                    validator_id = validator_id
                                );

                                if let Err(e) = channel
                                    .send(DoubleEchoCommand::Echo {
                                        signature: signature.into(),
                                        certificate_id,
                                        validator_id,
                                    })
                                    .await
                                {
                                    error!("Unable to pass received Echo message: {:?}", e);
                                }
                            } else {
                                error!("Unable to process Echo message due to invalid data");
                            }
                        });
                    }
                    double_echo_request::Request::Ready(Ready {
                        certificate_id: Some(certificate_id),
                        signature: Some(signature),
                        validator_id: Some(validator_id),
                    }) => {
                        let channel = self.tce_cli.get_double_echo_channel();
                        spawn(async move {
                            let certificate_id = certificate_id.clone().try_into().map_err(|e| {
                                error!(
                                    "Failed to parse the CertificateId {certificate_id} from \
                                     Ready: {e}"
                                );
                                e
                            });
                            let validator_id = validator_id.clone().try_into().map_err(|e| {
                                error!(
                                    "Failed to parse the ValidatorId {validator_id} from Ready: \
                                     {e}"
                                );
                                e
                            });
                            if let (Ok(certificate_id), Ok(validator_id)) =
                                (certificate_id, validator_id)
                            {
                                trace!(
                                    "Received Ready message, certificate_id: {certificate_id}, \
                                     validator_id: {validator_id} from: {from}",
                                    certificate_id = certificate_id,
                                    validator_id = validator_id
                                );
                                if let Err(e) = channel
                                    .send(DoubleEchoCommand::Ready {
                                        signature: signature.into(),
                                        certificate_id,
                                        validator_id,
                                    })
                                    .await
                                {
                                    error!("Unable to pass received Ready message: {:?}", e);
                                }
                            } else {
                                error!("Unable to process Ready message due to invalid data");
                            }
                        });
                    }
                    _ => {}
                }
            }
        }
    }
}
