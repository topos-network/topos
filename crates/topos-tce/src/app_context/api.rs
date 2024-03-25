use crate::AppContext;
use std::collections::HashMap;
use tokio::spawn;
use topos_core::uci::{Certificate, SubnetId};
use topos_metrics::CERTIFICATE_DELIVERY_LATENCY;
use topos_tce_api::RuntimeError;
use topos_tce_api::RuntimeEvent as ApiEvent;
use topos_tce_broadcast::DoubleEchoCommand;
use topos_tce_storage::errors::{InternalStorageError, StorageError};
use topos_tce_storage::types::PendingResult;
use tracing::debug;
use tracing::{error, warn};

impl AppContext {
    pub async fn on_api_event(&mut self, event: ApiEvent) {
        match event {
            ApiEvent::CertificateSubmitted {
                certificate,
                sender,
            } => {
                self.delivery_latency
                    .insert(certificate.id, CERTIFICATE_DELIVERY_LATENCY.start_timer());

                let validator_store = self.validator_store.clone();
                let double_echo = self.tce_cli.get_double_echo_channel();

                spawn(async move {
                    _ = match validator_store
                        .insert_pending_certificate(&certificate)
                        .await
                    {
                        Ok(Some(pending_id)) => {
                            let certificate_id = certificate.id;
                            debug!(
                                "Certificate {} from subnet {} has been inserted into pending pool",
                                certificate_id, certificate.source_subnet_id
                            );

                            if double_echo
                                .send(DoubleEchoCommand::Broadcast {
                                    need_gossip: true,
                                    cert: *certificate,
                                    pending_id,
                                })
                                .await
                                .is_err()
                            {
                                error!(
                                    "Unable to send DoubleEchoCommand::Broadcast command to \
                                     double echo for {}",
                                    certificate_id
                                );

                                sender.send(Err(RuntimeError::CommunicationError(
                                    "Unable to send DoubleEchoCommand::Broadcast command to \
                                     double echo"
                                        .to_string(),
                                )))
                            } else {
                                sender.send(Ok(PendingResult::InPending(pending_id)))
                            }
                        }
                        Ok(None) => {
                            debug!(
                                "Certificate {} from subnet {} has been inserted into precedence \
                                 pool waiting for {}",
                                certificate.id, certificate.source_subnet_id, certificate.prev_id
                            );
                            sender.send(Ok(PendingResult::AwaitPrecedence))
                        }
                        Err(StorageError::InternalStorage(
                            InternalStorageError::CertificateAlreadyPending,
                        )) => {
                            debug!(
                                "Certificate {} has already been added to the pending pool, \
                                 skipping",
                                certificate.id
                            );
                            sender.send(Ok(PendingResult::AlreadyPending))
                        }
                        Err(StorageError::InternalStorage(
                            InternalStorageError::CertificateAlreadyExists,
                        )) => {
                            debug!(
                                "Certificate {} has already been delivered, skipping",
                                certificate.id
                            );
                            sender.send(Ok(PendingResult::AlreadyDelivered))
                        }
                        Err(error) => {
                            error!(
                                "Unable to insert pending certificate {}: {}",
                                certificate.id, error
                            );

                            sender.send(Err(error.into()))
                        }
                    };
                });
            }

            ApiEvent::GetSourceHead { subnet_id, sender } => {
                // Get source head certificate
                let mut result = self
                    .pending_storage
                    .get_source_head(subnet_id)
                    .await
                    .and_then(|result| match result {
                        None => Err(StorageError::InternalStorage(
                            InternalStorageError::MissingHeadForSubnet(subnet_id),
                        )),
                        value => Ok(value),
                    })
                    .map_err(|e| match e {
                        StorageError::InternalStorage(internal) => {
                            if let InternalStorageError::MissingHeadForSubnet(subnet_id) = internal
                            {
                                RuntimeError::UnknownSubnet(subnet_id)
                            } else {
                                RuntimeError::UnableToGetSourceHead(subnet_id, internal.to_string())
                            }
                        }
                        e => RuntimeError::UnableToGetSourceHead(subnet_id, e.to_string()),
                    });

                // TODO: Initial genesis certificate eventually will be fetched from the topos subnet
                // Currently, for subnet starting from scratch there are no certificates in the database
                // So for MissingHeadForSubnet error we will return some default dummy certificate
                if let Err(RuntimeError::UnknownSubnet(subnet_id)) = result {
                    warn!("Returning dummy certificate as head certificate, to be fixed...");
                    result = Ok(Some((
                        0,
                        topos_core::uci::Certificate {
                            prev_id: AppContext::DUMMY_INITIAL_CERTIFICATE_ID,
                            source_subnet_id: subnet_id,
                            state_root: Default::default(),
                            tx_root_hash: Default::default(),
                            receipts_root_hash: Default::default(),
                            target_subnets: vec![],
                            verifier: 0,
                            id: AppContext::DUMMY_INITIAL_CERTIFICATE_ID,
                            proof: Default::default(),
                            signature: Default::default(),
                        },
                    )));
                };

                _ = sender.send(result);
            }

            ApiEvent::GetLastPendingCertificates {
                mut subnet_ids,
                sender,
            } => {
                let mut last_pending_certificates: HashMap<SubnetId, Option<(Certificate, u64)>> =
                    subnet_ids
                        .iter()
                        .map(|subnet_id| (*subnet_id, None))
                        .collect();

                if let Ok(pending_certificates) =
                    self.pending_storage.get_pending_certificates().await
                {
                    // Count number of pending certificates for every subnet
                    let mut indexes: HashMap<SubnetId, u64> = HashMap::new();
                    for (_pending_certificate_id, cert) in pending_certificates.iter() {
                        *indexes.entry(cert.source_subnet_id).or_insert(0) += 1;
                    }

                    // Iterate through pending certificates and determine last one for every subnet
                    // Last certificate in the subnet should be one with the highest index
                    for (_pending_certificate_id, cert) in pending_certificates.into_iter().rev() {
                        if let Some(subnet_id) = subnet_ids.take(&cert.source_subnet_id) {
                            *last_pending_certificates.entry(subnet_id).or_insert(None) =
                                Some((cert, indexes[&subnet_id]));
                        }
                        if subnet_ids.is_empty() {
                            break;
                        }
                    }
                }

                // Add None pending certificate for any other requested subnet_id
                subnet_ids.iter().for_each(|subnet_id| {
                    last_pending_certificates.insert(*subnet_id, None);
                });

                _ = sender.send(Ok(last_pending_certificates));
            }
        }
    }
}
