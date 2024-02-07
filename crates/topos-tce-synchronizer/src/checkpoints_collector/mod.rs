use std::{
    collections::{HashMap, HashSet},
    future::IntoFuture,
    str::FromStr,
    sync::Arc,
};

use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use topos_core::{
    api::grpc::{
        self,
        shared::v1::Uuid as APIUuid,
        tce::v1::{
            synchronizer_service_client::SynchronizerServiceClient,
            synchronizer_service_server::SynchronizerServiceServer, CheckpointRequest,
            CheckpointResponse, FetchCertificatesRequest,
        },
    },
    errors::GrpcParsingError,
    types::ProofOfDelivery,
    uci::{Certificate, CertificateId, SubnetId},
};

use topos_p2p::{error::P2PError, NetworkClient, PeerId};
use topos_tce_storage::{errors::StorageError, store::ReadStore, validator::ValidatorStore};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

mod config;
mod error;
#[cfg(test)]
mod tests;

pub use config::CheckpointsCollectorConfig;
pub use error::CheckpointsCollectorError;

use crate::SynchronizerService;

pub struct CheckpointSynchronizer {
    pub(crate) config: CheckpointsCollectorConfig,

    pub(crate) network: NetworkClient,
    #[allow(unused)]
    pub(crate) store: Arc<ValidatorStore>,

    pub(crate) current_request_id: Option<APIUuid>,

    pub(crate) shutdown: CancellationToken,

    #[allow(dead_code)]
    pub(crate) events: mpsc::Sender<CheckpointsCollectorEvent>,
}

impl IntoFuture for CheckpointSynchronizer {
    type Output = Result<(), CheckpointsCollectorError>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
                self.config.sync_interval_seconds,
            ));

            loop {
                tokio::select! {
                    _tick = interval.tick() => {
                        // On every tick, checking if there is a pending synchronization
                        // If there is, skip
                        // If there is not,
                        //  1. Ask a random peer for the diff between local and its latest checkpoint
                        //  2. Validate the PoD diff, if fail, go back to 1
                        //  3. Based on the diff, check if we already have some of the certs
                        //      - Fetch every missing certs from one peer
                        //      - Each certs triggers a precedence check
                        if self.current_request_id.is_none() {
                            if let Err(error) = self.initiate_request().await {
                                warn!("Unsuccessful sync due to: {}", error);
                            }
                        }
                    }

                    _ = self.shutdown.cancelled() => { break; }

                }
            }

            Ok(())
        }
        .boxed()
    }
}

#[derive(Debug, thiserror::Error)]
enum SyncError {
    #[error("Unable to fetch target peer from network layer")]
    UnableToFetchTargetPeer,

    #[error("Unable to parse subnet id")]
    // TODO: Check if needed after full merge of grpc over p2p
    #[allow(unused)]
    UnableToParseSubnetId,

    #[error(transparent)]
    GrpcParsingError(#[from] GrpcParsingError),

    #[error(transparent)]
    CertificateConversion(#[from] topos_core::api::grpc::shared::v1_conversions_certificate::Error),

    #[error(transparent)]
    SubnetConversion(#[from] topos_core::api::grpc::shared::v1_conversions_subnet::Error),

    #[error(transparent)]
    Store(#[from] StorageError),

    #[error(transparent)]
    Network(#[from] P2PError),

    #[error(transparent)]
    Grpc(#[from] Status),
}

impl CheckpointSynchronizer {
    async fn ask_for_checkpoint(
        &self,
        peer: PeerId,
    ) -> Result<HashMap<SubnetId, Vec<ProofOfDelivery>>, SyncError> {
        let request_id = Uuid::new_v4();

        let checkpoint: Vec<grpc::tce::v1::ProofOfDelivery> = {
            let certificate_ids = self
                .store
                .get_checkpoint()?
                .values()
                .map(|head| head.certificate_id)
                .collect::<Vec<_>>();

            self.store
                .get_certificates(&certificate_ids[..])?
                .into_iter()
                .filter_map(|value| {
                    value.map(|delivered_certificate| delivered_certificate.proof_of_delivery)
                })
                .map(Into::into)
                .collect()
        };

        debug!(
            "Asking {} for latest checkpoint (request_id: {}), with local checkpoint: {:?}",
            peer, request_id, checkpoint
        );

        let req = CheckpointRequest {
            request_id: Some(request_id.into()),
            checkpoint,
        };

        let mut client: SynchronizerServiceClient<_> = self
            .network
            .new_grpc_client::<SynchronizerServiceClient<_>, SynchronizerServiceServer<SynchronizerService>>(peer)
            .await?;

        let response: CheckpointResponse = client.fetch_checkpoint(req).await?.into_inner();

        let diff = response
            .checkpoint_diff
            .into_iter()
            .map(|v| {
                let subnet =
                    SubnetId::from_str(&v.key[..]).map_err(|_| SyncError::UnableToParseSubnetId)?;

                let proofs = v
                    .value
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<_, SyncError>((subnet, proofs))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        Ok(diff)
    }

    fn insert_unverified_proofs(
        &self,
        diff: HashMap<SubnetId, Vec<ProofOfDelivery>>,
    ) -> Result<Vec<Vec<CertificateId>>, SyncError> {
        let mut certs: HashSet<CertificateId> = HashSet::new();
        for (subnet, proofs) in diff {
            let len = proofs.len();
            let unverified_certs = self.store.insert_unverified_proofs(proofs)?;

            debug!(
                "Persist {} unverified proof of delivery for {}",
                len, subnet
            );
            certs.extend(&unverified_certs[..]);
        }

        // Chunk certs
        let mut chunked_certs: Vec<Vec<CertificateId>> = vec![];

        let certs = certs.into_iter().collect::<Vec<_>>();

        for certs in certs.chunks(10) {
            chunked_certs.push(certs.to_vec());
        }

        Ok(chunked_certs)
    }

    async fn fetch_certificates(
        &self,
        certificate_ids: Vec<CertificateId>,
    ) -> Result<Vec<Certificate>, SyncError> {
        let target_peer = self
            .network
            .random_known_peer()
            .await
            .map_err(|_| SyncError::UnableToFetchTargetPeer)?;

        let request_id: Option<APIUuid> = Some(Uuid::new_v4().into());
        let req = FetchCertificatesRequest {
            request_id,
            certificates: certificate_ids
                .iter()
                .map(|cert| (*cert.as_array()).into())
                .collect(),
        };

        debug!(
            "Ask {} for certificates payload: {:?}",
            target_peer, certificate_ids
        );
        let mut client: SynchronizerServiceClient<_> = self
            .network
            .new_grpc_client::<SynchronizerServiceClient<_>, SynchronizerServiceServer<SynchronizerService>>(target_peer)
            .await?;

        let response = client.fetch_certificates(req).await?.into_inner();

        let certificates: Result<Vec<Certificate>, _> = response
            .certificates
            .into_iter()
            .map(TryInto::try_into)
            .collect();

        Ok(certificates?)
    }

    async fn initiate_request(&mut self) -> Result<(), SyncError> {
        //  1. Ask a random peer for the diff between local and its latest checkpoint
        let target_peer = self
            .network
            .random_known_peer()
            .await
            .map_err(|_| SyncError::UnableToFetchTargetPeer)?;

        let diff = self.ask_for_checkpoint(target_peer).await?;

        let certificates_to_catchup = self.insert_unverified_proofs(diff)?;
        info!("Certificates to catchup: {}", certificates_to_catchup.len());

        for certificates in certificates_to_catchup {
            let certificates = self.fetch_certificates(certificates).await?;

            // TODO: verify every certificates
            for certificate in certificates {
                let store = self.store.clone();
                tokio::spawn(async move {
                    // Validate
                    // Check precedence
                    let certificate_id = certificate.id;
                    match store.synchronize_certificate(certificate).await {
                        Ok(_) => debug!("Certificate {} synchronized", certificate_id),
                        Err(StorageError::InternalStorage(topos_tce_storage::errors::InternalStorageError::CertificateAlreadyExists)) => {}
                        Err(e) => error!("Failed to sync because of: {:?}", e),
                    }
                });
            }
        }
        Ok(())
    }
}

pub enum CheckpointsCollectorEvent {}
