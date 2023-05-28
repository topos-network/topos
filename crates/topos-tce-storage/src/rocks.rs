use std::collections::HashMap;
use std::{
    fmt::Debug,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use topos_core::uci::{Certificate, CertificateId, CERTIFICATE_ID_LENGTH};
use tracing::warn;

use crate::{
    errors::InternalStorageError, CertificatePositions, CertificateSourceStreamPosition,
    CertificateTargetStreamPosition, PendingCertificateId, Position, SourceHead, Storage, SubnetId,
};

use self::{db::DB, db_column::DBColumn, iterator::ColumnIterator};
use self::{
    db::{init_db, RocksDB},
    map::Map,
};

pub(crate) mod constants;
pub(crate) mod db;
pub(crate) mod db_column;
pub(crate) mod iterator;
pub(crate) mod map;
mod types;

pub(crate) use types::*;

const EMPTY_PREVIOUS_CERT_ID: [u8; CERTIFICATE_ID_LENGTH] = [0u8; CERTIFICATE_ID_LENGTH];

#[derive(Debug)]
pub struct RocksDBStorage {
    pending_certificates: PendingCertificatesColumn,
    certificates: CertificatesColumn,
    source_streams: SourceStreamsColumn,
    target_streams: TargetStreamsColumn,
    target_source_list: TargetSourceListColumn,
    next_pending_id: AtomicU64,
}

impl RocksDBStorage {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn new(
        pending_certificates: PendingCertificatesColumn,
        certificates: CertificatesColumn,
        source_streams: SourceStreamsColumn,
        target_streams: TargetStreamsColumn,
        target_source_list: TargetSourceListColumn,
        next_pending_id: AtomicU64,
    ) -> Self {
        Self {
            pending_certificates,
            certificates,
            source_streams,
            target_streams,
            target_source_list,
            next_pending_id,
        }
    }

    pub fn with_isolation(path: &PathBuf) -> Result<Self, InternalStorageError> {
        Self::setup(&create_and_init(path)?)
    }

    pub fn open(path: &PathBuf) -> Result<Self, InternalStorageError> {
        Self::setup(DB.get_or_try_init(|| create_and_init(path))?)
    }

    fn setup(db: &RocksDB) -> Result<Self, InternalStorageError> {
        let pending_certificates = DBColumn::reopen(db, constants::PENDING_CERTIFICATES);

        let next_pending_id = match pending_certificates.iter()?.last() {
            Some((pending_id, _)) => AtomicU64::new(pending_id),
            None => AtomicU64::new(0),
        };

        Ok(Self {
            pending_certificates,
            certificates: DBColumn::reopen(db, constants::CERTIFICATES),
            source_streams: DBColumn::reopen(db, constants::SOURCE_STREAMS),
            target_streams: DBColumn::reopen(db, constants::TARGET_STREAMS),
            target_source_list: DBColumn::reopen(db, constants::TARGET_SOURCES),
            next_pending_id,
        })
    }
}

fn create_and_init(path: &PathBuf) -> Result<RocksDB, InternalStorageError> {
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    init_db(path, &mut options)
}

#[async_trait::async_trait]
impl Storage for RocksDBStorage {
    async fn get_pending_certificate(
        &self,
        certificate_id: CertificateId,
    ) -> Result<(PendingCertificateId, Certificate), InternalStorageError> {
        self.pending_certificates
            .iter()?
            .filter(|(_pending_id, cert)| cert.id == certificate_id)
            .collect::<Vec<_>>()
            .first()
            .cloned()
            .ok_or(InternalStorageError::CertificateNotFound(certificate_id))
    }

    async fn add_pending_certificate(
        &self,
        certificate: &Certificate,
    ) -> Result<PendingCertificateId, InternalStorageError> {
        let key = self.next_pending_id.fetch_add(1, Ordering::Relaxed);

        self.pending_certificates.insert(&key, certificate)?;

        Ok(key)
    }

    async fn persist(
        &self,
        certificate: &Certificate,
        pending_certificate_id: Option<PendingCertificateId>,
    ) -> Result<CertificatePositions, InternalStorageError> {
        let mut batch = self.certificates.batch();

        // Inserting the certificate data into the CERTIFICATES cf
        batch = batch.insert_batch(&self.certificates, [(&certificate.id, certificate)])?;

        if let Some(pending_id) = pending_certificate_id {
            match self.pending_certificates.get(&pending_id) {
                Ok(ref pending_certificate) if pending_certificate == certificate => {
                    batch = batch.delete(&self.pending_certificates, pending_id)?;
                }
                Ok(_) => {
                    warn!("PendingCertificateId {} ignored during persist execution: Difference in certificates", pending_id);
                }

                _ => {
                    warn!(
                        "PendingCertificateId {} ignored during persist execution: Not Found",
                        pending_id
                    );
                }
            }
        }

        let source_subnet_position = if certificate.prev_id.as_array() == &EMPTY_PREVIOUS_CERT_ID {
            Position::ZERO
        } else if let Some((SourceStreamPositionKey(_, position), _)) = self
            .source_streams
            .prefix_iter(&certificate.source_subnet_id)?
            .last()
        {
            position.increment().map_err(|error| {
                InternalStorageError::PositionError(error, certificate.source_subnet_id.into())
            })?
        } else {
            // TODO: Better error to define that we were expecting a previous defined position
            return Err(InternalStorageError::CertificateNotFound(
                certificate.prev_id,
            ));
        };

        // Return from function as info
        let source_subnet_stream_position = CertificateSourceStreamPosition {
            source_subnet_id: certificate.source_subnet_id,
            position: Position(source_subnet_position.0),
        };

        // Adding the certificate to the stream
        batch = batch.insert_batch(
            &self.source_streams,
            [(
                SourceStreamPositionKey(certificate.source_subnet_id, source_subnet_position),
                certificate.id,
            )],
        )?;

        // Return list of new target stream positions of certificate that will be persisted
        // Information is needed by sequencer/subnet contract to know from
        // where to continue with streaming on restart
        let mut target_subnet_stream_positions: HashMap<SubnetId, CertificateTargetStreamPosition> =
            HashMap::new();

        // Adding certificate to target_streams
        // TODO: Add expected position instead of calculating on the go
        let mut targets = Vec::new();

        for target_subnet_id in &certificate.target_subnets {
            let target = if let Some((TargetStreamPositionKey(target, source, position), _)) = self
                .target_streams
                .prefix_iter(&TargetSourceListKey(
                    *target_subnet_id,
                    certificate.source_subnet_id,
                ))?
                .last()
            {
                let target_stream_position = TargetStreamPositionKey(
                    target,
                    source,
                    position.increment().map_err(|error| {
                        InternalStorageError::PositionError(
                            error,
                            certificate.source_subnet_id.into(),
                        )
                    })?,
                );
                target_subnet_stream_positions.insert(
                    target_stream_position.0,
                    CertificateTargetStreamPosition {
                        target_subnet_id: target_stream_position.0,
                        source_subnet_id: target_stream_position.1,
                        position: target_stream_position.2,
                    },
                );
                (target_stream_position, certificate.id)
            } else {
                let target_stream_position = TargetStreamPositionKey(
                    *target_subnet_id,
                    certificate.source_subnet_id,
                    Position::ZERO,
                );
                target_subnet_stream_positions.insert(
                    target_stream_position.0,
                    CertificateTargetStreamPosition {
                        target_subnet_id: target_stream_position.0,
                        source_subnet_id: target_stream_position.1,
                        position: target_stream_position.2,
                    },
                );

                (target_stream_position, certificate.id)
            };

            let TargetStreamPositionKey(_, _, position) = &target.0;
            batch = batch.insert_batch(
                &self.target_source_list,
                [(
                    TargetSourceListKey(*target_subnet_id, certificate.source_subnet_id),
                    position.0,
                )],
            )?;
            targets.push(target);
        }

        batch = batch.insert_batch(&self.target_streams, targets)?;

        batch.write()?;

        Ok(CertificatePositions {
            targets: target_subnet_stream_positions,
            source: source_subnet_stream_position,
        })
    }

    async fn update(
        &self,
        _certificate_id: &CertificateId,
        _status: crate::CertificateStatus,
    ) -> Result<(), InternalStorageError> {
        unimplemented!();
    }

    async fn get_source_heads(
        &self,
        subnets: Vec<SubnetId>,
    ) -> Result<Vec<crate::SourceHead>, InternalStorageError> {
        let mut result: Vec<crate::SourceHead> = Vec::new();
        for source_subnet_id in subnets {
            let (position, cert_id) = self
                .source_streams
                .prefix_iter(&source_subnet_id)?
                .last()
                .map(|(source_stream_position, cert_id)| (source_stream_position.1, cert_id))
                .ok_or(InternalStorageError::MissingHeadForSubnet(source_subnet_id))?;
            result.push(SourceHead {
                position,
                cert_id,
                subnet_id: source_subnet_id,
            });
        }
        Ok(result)
    }

    async fn get_certificates(
        &self,
        certificate_ids: Vec<CertificateId>,
    ) -> Result<Vec<Certificate>, InternalStorageError> {
        let mut result = Vec::new();

        for certificate_id in certificate_ids {
            result.push(self.get_certificate(certificate_id).await?);
        }

        Ok(result)
    }

    async fn get_certificate(
        &self,
        certificate_id: CertificateId,
    ) -> Result<Certificate, InternalStorageError> {
        self.certificates.get(&certificate_id)
    }

    async fn get_certificates_by_source(
        &self,
        source_subnet_id: SubnetId,
        from: crate::Position,
        limit: usize,
    ) -> Result<Vec<CertificateId>, InternalStorageError> {
        Ok(self
            .source_streams
            .prefix_iter(&source_subnet_id)?
            // TODO: Find a better way to convert u64 to usize
            .skip(from.0.try_into().unwrap())
            .take(limit)
            .map(|(_, certificate_id)| certificate_id)
            .collect())
    }

    async fn get_certificates_by_target(
        &self,
        target_subnet_id: SubnetId,
        source_subnet_id: SubnetId,
        from: Position,
        limit: usize,
    ) -> Result<Vec<CertificateId>, InternalStorageError> {
        Ok(self
            .target_streams
            .prefix_iter(&(&target_subnet_id, &source_subnet_id))?
            // TODO: Find a better way to convert u64 to usize
            .skip(from.0.try_into().unwrap())
            .take(limit)
            .map(|(_, certificate_id)| certificate_id)
            .collect())
    }

    async fn get_pending_certificates(
        &self,
    ) -> Result<Vec<(u64, Certificate)>, InternalStorageError> {
        Ok(self.pending_certificates.iter()?.collect())
    }
    async fn get_next_pending_certificate(
        &self,
        starting_at: Option<usize>,
    ) -> Result<(PendingCertificateId, Certificate), InternalStorageError> {
        Ok(self
            .pending_certificates
            .iter()?
            .nth(starting_at.map(|v| v + 1).unwrap_or(0))
            .ok_or(InternalStorageError::NoPendingCertificates)?)
    }

    async fn remove_pending_certificate(&self, index: u64) -> Result<(), InternalStorageError> {
        self.pending_certificates.delete(&index)
    }

    async fn get_target_stream_iterator(
        &self,
        target: SubnetId,
        source: SubnetId,
        position: Position,
    ) -> Result<ColumnIterator<'_, TargetStreamPositionKey, CertificateId>, InternalStorageError>
    {
        Ok(self.target_streams.prefix_iter_at(
            &(&target, &source),
            &TargetStreamPositionKey(target, source, position),
        )?)
    }

    async fn get_source_list_by_target(
        &self,
        target: SubnetId,
    ) -> Result<Vec<SubnetId>, InternalStorageError> {
        Ok(self
            .target_source_list
            .prefix_iter(&target)?
            .map(|(TargetSourceListKey(_, k), _)| k)
            .collect())
    }
}

#[cfg(test)]
impl RocksDBStorage {
    pub(crate) fn pending_certificates_column(&self) -> PendingCertificatesColumn {
        self.pending_certificates.clone()
    }

    pub(crate) fn certificates_column(&self) -> CertificatesColumn {
        self.certificates.clone()
    }

    pub(crate) fn source_streams_column(&self) -> SourceStreamsColumn {
        self.source_streams.clone()
    }

    pub(crate) fn target_streams_column(&self) -> TargetStreamsColumn {
        self.target_streams.clone()
    }
}
