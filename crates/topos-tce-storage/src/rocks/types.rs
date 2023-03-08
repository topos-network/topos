use serde::{Deserialize, Serialize};
use topos_core::uci::{Certificate, CertificateId};

use crate::{Position, SubnetId};

use super::db_column::DBColumn;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TargetStreamPositionKey(
    // Target subnet id
    pub(crate) SubnetId,
    // Source subnet id
    pub(crate) SubnetId,
    // Position
    pub(crate) Position,
);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StorageTargetStreamPrefix(
    // Target subnet id
    pub(crate) SubnetId,
    // Source subnet id
    pub(crate) SubnetId,
);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct SourceStreamPositionKey(
    // Source subnet id
    pub(crate) SubnetId,
    // Source certificate position
    pub(crate) Position,
);

/// Column that keeps certificates that are not yet delivered
pub(crate) type PendingCertificatesColumn = DBColumn<u64, Certificate>;
/// Column that keeps list of all certificates retrievable by their id
pub(crate) type CertificatesColumn = DBColumn<CertificateId, Certificate>;
/// Column that keeps list of certificates received from particular subnet and
/// maps (source subnet id, source certificate position) to certificate id
pub(crate) type SourceStreamsColumn = DBColumn<SourceStreamPositionKey, CertificateId>;
/// Column that keeps list of certificates that are delivered to target subnet,
/// and maps their target (target subnet, source subnet and position/count per source subnet)
/// to certificate id
pub(crate) type TargetStreamsColumn = DBColumn<TargetStreamPositionKey, CertificateId>;
/// Keeps position for particular target subnet id <- source subnet id column in TargetStreamsColumn
pub(crate) type TargetSourceListColumn = DBColumn<StorageTargetStreamPrefix, u64>;
