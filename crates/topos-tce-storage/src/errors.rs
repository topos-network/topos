use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use topos_core::uci::CertificateId;

use crate::command::StorageCommand;

#[derive(Error, Debug)]
pub enum InternalStorageError {
    #[error("The certificate already exists")]
    CertificateAlreadyExists,

    #[error("Unable to find a certificate: {0}")]
    CertificateNotFound(CertificateId),

    #[error("Unable to start storage")]
    UnableToStartStorage,

    #[cfg(feature = "rocksdb")]
    #[error("Unable to execute query: {0}")]
    RocksDBError(#[from] rocksdb::Error),

    #[cfg(feature = "rocksdb")]
    #[error("Accessing invalid column family: {0}")]
    InvalidColumnFamily(&'static str),

    #[error("Unable to deserialize database value")]
    UnableToDeserializeValue,

    #[error("Invalid query argument: {0}")]
    InvalidQueryArgument(&'static str),

    #[error(transparent)]
    Bincode(#[from] Box<bincode::ErrorKind>),

    #[error("A concurrent DBBatch has been detected")]
    ConcurrentDBBatchDetected,

    #[error("{0}: {1}")]
    PositionError(#[source] PositionError, String),

    #[error("InvalidSubnetId")]
    InvalidSubnetId,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    InternalStorage(#[from] InternalStorageError),

    #[error("Unable to communicate with storage: {0}")]
    CommunicationChannel(#[from] mpsc::error::SendError<StorageCommand>),

    #[error("Unable to communicate with storage: closed")]
    CommunicationChannelClosed,

    #[error("Unable to receive expected response from storage: {0}")]
    ResponseChannel(#[from] oneshot::error::RecvError),
}

#[derive(Debug, Error)]
pub enum PositionError {
    #[error("Maximum position reached for subnet")]
    MaximumPositionReached,
}
