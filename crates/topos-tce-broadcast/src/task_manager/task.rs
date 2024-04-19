use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;

use topos_core::types::stream::Position;
use topos_core::uci::CertificateId;
use topos_tce_storage::errors::StorageError;
use topos_tce_storage::store::{ReadStore, WriteStore};
use topos_tce_storage::types::CertificateDeliveredWithPositions;
use topos_tce_storage::validator::ValidatorStore;
use tracing::{debug, error};

use crate::double_echo::broadcast_state::{BroadcastState, Status};
use crate::{DoubleEchoCommand, TaskStatus};

#[derive(Debug)]
pub struct TaskContext {
    pub sink: tokio::sync::mpsc::Sender<DoubleEchoCommand>,
    pub shutdown_sender: tokio::sync::mpsc::Sender<()>,
}

pub struct Task {
    pub validator_store: Arc<ValidatorStore>,
    pub message_receiver: tokio::sync::mpsc::Receiver<DoubleEchoCommand>,
    pub certificate_id: CertificateId,
    pub broadcast_state: BroadcastState,
    pub shutdown_receiver: tokio::sync::mpsc::Receiver<()>,
    broadcast_sender: broadcast::Sender<CertificateDeliveredWithPositions>,
}

impl Task {
    pub fn new(
        certificate_id: CertificateId,
        broadcast_state: BroadcastState,
        validator_store: Arc<ValidatorStore>,
        broadcast_sender: broadcast::Sender<CertificateDeliveredWithPositions>,
    ) -> (Task, TaskContext) {
        let (message_sender, message_receiver) = tokio::sync::mpsc::channel(10_024);
        let (shutdown_sender, shutdown_receiver) = tokio::sync::mpsc::channel(1);

        let task_context = TaskContext {
            sink: message_sender,
            shutdown_sender,
        };

        let task = Task {
            validator_store,
            message_receiver,
            certificate_id,
            broadcast_state,
            shutdown_receiver,
            broadcast_sender,
        };

        (task, task_context)
    }

    pub async fn persist(&self) -> Result<CertificateDeliveredWithPositions, StorageError> {
        let certificate_delivered = self.broadcast_state.into_delivered();

        let positions = self
            .validator_store
            .insert_certificate_delivered(&certificate_delivered)
            .await?;

        Ok(CertificateDeliveredWithPositions(
            certificate_delivered,
            positions,
        ))
    }
}

impl IntoFuture for Task {
    type Output = (CertificateId, TaskStatus);

    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'static>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            // When the task starts, we need to gather information such as current stream position
            // for the source subnet in order to expect its position
            let expected_position = match self.validator_store.last_delivered_position_for_subnet(
                &self.broadcast_state.certificate.source_subnet_id,
            ) {
                Ok(Some(stream_position)) => stream_position.position.increment().unwrap(),
                Ok(None) => Position::ZERO,
                Err(_) => return (self.certificate_id, TaskStatus::Failure),
            };

            debug!(
                "Expected position for Certificate {} is {:?} for the subnet {}",
                self.certificate_id,
                expected_position,
                self.broadcast_state.certificate.source_subnet_id
            );
            self.broadcast_state.expected_position = Some(expected_position);

            loop {
                tokio::select! {
                    Some(msg) = self.message_receiver.recv() => {
                        match msg {
                            DoubleEchoCommand::Echo { validator_id, .. } => {
                                if let Some(Status::DeliveredWithReadySent) = self.broadcast_state.apply_echo(validator_id) {
                                    match self.persist().await {
                                        Ok(delivered) => {
                                            _ = self.broadcast_sender.send(delivered);

                                            return (self.certificate_id, TaskStatus::Success);
                                        }
                                        Err(error) => {
                                            error!("Unable to persist one delivered certificate: {:?}", error);
                                            return (self.certificate_id, TaskStatus::Failure);
                                        }
                                    }

                                }
                            }
                            DoubleEchoCommand::Ready { validator_id, .. } => {
                                if let Some(Status::DeliveredWithReadySent) = self.broadcast_state.apply_ready(validator_id) {
                                    match self.persist().await {
                                        Ok(delivered) => {
                                            _ = self.broadcast_sender.send(delivered);

                                            return (self.certificate_id, TaskStatus::Success);
                                        }
                                        Err(error) => {
                                            error!("Unable to persist one delivered certificate: {:?}", error);
                                            return (self.certificate_id, TaskStatus::Failure);
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    _ = self.shutdown_receiver.recv() => {
                        debug!("Received shutdown, shutting down task {:?}", self.certificate_id);
                        return (self.certificate_id, TaskStatus::Failure)
                    }
                }
            }
        })
    }
}
