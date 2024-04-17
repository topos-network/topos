use crate::event::ProtocolEvents;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Mutex};
use tokio::{spawn, sync::mpsc};
use tokio_util::sync::CancellationToken;
use topos_config::tce::broadcast::ReliableBroadcastParams;
use topos_core::types::ValidatorId;
use topos_core::uci::Certificate;
use topos_core::uci::CertificateId;
use topos_metrics::CERTIFICATE_PROCESSING_FROM_API_TOTAL;
use topos_metrics::CERTIFICATE_PROCESSING_FROM_GOSSIP_TOTAL;
use topos_metrics::CERTIFICATE_PROCESSING_TOTAL;
use topos_metrics::DOUBLE_ECHO_ACTIVE_TASKS_COUNT;
use topos_tce_storage::store::ReadStore;
use topos_tce_storage::types::CertificateDeliveredWithPositions;
use topos_tce_storage::validator::ValidatorStore;
use topos_tce_storage::PendingCertificateId;
use tracing::{debug, error, info, trace, warn};

pub mod task;

use crate::constant::PENDING_LIMIT_PER_REQUEST_TO_STORAGE;
use crate::double_echo::broadcast_state::BroadcastState;
use crate::sampler::SubscriptionsView;
use crate::DoubleEchoCommand;
use crate::TaskStatus;
use task::{Task, TaskContext};
use topos_crypto::messages::MessageSigner;

type RunningTasks =
    FuturesUnordered<Pin<Box<dyn Future<Output = (CertificateId, TaskStatus)> + Send + 'static>>>;

/// The TaskManager is responsible for receiving messages from the network and distributing them
/// among tasks. These tasks are either created if none for a certain CertificateID exists yet,
/// or existing tasks will receive the messages.
pub struct TaskManager {
    pub message_receiver: mpsc::Receiver<DoubleEchoCommand>,
    pub subscriptions: SubscriptionsView,
    pub event_sender: mpsc::Sender<ProtocolEvents>,
    pub tasks: HashMap<CertificateId, TaskContext>,
    pub message_signer: Arc<MessageSigner>,
    #[allow(clippy::type_complexity)]
    pub running_tasks: RunningTasks,
    pub buffered_messages: HashMap<CertificateId, Vec<DoubleEchoCommand>>,
    pub thresholds: ReliableBroadcastParams,
    pub validator_id: ValidatorId,
    pub validator_store: Arc<ValidatorStore>,
    pub delivered_certficiates: Arc<Mutex<HashSet<CertificateId>>>,
    pub broadcast_sender: broadcast::Sender<CertificateDeliveredWithPositions>,
    pub latest_pending_id: PendingCertificateId,
}

impl TaskManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        message_receiver: mpsc::Receiver<DoubleEchoCommand>,
        subscriptions: SubscriptionsView,
        event_sender: mpsc::Sender<ProtocolEvents>,
        validator_id: ValidatorId,
        thresholds: ReliableBroadcastParams,
        message_signer: Arc<MessageSigner>,
        validator_store: Arc<ValidatorStore>,
        delivered_certficiates: Arc<Mutex<HashSet<CertificateId>>>,
        broadcast_sender: broadcast::Sender<CertificateDeliveredWithPositions>,
    ) -> Self {
        Self {
            message_receiver,
            subscriptions,
            event_sender,
            tasks: HashMap::new(),
            running_tasks: FuturesUnordered::new(),
            buffered_messages: Default::default(),
            validator_id,
            message_signer,
            thresholds,
            validator_store,
            delivered_certficiates,
            broadcast_sender,
            latest_pending_id: 0,
        }
    }

    /// Fetch the next pending certificates from the storage and create tasks for them.
    /// This method is called periodically to check for new pending certificates and when
    /// a task has finished.
    fn next_pending_certificate(&mut self) {
        debug!("Checking for next pending_certificates");
        match self.validator_store.get_next_pending_certificates(
            &self.latest_pending_id,
            *PENDING_LIMIT_PER_REQUEST_TO_STORAGE,
        ) {
            Ok(pendings) => {
                debug!("Received {} pending certificates", pendings.len());
                for (pending_id, certificate) in pendings {
                    self.create_task(&certificate, true, pending_id);
                    self.latest_pending_id = pending_id;
                }
            }
            Err(error) => {
                error!("Failed to fetch the pending certificates: {:?}", error);
            }
        }
    }

    /// Cancel tasks for certificates that have been synced in the background and are considered delivered.
    // async fn cancel_tasks_for_synced_certificates(&mut self) {
    //     let open_cert_ids = self.tasks.keys().cloned().collect::<Vec<_>>();
    //     let delivered_certificates = self.validator_store.get_certificates(&open_cert_ids);
    //
    //     match delivered_certificates {
    //         Ok(delivered_certificates) => {
    //             for certificate in delivered_certificates {
    //                 if let Some(certificate) = certificate {
    //                     if self.tasks.contains_key(&certificate.certificate.id) {
    //                         if let Some(task_context) =
    //                             self.tasks.remove(&certificate.certificate.id)
    //                         {
    //                             task_context.shutdown_sender.send(()).await.unwrap();
    //                             info!("Task for certificate {} has been cancelled; certificate already delivered", certificate.certificate.id);
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         Err(error) => {
    //             error!("Failed to fetch the delivered certificates: {:?}", error);
    //         }
    //     }
    // }

    pub async fn run(mut self, shutdown_receiver: CancellationToken) {
        let mut pending_interval = tokio::time::interval(Duration::from_millis(100));
        // let mut abort_interval = tokio::time::interval(Duration::from_millis(800));

        loop {
            tokio::select! {
                biased;

                _ = pending_interval.tick() => {
                    self.next_pending_certificate();

                }

                // _ = abort_interval.tick() => {
                //     self.cancel_tasks_for_synced_certificates().await;
                // }

                Some(msg) = self.message_receiver.recv() => {
                    match msg {
                        DoubleEchoCommand::Echo { certificate_id, .. } | DoubleEchoCommand::Ready { certificate_id, .. } => {
                            if let Some(task_context) = self.tasks.get(&certificate_id) {
                                _ = task_context.sink.send(msg).await;
                            } else {
                                self.buffered_messages
                                    .entry(certificate_id)
                                    .or_default()
                                    .push(msg);
                            };
                        }
                        DoubleEchoCommand::Broadcast { ref cert, need_gossip, pending_id } => {
                            trace!("Received broadcast message for certificate {} ", cert.id);

                            self.create_task(cert, need_gossip, pending_id)
                        }
                    }
                }


                Some((certificate_id, status)) = self.running_tasks.next() => {
                    if let TaskStatus::Success = status {
                        trace!("Task for certificate {} finished successfully", certificate_id);
                        self.tasks.remove(&certificate_id);
                        self.delivered_certficiates.lock().await.insert(certificate_id);
                        DOUBLE_ECHO_ACTIVE_TASKS_COUNT.dec();
                    } else {
                        error!("Task for certificate {} finished unsuccessfully", certificate_id);
                    }

                    self.next_pending_certificate();
                }

                _ = shutdown_receiver.cancelled() => {
                    info!("Task Manager shutting down");

                    debug!("Remaining active tasks: {:?}", self.tasks.len());
                    if !self.tasks.is_empty() {
                        debug!("Certificates still in broadcast: {:?}", self.tasks.keys());
                    }
                    warn!("Remaining buffered messages: {}", self.buffered_messages.len());
                    for task in self.tasks.iter() {
                        task.1.shutdown_sender.send(()).await.unwrap();
                    }

                    break;
                }
            }
        }
    }

    fn start_task(
        running_tasks: &RunningTasks,
        task: Task,
        sink: mpsc::Sender<DoubleEchoCommand>,
        messages: Option<Vec<DoubleEchoCommand>>,
        need_gossip: bool,
    ) {
        running_tasks.push(task.into_future());

        if let Some(messages) = messages {
            spawn(async move {
                for msg in messages {
                    _ = sink.send(msg).await;
                }
            });
        }

        DOUBLE_ECHO_ACTIVE_TASKS_COUNT.inc();

        CERTIFICATE_PROCESSING_TOTAL.inc();
        if need_gossip {
            CERTIFICATE_PROCESSING_FROM_API_TOTAL.inc();
        } else {
            CERTIFICATE_PROCESSING_FROM_GOSSIP_TOTAL.inc();
        }
    }

    /// Create a new task for the given certificate and add it to the running tasks.
    /// If the previous certificate is not available yet, the task will be created but not started.
    /// This method is called when a pending certificate is fetched from the storage.
    fn create_task(&mut self, cert: &Certificate, need_gossip: bool, pending_id: u64) {
        match self.tasks.entry(cert.id) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                let broadcast_state = BroadcastState::new(
                    cert.clone(),
                    self.validator_id,
                    self.thresholds.echo_threshold,
                    self.thresholds.ready_threshold,
                    self.thresholds.delivery_threshold,
                    self.event_sender.clone(),
                    self.subscriptions.clone(),
                    need_gossip,
                    self.message_signer.clone(),
                );

                let (task, task_context) = Task::new(
                    cert.id,
                    broadcast_state,
                    self.validator_store.clone(),
                    self.broadcast_sender.clone(),
                );

                let prev = self.validator_store.get_certificate(&cert.prev_id);
                if matches!(prev, Ok(Some(_)))
                    || cert.prev_id == topos_core::uci::INITIAL_CERTIFICATE_ID
                {
                    Self::start_task(
                        &self.running_tasks,
                        task,
                        task_context.sink.clone(),
                        self.buffered_messages.remove(&cert.id),
                        need_gossip,
                    );
                } else {
                    debug!(
                        "Received broadcast message for certificate {} but the previous \
                         certificate {} is not available yet",
                        cert.id, cert.prev_id
                    );
                }
                debug!(
                    "Creating task for pending certificate {} at position {} if needed",
                    cert.id, pending_id
                );
                entry.insert(task_context);
            }
            std::collections::hash_map::Entry::Occupied(_) => {
                trace!(
                    "Received broadcast message for certificate {} but it is already being \
                     processed",
                    cert.id
                );
            }
        }
    }
}
