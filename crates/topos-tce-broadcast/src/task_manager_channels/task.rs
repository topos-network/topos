use tokio::sync::mpsc;

use topos_core::uci::CertificateId;

use crate::task_manager_channels::Thresholds;
use crate::DoubleEchoCommand;

#[derive(Debug, PartialEq, Eq)]
pub enum Events {
    ReachedThresholdOfReady(CertificateId),
    ReceivedEcho(CertificateId),
    TimeOut(CertificateId),
}

#[derive(Debug)]
pub struct TaskCompletion {
    pub(crate) success: bool,
    pub(crate) certificate_id: CertificateId,
}

impl TaskCompletion {
    fn success(certificate_id: CertificateId) -> Self {
        TaskCompletion {
            success: true,
            certificate_id,
        }
    }

    #[allow(dead_code)]
    fn failure(certificate_id: CertificateId) -> Self {
        TaskCompletion {
            success: false,
            certificate_id,
        }
    }
}

#[derive(Clone)]
pub struct TaskContext {
    pub certificate_id: CertificateId,
    pub message_sender: mpsc::Sender<DoubleEchoCommand>,
}

pub struct Task {
    pub message_receiver: mpsc::Receiver<DoubleEchoCommand>,
    pub certificate_id: CertificateId,
    pub completion_sender: mpsc::Sender<TaskCompletion>,
    pub event_sender: mpsc::Sender<Events>,
    pub thresholds: Thresholds,
}

impl Task {
    pub fn new(
        certificate_id: CertificateId,
        completion_sender: mpsc::Sender<TaskCompletion>,
        event_sender: mpsc::Sender<Events>,
        thresholds: Thresholds,
    ) -> (Self, TaskContext) {
        let (message_sender, message_receiver) = mpsc::channel(1024);
        let task_context = TaskContext {
            certificate_id,
            message_sender,
        };

        let task = Task {
            message_receiver,
            certificate_id,
            completion_sender,
            event_sender,
            thresholds,
        };

        (task, task_context)
    }

    async fn handle_msg(&mut self, msg: DoubleEchoCommand) -> Result<bool, ()> {
        match msg {
            DoubleEchoCommand::Echo { certificate_id, .. } => {
                let _ = self
                    .completion_sender
                    .send(TaskCompletion::success(certificate_id))
                    .await;

                let _ = self
                    .event_sender
                    .send(Events::ReachedThresholdOfReady(self.certificate_id))
                    .await;

                Ok(true)
            }
            DoubleEchoCommand::Ready { .. } => Ok(true),
            DoubleEchoCommand::Broadcast { .. } => Ok(false),
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.message_receiver.recv() => if let Ok(true) = self.handle_msg(msg).await {
                    break;
                }
            }
        }
    }
}
