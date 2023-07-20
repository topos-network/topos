use std::collections::HashMap;
use tokio::{spawn, sync::mpsc};

use topos_core::uci::CertificateId;

pub mod task;

use crate::DoubleEchoCommand;
use task::{Task, TaskCompletion, TaskContext};

#[derive(Clone)]
pub struct Thresholds {
    pub echo: usize,
    pub ready: usize,
    pub delivery: usize,
}

/// The TaskManager is responsible for receiving messages from the network and distributing them
/// among tasks. These tasks are either created if none for a certain CertificateID exists yet,
/// or existing tasks will receive the messages.
pub struct TaskManager {
    pub message_receiver: mpsc::Receiver<DoubleEchoCommand>,
    pub task_completion: mpsc::Receiver<TaskCompletion>,
    pub task_context: HashMap<CertificateId, TaskContext>,
    pub thresholds: Thresholds,
}

impl TaskManager {
    pub async fn run(
        mut self,
        task_completion_sender: mpsc::Sender<TaskCompletion>,
        event_sender: mpsc::Sender<task::Events>,
    ) {
        loop {
            tokio::select! {
                // If a task sends a message over the completion channel, it is signalling that it
                // is done and can be removed from the open tasks inside `task_context`
                Some(task_completion) = self.task_completion.recv() => {
                    match task_completion.success {
                        true => {
                            self.task_context.remove(&task_completion.certificate_id);
                        }
                        false => {
                            self.task_context.remove(&task_completion.certificate_id);
                        }
                    }
                }

                Some(msg) = self.message_receiver.recv() => {
                    match msg {
                        DoubleEchoCommand::Echo { certificate_id, .. } | DoubleEchoCommand::Ready{ certificate_id, .. } => {
                            let task_context = match self.task_context.get(&certificate_id) {
                                Some(task_context) => task_context.to_owned(),
                                None => self.create_and_spawn_new_task(certificate_id, task_completion_sender.clone(), event_sender.clone()),
                            };

                            Self::send_message_to_task(task_context, msg).await;
                        }
                        DoubleEchoCommand::Broadcast { ref cert, .. } => {
                            if self.task_context.get(&cert.id).is_none() {
                                let task_context = self.create_and_spawn_new_task(cert.id, task_completion_sender.clone(), event_sender.clone());
                                Self::send_message_to_task(task_context, msg).await;
                            }
                        }
                    }
                }
            }
        }
    }

    fn create_and_spawn_new_task(
        &mut self,
        certificate_id: CertificateId,
        task_completion_sender: mpsc::Sender<TaskCompletion>,
        event_sender: mpsc::Sender<task::Events>,
    ) -> TaskContext {
        let (task, context) = Task::new(
            certificate_id,
            task_completion_sender,
            event_sender,
            self.thresholds.clone(),
        );

        spawn(task.run());

        self.task_context.insert(certificate_id, context.clone());

        context
    }

    async fn send_message_to_task(task_context: TaskContext, msg: DoubleEchoCommand) {
        spawn(async move {
            _ = task_context.message_sender.send(msg).await;
        });
    }
}
