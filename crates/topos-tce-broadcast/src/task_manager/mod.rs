use std::collections::HashMap;
use tokio::{spawn, sync::mpsc};

use topos_core::uci::CertificateId;

mod task;

use crate::DoubleEchoCommand;
use task::{Task, TaskContext};

struct TaskManager {
    receiver: mpsc::Receiver<DoubleEchoCommand>,
    task_completion: mpsc::Receiver<(bool, CertificateId)>,
    task_context: HashMap<CertificateId, TaskContext>,
}

impl TaskManager {
    async fn run(mut self, task_completion_sender: mpsc::Sender<(bool, CertificateId)>) {
        loop {
            tokio::select! {
                // Some(task_completion) = self.tasks_futures.next() => {
                Some(task_completion) = self.task_completion.recv() => {
                    println!("Task completed {:?}", task_completion);
                    match task_completion {
                        (true, certificate_id) => {
                            self.task_context.remove(&certificate_id);
                        }
                        (false, certificate_id) => {
                            self.task_context.remove(&certificate_id);
                        }
                    }


                }

                Some(msg) = self.receiver.recv() => {
                    // Check if we have task for this certificate_id
                    //      -> if yes forward the message
                    //      -> if no, create a new task and forward the message
                    match msg {
                        DoubleEchoCommand::Echo { certificate_id, .. } | DoubleEchoCommand::Ready{ certificate_id, .. } => {
                            if let Some(task_context) = self.task_context.get(&certificate_id) {

                                let sender = task_context.message_sender.clone();
                                spawn(async move {
                                    _ = sender.send(msg).await;
                                });
                                // task_context.sender.send(msg).await;
                                // Forward the message
                            } else {
                                // Create a new task
                                let (task, context) = Task::new(certificate_id, task_completion_sender.clone());

                                spawn(task.run());

                                let sender = context.message_sender.clone();
                                spawn(async move {
                                    _ = sender.send(msg).await;
                                });
                                self.task_context.insert(certificate_id, context);
                            }
                        }
                        _ => todo!()
                    }
                }
            }
        }
    }
}
