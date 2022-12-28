//! Protocol implementation guts.
//!

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use topos_core::uci::{
    Certificate, CertificateId, CrossChainTransaction, CrossChainTransactionData, SubnetId,
};
use topos_sequencer_types::{BlockInfo, CertificationCommand, CertificationEvent, SubnetEvent};

pub struct Certification {
    pub commands_channel: mpsc::UnboundedSender<CertificationCommand>,
    pub events_subscribers: Vec<mpsc::UnboundedSender<CertificationEvent>>,
    _tx_exit: mpsc::UnboundedSender<()>,

    pub history: HashMap<SubnetId, Vec<CertificateId>>,
    pub finalized_blocks: Vec<BlockInfo>,
    pub subnet_id: SubnetId,
}

impl Debug for Certification {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Certification instance").finish()
    }
}

fn create_cross_chain_transaction(event: &SubnetEvent) -> CrossChainTransaction {
    match event {
        SubnetEvent::TokenSent {
            sender,
            source_subnet_id: _,
            target_subnet_id,
            receiver,
            symbol,
            amount,
        } => CrossChainTransaction {
            target_subnet_id: hex::encode(target_subnet_id),
            sender_addr: hex::encode(sender),
            recipient_addr: hex::encode(receiver),
            transaction_data: CrossChainTransactionData::AssetTransfer {
                asset_id: symbol.clone(),
                amount: *amount,
            },
        },
        SubnetEvent::ContractCall {
            source_subnet_id: _,
            source_contract_addr,
            target_subnet_id,
            target_contract_addr,
            payload_hash: _,
            payload,
        } => CrossChainTransaction {
            target_subnet_id: hex::encode(target_subnet_id),
            sender_addr: "0x".to_string() + hex::encode(source_contract_addr).as_str(),
            recipient_addr: hex::encode(target_contract_addr),
            transaction_data: CrossChainTransactionData::FunctionCall {
                data: payload.clone(),
            },
        },
        SubnetEvent::ContractCallWithToken {
            source_subnet_id: _,
            source_contract_addr,
            target_subnet_id,
            target_contract_addr,
            payload_hash: _,
            payload,
            symbol: _,
            amount: _,
        } => {
            //TODO fix, update Cross chain transaction
            CrossChainTransaction {
                target_subnet_id: hex::encode(target_subnet_id),
                sender_addr: "0x".to_string() + hex::encode(source_contract_addr).as_str(),
                recipient_addr: hex::encode(target_contract_addr),
                transaction_data: CrossChainTransactionData::FunctionCall {
                    data: payload.clone(),
                },
            }
        }
    }
}

impl Certification {
    pub fn spawn_new(subnet_id: SubnetId) -> Result<Arc<Mutex<Certification>>, crate::Error> {
        //config: CertificationConfig) -> Arc<Mutex<Certification>> {
        let (command_sender, mut command_rcv) = mpsc::unbounded_channel::<CertificationCommand>();
        let (_tx_exit, mut rx_exit) = mpsc::unbounded_channel::<()>();

        // Initialize the history
        let mut history = HashMap::new();
        history.insert(subnet_id.clone(), Vec::new());
        let me = Arc::new(Mutex::from(Self {
            commands_channel: command_sender,
            events_subscribers: Vec::new(),
            // todo: implement sync mechanism for the last seen cert
            _tx_exit,
            history,
            finalized_blocks: Vec::<BlockInfo>::new(),
            subnet_id,
        }));
        // spawn running closure
        let me_cl = me.clone();
        let me_c2 = me.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(6)); // arbitrary time for 1 block
            loop {
                interval.tick().await;
                if let Some(cert) = Self::generate_certificate(me_c2.clone()) {
                    Self::send_new_certificate(
                        me_c2.clone(),
                        CertificationEvent::NewCertificate(cert),
                    );
                }
            }
        });

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Poll commands channel
                    cmd = command_rcv.recv() => {
                        Self::on_command(me_cl.clone(), cmd);
                    },
                    Some(_) = rx_exit.recv() => {
                        break;
                    }
                }
            }
        });
        Ok(me)
    }

    fn send_new_certificate(certification: Arc<Mutex<Certification>>, evt: CertificationEvent) {
        let mut certification = certification.lock().unwrap();
        certification.send_out_events(evt);
    }

    fn on_command(certifiation: Arc<Mutex<Certification>>, mb_cmd: Option<CertificationCommand>) {
        let mut certification = certifiation.lock().unwrap();

        match mb_cmd {
            Some(cmd) => match cmd {
                CertificationCommand::AddFinalizedBlock(block_info) => {
                    certification.finalized_blocks.push(block_info);
                    log::debug!(
                        "Finalized blocks mempool updated: {:?}",
                        &certification.finalized_blocks
                    );
                }
            },
            _ => {
                log::warn!("Empty command was passed");
            }
        }
    }

    #[allow(unused)]
    fn send_out_events(&mut self, evt: CertificationEvent) {
        for tx in &self.events_subscribers {
            // FIXME: When error is returned it means that receiving side of the channel is closed
            // Thus we better remove the sender from our subscribers
            let _ = tx.send(evt.clone());
        }
    }

    /// Generation of Certificate
    fn generate_certificate(certification: Arc<Mutex<Certification>>) -> Option<Certificate> {
        let mut certification = certification.lock().unwrap();
        let subnet_id = certification.subnet_id.clone();

        let mut block_data = Vec::<u8>::new();
        let mut cross_chain_calls: Vec<CrossChainTransaction> = Vec::new();
        for block_info in &certification.finalized_blocks {
            block_data.extend_from_slice(&block_info.data);
            for event in &block_info.events {
                cross_chain_calls.push(create_cross_chain_transaction(event));
            }
        }

        // No contents for the Certificate
        if block_data.is_empty() {
            return None;
        }

        // Get the id of the previous Certificate
        let previous_cert_id: CertificateId = match certification.history.get(&subnet_id) {
            Some(certs) => match certs.last() {
                Some(cert_id) => cert_id.clone(),
                None => 0.to_string(),
            },
            None => {
                log::error!("ill-formed subnet history for {:?}", subnet_id);
                return None;
            }
        };

        let certificate = Certificate::new(previous_cert_id, subnet_id.clone(), cross_chain_calls);

        certification.finalized_blocks.clear();
        certification
            .history
            .get_mut(&subnet_id)
            .unwrap()
            .push(certificate.cert_id.clone());

        Some(certificate)
    }
}
