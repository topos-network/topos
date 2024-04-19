use crate::event::ProtocolEvents;
use crate::sampler::SubscriptionsView;
use std::sync::Arc;
use std::{collections::HashSet, time};
use topos_core::{
    types::{
        stream::{CertificateSourceStreamPosition, Position},
        CertificateDelivered, ProofOfDelivery, Ready, ValidatorId,
    },
    uci::Certificate,
};
use topos_crypto::messages::MessageSigner;
use topos_metrics::channels::mpsc;
use topos_metrics::DOUBLE_ECHO_BROADCAST_FINISHED_TOTAL;
use tracing::{debug, error, info, trace};
mod status;

pub use status::Status;

#[derive(Debug)]
pub struct BroadcastState {
    subscriptions_view: SubscriptionsView,
    status: Status,
    pub(crate) certificate: Certificate,
    validator_id: ValidatorId,
    echo_threshold: usize,
    ready_threshold: usize,
    delivery_threshold: usize,
    message_signer: Arc<MessageSigner>,
    event_sender: mpsc::Sender<ProtocolEvents>,
    delivery_time: time::Instant,
    readies: HashSet<Ready>,
    pub(crate) expected_position: Option<Position>,
}

impl BroadcastState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        certificate: Certificate,
        validator_id: ValidatorId,
        echo_threshold: usize,
        ready_threshold: usize,
        delivery_threshold: usize,
        event_sender: mpsc::Sender<ProtocolEvents>,
        subscriptions_view: SubscriptionsView,
        need_gossip: bool,
        message_signer: Arc<MessageSigner>,
    ) -> Self {
        let mut state = Self {
            subscriptions_view,
            status: Status::Pending,
            certificate,
            validator_id,
            echo_threshold,
            ready_threshold,
            delivery_threshold,
            message_signer,
            event_sender,
            delivery_time: time::Instant::now(),
            readies: HashSet::new(),
            expected_position: None,
        };

        _ = state.event_sender.try_send(ProtocolEvents::Broadcast {
            certificate_id: state.certificate.id,
        });

        if need_gossip {
            debug!(
                "📣 Gossiping the Certificate {} from the source subnet {}",
                &state.certificate.id, &state.certificate.source_subnet_id
            );
            let _ = state.event_sender.try_send(ProtocolEvents::Gossip {
                cert: state.certificate.clone(),
            });
        }

        state.update_status();

        state
    }

    pub fn into_delivered(&self) -> CertificateDelivered {
        CertificateDelivered {
            certificate: self.certificate.clone(),
            proof_of_delivery: ProofOfDelivery {
                certificate_id: self.certificate.id,
                delivery_position: CertificateSourceStreamPosition {
                    subnet_id: self.certificate.source_subnet_id,
                    // FIXME: Should never fails but need to find how to remove the unwrap
                    position: self
                        .expected_position
                        .expect("Expected position is not set, this is a bug"),
                },
                readies: self
                    .readies
                    .iter()
                    .cloned()
                    .map(|r| (r, "signature".to_string()))
                    .collect(),
                threshold: self.delivery_threshold as u64,
            },
        }
    }

    pub fn apply_echo(&mut self, validator_id: ValidatorId) -> Option<Status> {
        if self.subscriptions_view.echo.remove(&validator_id) {
            self.update_status()
        } else {
            None
        }
    }

    pub fn apply_ready(&mut self, validator_id: ValidatorId) -> Option<Status> {
        if self.subscriptions_view.ready.remove(&validator_id) {
            self.readies.insert(validator_id.to_string());
            self.update_status()
        } else {
            None
        }
    }

    fn update_status(&mut self) -> Option<Status> {
        // Nothing happened yet, we're in the initial state and didn't process
        // any Echo or Ready messages
        // Sending our Echo message
        if let Status::Pending = self.status {
            let mut payload = Vec::new();
            payload.extend_from_slice(self.certificate.id.as_array());
            payload.extend_from_slice(self.validator_id.as_bytes());

            let _ = self.event_sender.try_send(ProtocolEvents::Echo {
                certificate_id: self.certificate.id,
                signature: self.message_signer.sign_message(&payload).ok()?,
                validator_id: self.validator_id,
            });

            self.status = Status::EchoSent;
            trace!(
                "Certificate {} is now {}",
                &self.certificate.id,
                self.status
            );
            return Some(self.status);
        }

        // Upon reaching the Echo or Ready threshold, if the status is either
        // EchoSent or Delivered (without ReadySent), we send the Ready message
        // and update the status accordingly.
        // If the status was EchoSent, we update it to ReadySent
        // If the status was Delivered, we update it to DeliveredWithReadySent
        if !self.status.is_ready_sent() && self.reached_ready_threshold() {
            let mut payload = Vec::new();
            payload.extend_from_slice(self.certificate.id.as_array());
            payload.extend_from_slice(self.validator_id.as_bytes());

            let event = ProtocolEvents::Ready {
                certificate_id: self.certificate.id,
                signature: self.message_signer.sign_message(&payload).ok()?,
                validator_id: self.validator_id,
            };
            if let Err(e) = self.event_sender.try_send(event) {
                error!("Failed to send the Ready message: {}", e);
            }

            self.status = self.status.ready_sent();

            trace!(
                "Certificate {} is now {}",
                &self.certificate.id,
                self.status
            );
            return Some(self.status);
        }

        // Upon reaching the Delivery threshold, if the status is not Delivered,
        // we update the status to Delivered and change the status
        if !self.status.is_delivered() && self.reached_delivery_threshold() {
            self.status = self.status.delivered();

            trace!(
                "Certificate {} is now {}",
                &self.certificate.id,
                self.status
            );
            // Calculate delivery time
            let from = self.delivery_time;
            let duration = from.elapsed();
            let d = duration;

            info!(
                "📝 Certificate delivered {} with broadcast duration: {:?}",
                self.certificate.id, d
            );

            DOUBLE_ECHO_BROADCAST_FINISHED_TOTAL.inc();

            return Some(self.status);
        }

        None
    }

    fn reached_ready_threshold(&self) -> bool {
        // Compute the threshold
        let reached_echo_threshold = match self
            .subscriptions_view
            .network_size
            .checked_sub(self.subscriptions_view.echo.len())
        {
            Some(consumed) => consumed >= self.echo_threshold,
            None => false,
        };

        let reached_ready_threshold = match self
            .subscriptions_view
            .network_size
            .checked_sub(self.subscriptions_view.ready.len())
        {
            Some(consumed) => consumed >= self.ready_threshold,
            None => false,
        };

        trace!(
            "Certificate {} reached Echo threshold: {} and Ready threshold: {}",
            &self.certificate.id,
            reached_echo_threshold,
            reached_ready_threshold
        );
        // If reached any of the Echo or Ready thresholds, I send the Ready
        reached_echo_threshold || reached_ready_threshold
    }

    fn reached_delivery_threshold(&self) -> bool {
        // If reached the delivery threshold, I can deliver
        let delivery_threshold = match self
            .subscriptions_view
            .network_size
            .checked_sub(self.subscriptions_view.ready.len())
        {
            Some(consumed) => consumed >= self.delivery_threshold,
            None => false,
        };

        trace!(
            "Certificate {} reached Delivery threshold: {}",
            &self.certificate.id,
            delivery_threshold
        );

        delivery_threshold
    }
}
