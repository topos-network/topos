use crate::sampler::SubscribersView;
use crate::Errors;
use crate::{
    sampler::SampleType, tce_store::TceStore, DoubleEchoCommand, SubscribersUpdate,
    SubscriptionsView,
};
use opentelemetry::Context;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    time,
};
use tce_transport::{ReliableBroadcastParams, TceEvents};
use tokio::sync::{broadcast, mpsc, oneshot};
use topos_core::uci::{Certificate, CertificateId, DigestCompressed};
use topos_p2p::PeerId;
use tracing::{debug, error, info, info_span, warn, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Processing data associated to a Certificate candidate for delivery
/// Sample repartition, one peer may belongs to multiple samples
/// Ready is always sent to current subscribers view
#[derive(Clone)]
pub struct DeliveryState {
    pub subscriptions: SubscriptionsView,
    ctx: Context,
}

pub struct DoubleEcho {
    params: ReliableBroadcastParams,
    command_receiver: mpsc::Receiver<DoubleEchoCommand>,
    subscriptions_view_receiver: mpsc::Receiver<SubscriptionsView>,
    subscribers_update_receiver: mpsc::Receiver<SubscribersUpdate>,
    event_sender: broadcast::Sender<TceEvents>,
    store: Box<dyn TceStore + Send>,
    cert_candidate: HashMap<CertificateId, (Certificate, DeliveryState)>,
    pending_delivery: HashMap<CertificateId, (Certificate, Context)>,
    span_tracker: HashMap<CertificateId, opentelemetry::Context>,
    all_known_certs: Vec<Certificate>,
    delivery_time: HashMap<CertificateId, (time::SystemTime, time::Duration)>,
    subscriptions: SubscriptionsView, // My subscriptions for echo, ready and delivery feedback
    subscribers: SubscribersView,     // Echo and ready subscribers that are following me
    buffer: VecDeque<(Certificate, Context)>,
    pub(crate) shutdown: mpsc::Receiver<oneshot::Sender<()>>,

    local_peer_id: String,
}

impl DoubleEcho {
    pub const MAX_BUFFER_SIZE: usize = 2048;

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        params: ReliableBroadcastParams,
        command_receiver: mpsc::Receiver<DoubleEchoCommand>,
        subscriptions_view_receiver: mpsc::Receiver<SubscriptionsView>,
        subscribers_update_receiver: mpsc::Receiver<SubscribersUpdate>,
        event_sender: broadcast::Sender<TceEvents>,
        store: Box<dyn TceStore + Send>,
        shutdown: mpsc::Receiver<oneshot::Sender<()>>,
        local_peer_id: String,
    ) -> Self {
        Self {
            params,
            command_receiver,
            subscriptions_view_receiver,
            subscribers_update_receiver,
            event_sender,
            store,
            cert_candidate: Default::default(),
            pending_delivery: Default::default(),
            span_tracker: Default::default(),
            all_known_certs: Default::default(),
            delivery_time: Default::default(),
            subscriptions: SubscriptionsView::default(),
            subscribers: SubscribersView::default(),
            buffer: VecDeque::new(),
            shutdown,
            local_peer_id,
        }
    }

    pub(crate) async fn run(mut self) {
        info!("DoubleEcho started");
        let shutdowned: Option<oneshot::Sender<()>> = loop {
            tokio::select! {
                shutdown = self.shutdown.recv() => {
                        debug!("Double echo shutdown signal received {:?}", shutdown);
                        break shutdown;
                },
                Some(command) = self.command_receiver.recv() => {
                    match command {

                        DoubleEchoCommand::DeliveredCerts { subnet_id, limit, sender } => {

                            debug!("DoubleEchoCommand::DeliveredCerts, subnet_id: {:?}, limit: {}", &subnet_id, &limit);
                            let value = self
                                .store
                                .recent_certificates_for_subnet(&subnet_id, limit)
                                .iter()
                                .filter_map(|cert_id| self.store.cert_by_id(cert_id).ok())
                                .collect();

                            // TODO: Catch send failure
                            let _ = sender.send(Ok(value));
                        }

                        DoubleEchoCommand::BroadcastMany { certificates } if self.buffer.is_empty() => {
                            debug!("DoubleEchoCommand::BroadcastMany cert ids: {:?}",
                                certificates.iter().map(|cert| &cert.id).collect::<Vec<&CertificateId>>());

                            self.buffer = certificates.into_iter().map(|c| (c, Span::current().context())).collect();
                        }

                        DoubleEchoCommand::Broadcast { cert, ctx } => {
                            let span = info_span!(target: "topos", "DoubleEcho buffering", peer_id = self.local_peer_id, certificate_id = cert.id.to_string());
                            span.set_parent(ctx);

                            span.in_scope(||{
                                debug!("DoubleEchoCommand::Broadcast cert_id: {}", cert.id);
                                if self.buffer.len() < Self::MAX_BUFFER_SIZE {
                                    self.span_tracker.insert(cert.id, Span::current().context());
                                    self.buffer.push_back((cert, Span::current().context()));
                                } else {
                                    error!("Double echo buffer is full for certificate {}", cert.id);
                                }
                            });
                        }

                        DoubleEchoCommand::GetSpanOfCert { certificate_id, sender } => {
                            if let Some(ctx) = self.span_tracker.get(&certificate_id) {
                                _ = sender.send(Ok(ctx.clone()));
                            } else {
                                _ = sender.send(Err(Errors::CertificateNotFound));
                            }
                        }

                        command if self.subscriptions.is_some() => {
                            let mut _span = None;
                            match command {
                                DoubleEchoCommand::Echo {from_peer, cert, ctx } => {
                                    let span = info_span!("Handling Echo", peer = self.local_peer_id, certificate_id = cert.id.to_string());
                                    span.set_parent(ctx);
                                    _span = Some(span.entered());
                                    debug!("handling DoubleEchoCommand::Echo from_peer: {} cert_id: {}", &from_peer, cert.id);
                                    self.handle_echo(from_peer, &cert.id);
                                },
                                DoubleEchoCommand::Ready { from_peer, cert, ctx } => {
                                    let span = info_span!("Handling Ready", peer = self.local_peer_id, certificate_id = cert.id.to_string());
                                    span.set_parent(ctx);
                                    _span = Some(span.entered());
                                    debug!("handling DoubleEchoCommand::Ready from_peer: {} cert_id: {}", &from_peer, &cert.id);
                                    self.handle_ready(from_peer, &cert.id);
                                },
                                DoubleEchoCommand::Deliver {  cert, digest, ctx, .. } => {
                                    let span = info_span!("Handling Deliver", peer = self.local_peer_id, certificate_id = cert.id.to_string());
                                    span.set_parent(ctx);
                                    _span = Some(span.entered());
                                    info!("handling DoubleEchoCommand::Deliver cert_id: {} digest: {:?}", cert.id, &digest);
                                    self.handle_deliver(cert, digest)
                                },


                                _ => {}
                            }

                            self.state_change_follow_up();
                        }
                        command => {
                            warn!("Received a command {command:?} when no subscriptions");
                        }
                    }
                }

                Some(new_subscriptions_view) = self.subscriptions_view_receiver.recv() => {
                    info!("new sample view received {:?}", &new_subscriptions_view);
                    self.subscriptions = new_subscriptions_view;
                }

                Some(new_subscribers_update) = self.subscribers_update_receiver.recv() => {
                    info!( peer_id = self.local_peer_id,"new subscribers update {:?}", &new_subscribers_update);
                    match new_subscribers_update {
                        SubscribersUpdate::NewEchoSubscriber(peer) => {
                            self.subscribers.echo.insert(peer);
                        }
                        SubscribersUpdate::NewReadySubscriber(peer) => {
                            self.subscribers.ready.insert(peer);
                        }
                        SubscribersUpdate::RemoveEchoSubscriber(peer) => {
                            self.subscribers.echo.remove(&peer);
                        }
                        SubscribersUpdate::RemoveEchoSubscribers(peers) => {
                            self.subscribers.echo = self.subscribers.echo.drain().filter(|v| !peers.contains(v)).collect();
                        }
                        SubscribersUpdate::RemoveReadySubscriber(peer) => {
                            self.subscribers.ready.remove(&peer);

                        }
                        SubscribersUpdate::RemoveReadySubscribers(peers) => {
                            self.subscribers.ready = self.subscribers.ready.drain().filter(|v| !peers.contains(v)).collect();
                        }
                    }
                    debug!("Subscribers are now: {:?}", self.subscribers);
                }
                else => {
                    info!("Break tokio loop in double echo");
                    break None;
                }
            };

            // Broadcast next certificate
            if self.subscriptions.is_some() {
                while let Some((cert, ctx)) = self.buffer.pop_front() {
                    let span = info_span!(
                        "DoubleEcho start dispatching",
                        certificate_id = cert.id.to_string(),
                        peer_id = self.local_peer_id,
                        "otel.kind" = "producer"
                    );
                    span.set_parent(ctx);
                    let entry = self.span_tracker.entry(cert.id).or_default();

                    *entry = span.context();
                    span.in_scope(|| {
                        self.handle_broadcast(cert);
                    });
                }
            }
        };

        if let Some(sender) = shutdowned {
            info!("Shutting down p2p double echo...");
            _ = sender.send(());
        } else {
            warn!("Shutting down p2p double echo due to error...");
        }
    }
}

impl DoubleEcho {
    fn sample_consume_peer(from_peer: &PeerId, state: &mut DeliveryState, sample_type: SampleType) {
        match sample_type {
            SampleType::EchoSubscription => state.subscriptions.echo.remove(from_peer),
            SampleType::ReadySubscription => state.subscriptions.ready.remove(from_peer),
            SampleType::DeliverySubscription => state.subscriptions.delivery.remove(from_peer),
            _ => false,
        };
    }

    fn handle_echo(&mut self, from_peer: PeerId, certificate_id: &CertificateId) {
        if let Some((_certificate, state)) = self.cert_candidate.get_mut(certificate_id) {
            Self::sample_consume_peer(&from_peer, state, SampleType::EchoSubscription);
        }
    }

    fn handle_ready(&mut self, from_peer: PeerId, certificate_id: &CertificateId) {
        if let Some((_certificate, state)) = self.cert_candidate.get_mut(certificate_id) {
            Self::sample_consume_peer(&from_peer, state, SampleType::ReadySubscription);
            Self::sample_consume_peer(&from_peer, state, SampleType::DeliverySubscription);
        }
    }

    fn handle_broadcast(&mut self, cert: Certificate) {
        debug!("handling broadcast of cert_id {}", &cert.id);
        let digest = self
            .store
            .flush_digest_view(&cert.source_subnet_id)
            .unwrap_or_default();

        self.dispatch(cert, digest);
    }

    fn handle_deliver(&mut self, cert: Certificate, digest: DigestCompressed) {
        self.dispatch(cert, digest)
    }

    /// Called to process potentially new certificate:
    /// - either submitted from API ( [tce_transport::TceCommands::Broadcast] command)
    /// - or received through the gossip (first step of protocol exchange)
    fn dispatch(&mut self, cert: Certificate, digest: DigestCompressed) {
        if self.cert_pre_delivery_check(&cert).is_err() {
            error!("Error on the pre cert delivery check");
            return;
        }
        // Don't gossip one cert already gossiped
        if self.cert_candidate.contains_key(&cert.id) {
            return;
        }

        if self.store.cert_by_id(&cert.id).is_ok() {
            return;
        }

        // Gossip the certificate to all my peers
        let gossip_peers = self.gossip_peers();
        info!(
            "dispatching gossip event cert_id: {} to gossip peers {:?}",
            cert.id, &gossip_peers
        );

        let _ = self.event_sender.send(TceEvents::Gossip {
            peers: gossip_peers, // considered as the G-set for erdos-renyi
            cert: cert.clone(),
            digest: digest.clone(),
            ctx: Span::current().context(),
        });

        // Trigger event of new certificate candidate for delivery
        self.start_delivery(cert, digest);
    }

    /// Make gossip peer list from echo and ready
    /// subscribers that listen to me
    fn gossip_peers(&self) -> Vec<PeerId> {
        self.subscriptions
            .get_subscriptions()
            .into_iter()
            .chain(self.subscribers.get_subscribers().into_iter())
            .collect::<HashSet<_>>() //Filter duplicates
            .into_iter()
            .collect()
    }

    fn start_delivery(&mut self, cert: Certificate, digest: DigestCompressed) {
        // To include tracing context in client requests from _this_ app,
        // use `context` to extract the current OpenTelemetry context.
        warn!("🙌 StartDelivery[{}]\t", &cert.id);
        // Add new entry for the new Cert candidate
        match self.delivery_state_for_new_cert(&cert.id) {
            Some(delivery_state) => {
                info!("DeliveryState is : {:?}", delivery_state.subscriptions);

                self.cert_candidate
                    .insert(cert.id, (cert.clone(), delivery_state));
            }
            None => {
                error!("Ill-formed samples");
                let _ = self.event_sender.send(TceEvents::Die);
                return;
            }
        }
        self.all_known_certs.push(cert.clone());
        self.store.new_cert_candidate(&cert, &digest);
        self.delivery_time
            .insert(cert.id, (time::SystemTime::now(), Default::default()));
        // Send Echo to the echo sample
        let echo_peers = self.subscribers.echo.iter().cloned().collect::<Vec<_>>();
        if echo_peers.is_empty() {
            warn!("EchoSubscriber peers set is empty");
            return;
        }

        let _ = self.event_sender.send(TceEvents::Echo {
            peers: echo_peers,
            cert: cert.clone(),
            ctx: Span::current().context(),
        });
    }

    /// Build initial delivery state
    fn delivery_state_for_new_cert(
        &mut self,
        certificate_id: &CertificateId,
    ) -> Option<DeliveryState> {
        let subscriptions = self.subscriptions.clone();
        // check inbound sets are not empty

        if subscriptions.echo.is_empty()
            || subscriptions.ready.is_empty()
            || subscriptions.delivery.is_empty()
        {
            error!(
                "Subscriptions empty: Echo({}), Ready({}), Delivery({})",
                subscriptions.echo.is_empty(),
                subscriptions.ready.is_empty(),
                subscriptions.delivery.is_empty()
            );
            None
        } else {
            let ctx = self
                .span_tracker
                .get(certificate_id)
                .cloned()
                .unwrap_or_else(|| Span::current().context());

            Some(DeliveryState { subscriptions, ctx })
        }
    }

    fn state_change_follow_up(&mut self) {
        debug!("StateChangeFollowUp called");
        let mut state_modified = false;
        let mut gen_evts = Vec::<TceEvents>::new();
        // For all current Cert on processing
        for (certificate_id, (certificate, state_to_delivery)) in &mut self.cert_candidate {
            if !state_to_delivery.subscriptions.ready.is_empty()
                && (is_e_ready(&self.params, state_to_delivery)
                    || is_r_ready(&self.params, state_to_delivery))
            {
                // Fanout the Ready messages to my subscribers
                let readies = self.subscribers.ready.iter().cloned().collect::<Vec<_>>();
                if !readies.is_empty() {
                    gen_evts.push(TceEvents::Ready {
                        peers: readies.clone(),
                        cert: certificate.clone(),
                        ctx: state_to_delivery.ctx.clone(),
                    });
                }
                state_to_delivery.subscriptions.ready.clear();
            }

            if is_ok_to_deliver(&self.params, state_to_delivery) {
                self.pending_delivery.insert(
                    *certificate_id,
                    (certificate.clone(), state_to_delivery.ctx.clone()),
                );
                state_modified = true;
            }

            let echo_missing = self
                .params
                .echo_sample_size
                .checked_sub(state_to_delivery.subscriptions.echo.len())
                .map(|consumed| self.params.echo_threshold.saturating_sub(consumed))
                .unwrap_or(0);
            let ready_missing = self
                .params
                .ready_sample_size
                .checked_sub(state_to_delivery.subscriptions.ready.len())
                .map(|consumed| self.params.ready_threshold.saturating_sub(consumed))
                .unwrap_or(0);
            let delivery_missing = self
                .params
                .delivery_sample_size
                .checked_sub(state_to_delivery.subscriptions.delivery.len())
                .map(|consumed| self.params.delivery_threshold.saturating_sub(consumed))
                .unwrap_or(0);

            debug!("Waiting for {echo_missing} Echo");
            if echo_missing > 0 {
                debug!(
                    "waiting for Echo from {:?}",
                    state_to_delivery.subscriptions.echo
                );
            }
            debug!("Waiting for {ready_missing} Ready");
            if ready_missing > 0 {
                debug!(
                    "waiting for Ready from {:?}",
                    state_to_delivery.subscriptions.ready
                );
            }
            debug!("Waiting for {delivery_missing} Deliver");
            if delivery_missing > 0 {
                debug!(
                    "waiting for Delivery from {:?}",
                    state_to_delivery.subscriptions.delivery
                );
            }
        }

        if state_modified {
            self.cert_candidate
                .retain(|c, _| !self.pending_delivery.contains_key(c));

            let cert_to_pending = self
                .pending_delivery
                .iter()
                .filter(|(_, (c, _))| self.cert_post_delivery_check(c).is_ok())
                .map(|(c, s)| (*c, s.clone()))
                .collect::<HashMap<_, _>>();

            for (certificate_id, (certificate, ctx)) in cert_to_pending {
                let span = info_span!("Delivered");
                span.set_parent(ctx);

                span.in_scope(|| {
                    let mut d = time::Duration::from_millis(0);
                    if let Some((from, duration)) = self.delivery_time.get_mut(&certificate_id) {
                        *duration = from.elapsed().unwrap();
                        d = *duration;

                        info!("certificate delivered {} => {:?}", certificate_id, d);
                    }
                    self.pending_delivery.remove(&certificate_id);
                    debug!("📝 Accepted[{}]\t Delivery time: {:?}", &certificate_id, d);

                    _ = self.event_sender.send(TceEvents::CertificateDelivered {
                        certificate: certificate.clone(),
                    });
                });
            }
        }

        for evt in gen_evts {
            let _ = self.event_sender.send(evt);
        }
    }

    /// Here comes test that can be done before delivery process
    /// in order to avoid going to delivery process for a Cert
    /// that is already known as incorrect
    fn cert_pre_delivery_check(&self, cert: &Certificate) -> Result<(), ()> {
        if cert.check_signature().is_err() {
            error!("Error on the signature");
        }

        if cert.check_proof().is_err() {
            error!("Error on the proof");
        }

        Ok(())
    }

    /// Here comes test that is necessarily done after delivery
    fn cert_post_delivery_check(&self, _cert: &Certificate) -> Result<(), ()> {
        Ok(())
    }
}

// state checkers
fn is_ok_to_deliver(params: &ReliableBroadcastParams, state: &DeliveryState) -> bool {
    match params
        .delivery_sample_size
        .checked_sub(state.subscriptions.delivery.len())
    {
        Some(consumed) => consumed >= params.delivery_threshold,
        None => false,
    }
}

fn is_e_ready(params: &ReliableBroadcastParams, state: &DeliveryState) -> bool {
    match params
        .echo_sample_size
        .checked_sub(state.subscriptions.echo.len())
    {
        Some(consumed) => consumed >= params.echo_threshold,
        None => false,
    }
}

fn is_r_ready(params: &ReliableBroadcastParams, state: &DeliveryState) -> bool {
    match params
        .ready_sample_size
        .checked_sub(state.subscriptions.ready.len())
    {
        Some(consumed) => consumed >= params.ready_threshold,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::FromIterator, usize};
    use test_log::test;

    use super::*;
    use crate::mem_store::TceMemStore;
    // use rand::{distributions::Uniform, Rng};
    use rand::seq::IteratorRandom;
    use tokio::{spawn, sync::broadcast::error::TryRecvError};
    use tracing::Span;

    const PREV_CERTIFICATE_ID: topos_core::uci::CertificateId =
        CertificateId::from_array([4u8; 32]);
    const SOURCE_SUBNET_ID: topos_core::uci::SubnetId = [1u8; 32];

    fn get_sample(peers: &[PeerId], sample_size: usize) -> HashSet<PeerId> {
        let mut rng = rand::thread_rng();
        HashSet::from_iter(peers.iter().cloned().choose_multiple(&mut rng, sample_size))
    }

    fn get_subscriptions_view(peers: &[PeerId], sample_size: usize) -> SubscriptionsView {
        let mut expected_view = SubscriptionsView::default();
        expected_view.echo = get_sample(&peers, sample_size);
        expected_view.ready = get_sample(&peers, sample_size);
        expected_view.delivery = get_sample(&peers, sample_size);
        expected_view
    }

    fn get_subscriber_view(peers: &[PeerId], sample_size: usize) -> SubscribersView {
        let mut expected_view = SubscribersView::default();
        expected_view.echo = get_sample(&peers, sample_size);
        expected_view.ready = get_sample(&peers, sample_size);
        expected_view
    }

    #[test(tokio::test)]
    async fn handle_receiving_sample_view() {
        let (_subscriptions_view_sender, subscriptions_view_receiver) = mpsc::channel(10);
        let (_subscribers_update_sender, subscribers_update_receiver) = mpsc::channel(10);

        // Network parameters
        let nb_peers = 100;
        let subscription_sample_size = 10;
        let subscriber_sample_size = 8;
        let g = |a, b| ((a as f32) * b) as usize;
        let broadcast_params = ReliableBroadcastParams {
            echo_threshold: g(subscription_sample_size, 0.5),
            echo_sample_size: subscription_sample_size,
            ready_threshold: g(subscription_sample_size, 0.5),
            ready_sample_size: subscription_sample_size,
            delivery_threshold: g(subscription_sample_size, 0.5),
            delivery_sample_size: subscription_sample_size,
        };

        // List of peers
        let mut peers = Vec::new();
        for i in 0..nb_peers {
            let peer = topos_p2p::utils::local_key_pair(Some(i))
                .public()
                .to_peer_id();
            peers.push(peer);
        }

        let expected_subscriptions_view = get_subscriptions_view(&peers, subscription_sample_size);
        let expected_subscriber_view = get_subscriber_view(&peers, subscriber_sample_size);

        // Double Echo
        let (_cmd_sender, cmd_receiver) = mpsc::channel(10);
        let (event_sender, mut event_receiver) = broadcast::channel(10);
        let (_double_echo_shutdown_sender, double_echo_shutdown_receiver) =
            mpsc::channel::<oneshot::Sender<()>>(1);
        let mut double_echo = DoubleEcho::new(
            broadcast_params.clone(),
            cmd_receiver,
            subscriptions_view_receiver,
            subscribers_update_receiver,
            event_sender,
            Box::new(TceMemStore::default()),
            double_echo_shutdown_receiver,
            String::new(),
        );

        assert!(double_echo.subscriptions.is_none());
        double_echo.subscriptions = expected_subscriptions_view.clone();
        double_echo.subscribers = expected_subscriber_view;

        let le_cert = Certificate::new(
            PREV_CERTIFICATE_ID,
            SOURCE_SUBNET_ID,
            Default::default(),
            Default::default(),
            &vec![],
            0,
        )
        .unwrap();
        double_echo.handle_broadcast(le_cert.clone());

        assert_eq!(event_receiver.len(), 2);

        assert!(matches!(
            event_receiver.try_recv(),
            Ok(TceEvents::Gossip { peers, .. }) if peers.len() == double_echo.gossip_peers().len()
        ));

        assert!(matches!(
            event_receiver.try_recv(),
            Ok(TceEvents::Echo { peers, .. }) if peers.len() == subscriber_sample_size));

        assert!(matches!(
            event_receiver.try_recv(),
            Err(TryRecvError::Empty)
        ));

        // Echo phase
        let echo_subscription = expected_subscriptions_view
            .echo
            .into_iter()
            .take(broadcast_params.echo_threshold)
            .collect::<Vec<_>>();

        let id = le_cert.id;
        // Send just enough Echo to reach the threshold
        for p in echo_subscription {
            double_echo.handle_echo(p.clone(), &id);
        }

        assert_eq!(event_receiver.len(), 0);

        double_echo.state_change_follow_up();
        assert_eq!(event_receiver.len(), 1);

        assert!(matches!(
            event_receiver.try_recv(),
            Ok(TceEvents::Ready { peers, .. }) if peers.len() == subscriber_sample_size
        ));

        // Ready phase
        let delivery_subscription = expected_subscriptions_view
            .delivery
            .into_iter()
            .take(broadcast_params.delivery_threshold)
            .collect::<Vec<_>>();

        // Send just enough Ready to reach the delivery threshold
        for p in delivery_subscription {
            double_echo.handle_ready(p.clone(), &le_cert.id);
        }

        assert_eq!(event_receiver.len(), 0);

        double_echo.state_change_follow_up();
        assert_eq!(event_receiver.len(), 1);

        assert!(matches!(
            event_receiver.try_recv(),
            Ok(TceEvents::CertificateDelivered { certificate }) if certificate == le_cert
        ));
    }

    #[test(tokio::test)]
    async fn buffering_certificate() {
        let (subscriptions_view_sender, subscriptions_view_receiver) = mpsc::channel(30);
        let (subscribers_update_sender, subscribers_update_receiver) = mpsc::channel(30);

        // Network parameters
        let nb_peers = 100;
        let subscription_sample_size = 10;
        let subscriber_sample_size = 8;
        let g = |a, b| ((a as f32) * b) as usize;
        let broadcast_params = ReliableBroadcastParams {
            echo_threshold: g(subscription_sample_size, 0.5),
            echo_sample_size: subscription_sample_size,
            ready_threshold: g(subscription_sample_size, 0.5),
            ready_sample_size: subscription_sample_size,
            delivery_threshold: g(subscription_sample_size, 0.5),
            delivery_sample_size: subscription_sample_size,
        };

        // List of peers
        let mut peers = Vec::new();
        for i in 0..nb_peers {
            let peer = topos_p2p::utils::local_key_pair(Some(i))
                .public()
                .to_peer_id();
            peers.push(peer);
        }

        let expected_subscriptions_view = get_subscriptions_view(&peers, subscription_sample_size);
        let expected_subscriber_view = get_subscriber_view(&peers, subscriber_sample_size);

        // Double Echo
        let (cmd_sender, cmd_receiver) = mpsc::channel(10);
        let (event_sender, mut event_receiver) = broadcast::channel(10);
        let (double_echo_shutdown_sender, double_echo_shutdown_receiver) =
            mpsc::channel::<oneshot::Sender<()>>(1);
        let double_echo = DoubleEcho::new(
            broadcast_params.clone(),
            cmd_receiver,
            subscriptions_view_receiver,
            subscribers_update_receiver,
            event_sender,
            Box::new(TceMemStore::default()),
            double_echo_shutdown_receiver,
            String::new(),
        );

        spawn(double_echo.run());

        // Add subscribers
        for peer in expected_subscriber_view.echo.clone() {
            subscribers_update_sender
                .send(SubscribersUpdate::NewEchoSubscriber(peer.clone()))
                .await
                .expect("Added new subscriber");
        }
        for peer in expected_subscriber_view.ready.clone() {
            subscribers_update_sender
                .send(SubscribersUpdate::NewReadySubscriber(peer.clone()))
                .await
                .expect("Added new subscriber");
        }
        // Wait to receive subscribers
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        let le_cert = Certificate::default();
        cmd_sender
            .send(DoubleEchoCommand::Broadcast {
                cert: le_cert.clone(),
                ctx: Span::current().context(),
            })
            .await
            .expect("Cannot send broadcast command");

        assert_eq!(event_receiver.len(), 0);
        subscriptions_view_sender
            .send(expected_subscriptions_view.clone())
            .await
            .expect("Cannot send expected view");

        let mut received_gossip_commands: Vec<(HashSet<PeerId>, Certificate)> = Vec::new();
        let assertion = async {
            loop {
                while let Ok(event) = event_receiver.try_recv() {
                    if let TceEvents::Gossip { peers, cert, .. } = event {
                        received_gossip_commands.push((peers.into_iter().collect(), cert));
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        };

        let _ = tokio::time::timeout(std::time::Duration::from_millis(1000), assertion).await;

        debug!(
            "ECHO SUBSCRIBERS: {:#?}\n READY SUBCRIBERS {:#?}\n RECEIVED {:#?}",
            &expected_subscriber_view.echo,
            &expected_subscriber_view.ready,
            received_gossip_commands[0].0
        );
        // Check if gossip Event is sent to all peers
        let mut all_gossip_peers: HashSet<PeerId> = expected_subscriptions_view
            .get_subscriptions()
            .into_iter()
            .collect();
        all_gossip_peers.extend(expected_subscriber_view.echo);
        all_gossip_peers.extend(expected_subscriber_view.ready);

        assert_eq!(received_gossip_commands.len(), 1);
        assert_eq!(received_gossip_commands[0].0, all_gossip_peers);
        assert_eq!(received_gossip_commands[0].1.id, le_cert.id);

        // Test shutdown
        info!("Waiting for double echo to shutdown...");
        let (sender, receiver) = oneshot::channel();
        double_echo_shutdown_sender
            .send(sender)
            .await
            .expect("Valid shutdown signal sending");
        assert_eq!(receiver.await, Ok(()));
    }
}
