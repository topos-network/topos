use std::{future::IntoFuture, sync::Arc, time::Duration};

use rstest::rstest;
use tokio::{spawn, sync::broadcast};
use topos_core::uci::Certificate;
use topos_crypto::{messages::MessageSigner, validator_id::ValidatorId};
use topos_metrics::DOUBLE_ECHO_EVENT_CHANNEL;
use topos_tce_storage::validator::ValidatorStore;
use topos_test_sdk::{
    certificates::create_certificate_chain,
    constants::{SOURCE_SUBNET_ID_1, TARGET_SUBNET_ID_1},
    crypto::message_signer,
    storage::create_validator_store,
};

use crate::{
    double_echo::broadcast_state::BroadcastState, event::ProtocolEvents,
    sampler::SubscriptionsView, task_manager::task::Task,
};

#[rstest]
#[test_log::test(tokio::test)]
#[timeout(Duration::from_secs(1))]
async fn start_with_ungossiped_cert(
    #[future(awt)]
    #[from(create_validator_store)]
    validatore_store: Arc<ValidatorStore>,
    message_signer: Arc<MessageSigner>,
) {
    let certificate = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 1)
        .pop()
        .unwrap()
        .certificate;
    let certificate_id = certificate.id;
    let validator_id = ValidatorId::default();
    let thresholds = topos_config::tce::broadcast::ReliableBroadcastParams {
        echo_threshold: 1,
        ready_threshold: 1,
        delivery_threshold: 1,
    };
    let (event_sender, mut event_receiver) =
        topos_metrics::channels::mpsc::channel(2, &DOUBLE_ECHO_EVENT_CHANNEL);
    let (broadcast_sender, _) = broadcast::channel(1);
    let need_gossip = true;
    let subscriptions = SubscriptionsView::default();

    let broadcast_state = BroadcastState::new(
        certificate,
        validator_id,
        thresholds.echo_threshold,
        thresholds.ready_threshold,
        thresholds.delivery_threshold,
        event_sender,
        subscriptions,
        need_gossip,
        message_signer,
    );

    let (task, _ctx) = Task::new(
        certificate_id,
        broadcast_state,
        validatore_store,
        broadcast_sender,
    );

    let _handle = spawn(task.into_future());

    let event = event_receiver.recv().await;
    assert!(matches!(
            event,
        Some(ProtocolEvents::Broadcast {
            certificate_id: id
        }) if id == certificate_id
    ));

    let event = event_receiver.recv().await;
    assert!(matches!(
            event,
        Some(ProtocolEvents::Gossip {
            cert: Certificate { id, .. }
        }) if id == certificate_id
    ));
}
