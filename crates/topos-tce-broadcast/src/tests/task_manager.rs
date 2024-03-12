use std::{str::FromStr, sync::Arc};

use rstest::rstest;
use tokio::{
    spawn,
    sync::{broadcast, mpsc},
};
use tokio_util::sync::CancellationToken;
use topos_crypto::{messages::MessageSigner, validator_id::ValidatorId};
use topos_metrics::DOUBLE_ECHO_ACTIVE_TASKS_COUNT;
use topos_tce_storage::validator::ValidatorStore;
use topos_test_sdk::{
    certificates::create_certificate_chain,
    constants::{SOURCE_SUBNET_ID_1, TARGET_SUBNET_ID_1},
    storage::create_validator_store,
};

use crate::{sampler::SubscriptionsView, task_manager::TaskManager};

#[rstest]
#[tokio::test]
async fn can_start(#[future] create_validator_store: Arc<ValidatorStore>) {
    let validator_store = create_validator_store.await;
    let (message_sender, message_receiver) = mpsc::channel(1);
    let (event_sender, _) = mpsc::channel(1);
    let (broadcast_sender, _) = broadcast::channel(1);
    let shutdown = CancellationToken::new();
    let validator_id = ValidatorId::default();
    let thresholds = topos_config::tce::broadcast::ReliableBroadcastParams {
        echo_threshold: 1,
        ready_threshold: 1,
        delivery_threshold: 1,
    };

    let message_signer = Arc::new(
        MessageSigner::from_str("122f3ae6ade1fd136b292cea4f6243c7811160352c8821528547a1fe7c459daf")
            .unwrap(),
    );

    let manager = TaskManager::new(
        message_receiver,
        SubscriptionsView::default(),
        event_sender,
        validator_id,
        thresholds,
        message_signer,
        validator_store,
        broadcast_sender,
    );

    spawn(manager.run(shutdown));

    let certificates = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 2);
    let parent = certificates
        .first()
        .take()
        .expect("Failed to create certificate");

    let child = certificates
        .last()
        .take()
        .expect("Failed to create certificate");

    let _ = message_sender
        .send(crate::DoubleEchoCommand::Broadcast {
            need_gossip: false,
            cert: child.certificate.clone(),
            pending_id: 0,
        })
        .await;

    let _ = message_sender
        .send(crate::DoubleEchoCommand::Broadcast {
            need_gossip: false,
            cert: parent.certificate.clone(),
            pending_id: 0,
        })
        .await;

    let _ = message_sender
        .send(crate::DoubleEchoCommand::Broadcast {
            need_gossip: false,
            cert: parent.certificate.clone(),
            pending_id: 0,
        })
        .await;

    assert_eq!(DOUBLE_ECHO_ACTIVE_TASKS_COUNT.get(), 1);
}
