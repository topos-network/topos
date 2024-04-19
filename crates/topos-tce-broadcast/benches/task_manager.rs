use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot};
use topos_config::tce::broadcast::ReliableBroadcastParams;
use topos_core::types::ValidatorId;
use topos_crypto::messages::MessageSigner;
use topos_metrics::{
    DOUBLE_CLIENT_TO_DOUBLE_ECHO_CHANNEL, DOUBLE_ECHO_EVENT_CHANNEL,
    DOUBLE_ECHO_TO_TASK_MANAGER_CHANNEL,
};
use topos_tce_broadcast::double_echo::DoubleEcho;
use topos_tce_storage::validator::ValidatorStore;
use topos_test_sdk::certificates::create_certificate_chain;
use topos_test_sdk::constants::{SOURCE_SUBNET_ID_1, TARGET_SUBNET_ID_1};

const CHANNEL_SIZE: usize = 256_000;
const PRIVATE_KEY: &str = "d6f8d1fe6d0f3606ccb15ef383910f10d83ca77bf3d73007f12fef023dabaab9";

struct TceParams {
    nb_peers: usize,
    broadcast_params: ReliableBroadcastParams,
}

pub async fn processing_double_echo(n: u64, validator_store: Arc<ValidatorStore>) {
    let (_cmd_sender, cmd_receiver) =
        topos_metrics::channels::mpsc::channel(CHANNEL_SIZE, &DOUBLE_CLIENT_TO_DOUBLE_ECHO_CHANNEL);
    let (event_sender, _event_receiver) =
        topos_metrics::channels::mpsc::channel(CHANNEL_SIZE, &DOUBLE_ECHO_EVENT_CHANNEL);
    let (broadcast_sender, mut broadcast_receiver) = broadcast::channel(CHANNEL_SIZE);
    let (_double_echo_shutdown_sender, double_echo_shutdown_receiver) =
        mpsc::channel::<oneshot::Sender<()>>(1);
    let (task_manager_message_sender, task_manager_message_receiver) =
        topos_metrics::channels::mpsc::channel(CHANNEL_SIZE, &DOUBLE_ECHO_TO_TASK_MANAGER_CHANNEL);

    let params = TceParams {
        nb_peers: 10,
        broadcast_params: ReliableBroadcastParams {
            echo_threshold: 8,
            ready_threshold: 5,
            delivery_threshold: 8,
        },
    };

    let message_signer: Arc<MessageSigner> =
        Arc::new(MessageSigner::from_str(PRIVATE_KEY).unwrap());
    let mut validators = HashSet::new();
    let validator_id = ValidatorId::from(message_signer.public_address);
    validators.insert(validator_id);

    for i in 1..params.nb_peers {
        validators.insert(ValidatorId::from(
            MessageSigner::new(&[i as u8; 32]).unwrap().public_address,
        ));
    }

    let mut double_echo = DoubleEcho::new(
        params.broadcast_params,
        validator_id,
        message_signer.clone(),
        validators.clone(),
        task_manager_message_sender.clone(),
        cmd_receiver,
        event_sender,
        double_echo_shutdown_receiver,
        validator_store.clone(),
        broadcast_sender,
    );

    double_echo.spawn_task_manager(task_manager_message_receiver);

    let certificates =
        create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], n as usize);

    let double_echo_selected_echo = double_echo
        .subscriptions
        .echo
        .iter()
        .take(double_echo.params.echo_threshold)
        .cloned()
        .collect::<Vec<_>>();

    let double_echo_selected_ready = double_echo
        .subscriptions
        .ready
        .iter()
        .take(double_echo.params.delivery_threshold)
        .cloned()
        .collect::<Vec<_>>();

    for cert in &certificates {
        _ = validator_store
            .insert_pending_certificate(&cert.certificate)
            .await
            .unwrap();
    }

    for cert in &certificates {
        let mut payload = Vec::new();
        payload.extend_from_slice(cert.certificate.id.as_array());
        payload.extend_from_slice(validator_id.as_bytes());

        for _ in &double_echo_selected_echo {
            let signature = message_signer.sign_message(&payload).unwrap();

            double_echo
                .handle_echo(cert.certificate.id, validator_id, signature)
                .await;
        }

        for _ in &double_echo_selected_ready {
            let signature = message_signer.sign_message(&payload).unwrap();

            double_echo
                .handle_ready(cert.certificate.id, validator_id, signature)
                .await;
        }
    }

    let mut count = 0;

    while let Ok(_event) = broadcast_receiver.recv().await {
        count += 1;

        if count == n {
            break;
        }
    }
}
