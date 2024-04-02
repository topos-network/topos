use libp2p::PeerId;
use rstest::{fixture, rstest};
use std::{collections::HashSet, future::IntoFuture, sync::Arc};
use tokio_stream::Stream;
use topos_tce_api::RuntimeEvent;
use topos_tce_broadcast::event::ProtocolEvents;
use topos_tce_gatekeeper::Gatekeeper;

use tokio::sync::{broadcast, mpsc};
use topos_crypto::messages::MessageSigner;
use topos_p2p::{utils::GrpcOverP2P, NetworkClient};
use topos_tce_broadcast::{ReliableBroadcastClient, ReliableBroadcastConfig};
use topos_tce_storage::{validator::ValidatorStore, StorageClient};
use topos_test_sdk::{
    certificates::create_certificate_chain,
    constants::{CERTIFICATE_ID_1, SOURCE_SUBNET_ID_1, TARGET_SUBNET_ID_1},
    storage::create_validator_store,
    tce::public_api::{create_public_api, PublicApiContext},
};

use crate::AppContext;

mod api;
mod network;

#[rstest]
#[tokio::test]
async fn non_validator_publish_gossip(
    #[future] setup_test: (
        AppContext,
        mpsc::Receiver<topos_p2p::Command>,
        Arc<MessageSigner>,
    ),
) {
    let (mut context, mut p2p_receiver, _) = setup_test.await;
    let certificates = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 1);
    context
        .on_protocol_event(ProtocolEvents::Gossip {
            cert: certificates[0].certificate.clone(),
        })
        .await;

    assert!(matches!(
        p2p_receiver.try_recv(),
        Ok(topos_p2p::Command::Gossip { topic, .. }) if topic == "topos_gossip"
    ));
}

#[rstest]
#[tokio::test]
async fn non_validator_do_not_publish_echo(
    #[future] setup_test: (
        AppContext,
        mpsc::Receiver<topos_p2p::Command>,
        Arc<MessageSigner>,
    ),
) {
    let (mut context, mut p2p_receiver, message_signer) = setup_test.await;
    context
        .on_protocol_event(ProtocolEvents::Echo {
            certificate_id: CERTIFICATE_ID_1,
            signature: message_signer.sign_message(&[]).ok().unwrap(),
            validator_id: message_signer.public_address.into(),
        })
        .await;

    assert!(p2p_receiver.try_recv().is_err(),);
}

#[rstest]
#[tokio::test]
async fn non_validator_do_not_publish_ready(
    #[future] setup_test: (
        AppContext,
        mpsc::Receiver<topos_p2p::Command>,
        Arc<MessageSigner>,
    ),
) {
    let (mut context, mut p2p_receiver, message_signer) = setup_test.await;
    context
        .on_protocol_event(ProtocolEvents::Ready {
            certificate_id: CERTIFICATE_ID_1,
            signature: message_signer.sign_message(&[]).ok().unwrap(),
            validator_id: message_signer.public_address.into(),
        })
        .await;

    assert!(p2p_receiver.try_recv().is_err(),);
}

#[fixture]
pub async fn setup_test(
    #[future] create_validator_store: Arc<ValidatorStore>,
    #[future] create_public_api: (PublicApiContext, impl Stream<Item = RuntimeEvent>),
) -> (
    AppContext,
    mpsc::Receiver<topos_p2p::Command>,
    Arc<MessageSigner>,
) {
    let validator_store = create_validator_store.await;
    let is_validator = false;
    let message_signer = Arc::new(MessageSigner::new(&[5u8; 32]).unwrap());
    let validator_id = message_signer.public_address.into();

    let (broadcast_sender, _) = broadcast::channel(1);

    let (tce_cli, _) = ReliableBroadcastClient::new(
        ReliableBroadcastConfig {
            tce_params: topos_config::tce::broadcast::ReliableBroadcastParams::default(),
            validator_id,
            validators: HashSet::new(),
            message_signer: message_signer.clone(),
        },
        validator_store.clone(),
        broadcast_sender,
    )
    .await;

    let (shutdown_p2p, _) = mpsc::channel(1);
    let (p2p_sender, p2p_receiver) = mpsc::channel(1);
    let grpc_over_p2p = GrpcOverP2P::new(p2p_sender.clone());
    let network_client = NetworkClient {
        retry_ttl: 10,
        local_peer_id: PeerId::random(),
        sender: p2p_sender,
        grpc_over_p2p,
        shutdown_channel: shutdown_p2p,
    };

    let (api_context, _api_stream) = create_public_api.await;
    let api_client = api_context.client;

    let (gatekeeper_client, _) = Gatekeeper::builder().into_future().await.unwrap();

    let context = AppContext::new(
        is_validator,
        StorageClient::new(validator_store.clone()),
        tce_cli,
        network_client,
        api_client,
        gatekeeper_client,
        validator_store,
        api_context.api_context.unwrap(),
    );

    (context, p2p_receiver, message_signer)
}
