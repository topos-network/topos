use std::sync::Arc;

use libp2p::PeerId;
use prost::Message;
use rstest::rstest;
use test_log::test;
use tokio::sync::mpsc;
use topos_core::api::grpc::tce::v1::{double_echo_request, DoubleEchoRequest, Echo, Gossip, Ready};
use topos_crypto::{messages::MessageSigner, validator_id::ValidatorId};
use topos_tce_storage::store::WriteStore;
use topos_test_sdk::{
    certificates::create_certificate_chain,
    constants::{SOURCE_SUBNET_ID_1, TARGET_SUBNET_ID_1},
};

use crate::AppContext;

use super::setup_test;

#[rstest]
#[test(tokio::test)]
async fn handle_gossip(
    #[future] setup_test: (
        AppContext,
        mpsc::Receiver<topos_p2p::Command>,
        Arc<MessageSigner>,
    ),
) {
    let (mut context, _, _) = setup_test.await;
    let mut certificates = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 1);
    let certificate = certificates.pop().unwrap().certificate;

    let msg = DoubleEchoRequest {
        request: Some(double_echo_request::Request::Gossip(Gossip {
            certificate: Some(certificate.into()),
        })),
    };
    context
        .on_net_event(topos_p2p::Event::Gossip {
            from: PeerId::random(),
            data: msg.encode_to_vec(),
            id: "0".to_string(),
        })
        .await;
}

#[rstest]
#[test(tokio::test)]
async fn handle_echo(
    #[future] setup_test: (
        AppContext,
        mpsc::Receiver<topos_p2p::Command>,
        Arc<MessageSigner>,
    ),
) {
    let (mut context, _, message_signer) = setup_test.await;
    let mut certificates = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 1);
    let certificate = certificates.pop().unwrap().certificate;
    let validator_id: ValidatorId = message_signer.public_address.into();

    let msg = DoubleEchoRequest {
        request: Some(double_echo_request::Request::Echo(Echo {
            certificate_id: Some(certificate.id.into()),
            signature: Some(message_signer.sign_message(&[]).ok().unwrap().into()),
            validator_id: Some(validator_id.into()),
        })),
    };
    context
        .on_net_event(topos_p2p::Event::Gossip {
            from: PeerId::random(),
            data: msg.encode_to_vec(),
            id: "0".to_string(),
        })
        .await;
}

#[rstest]
#[test(tokio::test)]
async fn handle_ready(
    #[future] setup_test: (
        AppContext,
        mpsc::Receiver<topos_p2p::Command>,
        Arc<MessageSigner>,
    ),
) {
    let (mut context, _, message_signer) = setup_test.await;
    let mut certificates = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 1);
    let certificate = certificates.pop().unwrap().certificate;
    let validator_id: ValidatorId = message_signer.public_address.into();

    let msg = DoubleEchoRequest {
        request: Some(double_echo_request::Request::Ready(Ready {
            certificate_id: Some(certificate.id.into()),
            signature: Some(message_signer.sign_message(&[]).ok().unwrap().into()),
            validator_id: Some(validator_id.into()),
        })),
    };
    context
        .on_net_event(topos_p2p::Event::Gossip {
            from: PeerId::random(),
            data: msg.encode_to_vec(),
            id: "0".to_string(),
        })
        .await;
}

#[rstest]
#[test(tokio::test)]
async fn handle_already_delivered(
    #[future] setup_test: (
        AppContext,
        mpsc::Receiver<topos_p2p::Command>,
        Arc<MessageSigner>,
    ),
) {
    let (mut context, _, _) = setup_test.await;
    let mut certificates = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 1);
    let certificate_delivered = certificates.pop().unwrap();
    let certificate = certificate_delivered.certificate.clone();

    let msg = DoubleEchoRequest {
        request: Some(double_echo_request::Request::Gossip(Gossip {
            certificate: Some(certificate.into()),
        })),
    };
    _ = context
        .validator_store
        .insert_certificate_delivered(&certificate_delivered)
        .await
        .unwrap();

    context
        .on_net_event(topos_p2p::Event::Gossip {
            from: PeerId::random(),
            data: msg.encode_to_vec(),
            id: "0".to_string(),
        })
        .await;
}
