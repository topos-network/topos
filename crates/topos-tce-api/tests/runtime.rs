use futures::Stream;
use rstest::rstest;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use test_log::test;
use tokio::sync::{broadcast, mpsc};
use tokio::{spawn, sync::oneshot};
use tokio_stream::StreamExt;
use tonic::transport::channel;
use tonic::transport::Uri;
use topos_core::api::graphql::certificate::Certificate as GraphQLCertificate;
use topos_core::api::grpc::shared::v1::checkpoints::TargetCheckpoint;
use topos_core::api::grpc::shared::v1::positions::TargetStreamPosition;
use topos_core::types::stream::Position;
use topos_core::types::CertificateDelivered;
use topos_core::uci::CertificateId;
use topos_core::{
    api::grpc::tce::v1::{
        api_service_client::ApiServiceClient,
        watch_certificates_request::OpenStream,
        watch_certificates_response::{CertificatePushed, Event},
    },
    uci::Certificate,
};
use topos_metrics::{STORAGE_PENDING_POOL_COUNT, STORAGE_PRECEDENCE_POOL_COUNT};
use topos_tce_api::{Runtime, RuntimeEvent};
use topos_tce_storage::types::CertificateDeliveredWithPositions;
use topos_tce_storage::validator::ValidatorStore;
use topos_tce_storage::StorageClient;
use topos_test_sdk::certificates::{
    create_certificate, create_certificate_at_position, create_certificate_chain,
};
use topos_test_sdk::constants::*;
use topos_test_sdk::networking::get_available_addr;
use topos_test_sdk::storage::{create_fullnode_store, create_validator_store, storage_client};
use topos_test_sdk::tce::public_api::{broadcast_stream, create_public_api, PublicApiContext};

mod grpc;

#[rstest]
#[timeout(Duration::from_secs(4))]
#[test(tokio::test)]
async fn runtime_can_dispatch_a_cert(
    #[future] create_public_api: (PublicApiContext, impl Stream<Item = RuntimeEvent>),
) {
    let (mut api_context, _) = create_public_api.await;
    let mut client = api_context.api_client;
    let (tx, rx) = oneshot::channel::<Certificate>();

    // This block represent a subnet A
    spawn(async move {
        let in_stream = async_stream::stream! {
            yield OpenStream {
                target_checkpoint: Some(TargetCheckpoint {
                    target_subnet_ids: vec![TARGET_SUBNET_ID_1.into()],
                    positions: Vec::new()
                }),
                source_checkpoint: None
            }.into()
        };

        let response = client.watch_certificates(in_stream).await.unwrap();

        let mut resp_stream = response.into_inner();

        let mut tx = Some(tx);
        while let Some(received) = resp_stream.next().await {
            let received = received.unwrap();
            if let Some(Event::CertificatePushed(CertificatePushed {
                certificate: Some(certificate),
                ..
            })) = received.event
            {
                if let Some(tx) = tx.take() {
                    _ = tx.send(certificate.try_into().unwrap());
                } else {
                    panic!("Double certificate sent");
                }
            }
        }
    });

    // Wait for client to be ready
    tokio::time::sleep(Duration::from_millis(10)).await;

    let cert = create_certificate_at_position(
        Position::ZERO,
        create_certificate(
            SOURCE_SUBNET_ID_1,
            &[TARGET_SUBNET_ID_1],
            Some(PREV_CERTIFICATE_ID),
        ),
    );

    let mut target_positions = std::collections::HashMap::new();
    target_positions.insert(
        TARGET_SUBNET_ID_1,
        topos_core::api::grpc::checkpoints::TargetStreamPosition {
            position: 0,
            source_subnet_id: SOURCE_SUBNET_ID_1,
            target_subnet_id: TARGET_SUBNET_ID_1,
            certificate_id: Some(cert.certificate.id),
        },
    );

    // Send a dispatch command that will be push to the subnet A
    api_context
        .client
        .dispatch_certificate(cert.clone(), target_positions)
        .await;

    let certificate_received = rx.await.unwrap();
    assert_eq!(cert.certificate, certificate_received);
    drop(api_context.api_context.take());
}

#[rstest]
#[timeout(Duration::from_secs(4))]
#[test(tokio::test)]
async fn can_catchup_with_old_certs(
    #[with(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 15)]
    #[from(create_certificate_chain)]
    certificates: Vec<CertificateDelivered>,
) {
    let storage_client = storage_client::partial_1(&certificates[..]);
    let (mut api_context, _) = create_public_api::partial_1(storage_client).await;

    let mut client = api_context.api_client;

    let (tx, mut rx) = mpsc::channel::<Certificate>(16);

    // This block represent a subnet A
    spawn(async move {
        let in_stream = async_stream::stream! {
            yield OpenStream {
                target_checkpoint: Some(TargetCheckpoint {
                    target_subnet_ids: vec![TARGET_SUBNET_ID_1.into()],
                    positions: Vec::new()
                }),
                source_checkpoint: None
            }.into()
        };

        let response = client.watch_certificates(in_stream).await.unwrap();

        let mut resp_stream = response.into_inner();

        while let Some(received) = resp_stream.next().await {
            let received = received.unwrap();
            if let Some(Event::CertificatePushed(CertificatePushed {
                certificate: Some(certificate),
                ..
            })) = received.event
            {
                _ = tx.send(certificate.try_into().unwrap()).await;
            }
        }
    });

    // Wait for client to be ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    let last = certificates.last().map(|c| c.certificate.id).unwrap();
    let cert = create_certificate_at_position(
        certificates.len().try_into().unwrap(),
        create_certificate(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], Some(last)),
    );

    let mut target_positions = std::collections::HashMap::new();
    target_positions.insert(
        TARGET_SUBNET_ID_1,
        topos_core::api::grpc::checkpoints::TargetStreamPosition {
            position: certificates.len() as u64,
            source_subnet_id: SOURCE_SUBNET_ID_1,
            target_subnet_id: TARGET_SUBNET_ID_1,
            certificate_id: Some(cert.certificate.id),
        },
    );

    // Send a dispatch command that will be push to the subnet A
    api_context
        .client
        .dispatch_certificate(cert.clone(), target_positions)
        .await;

    for (index, certificate) in certificates.iter().enumerate() {
        let certificate_received = rx
            .recv()
            .await
            .unwrap_or_else(|| panic!("Didn't received index {index}"));
        assert_eq!(
            certificate.certificate, certificate_received,
            "Certificate at index {index} not received"
        );
    }

    let certificate_received = rx.recv().await.unwrap();
    assert_eq!(cert.certificate, certificate_received);
    drop(api_context.api_context.take());
}

#[rstest]
#[timeout(Duration::from_secs(4))]
#[test(tokio::test)]
async fn can_catchup_with_old_certs_with_position(
    broadcast_stream: broadcast::Receiver<CertificateDeliveredWithPositions>,
) {
    let (tx, mut rx) = mpsc::channel::<Certificate>(16);

    let addr = get_available_addr();
    let graphql_addr = get_available_addr();
    let metrics_addr = get_available_addr();

    // launch data store
    let certificates = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 15);

    let fullnode_store = create_fullnode_store::default().await;
    let store = create_validator_store(
        &certificates[..],
        futures::future::ready(fullnode_store.clone()),
    )
    .await;

    let storage_client = StorageClient::new(store.clone());

    let (runtime_client, _launcher, _ctx) = Runtime::builder()
        .with_broadcast_stream(broadcast_stream)
        .storage(storage_client)
        .store(store)
        .serve_grpc_addr(addr)
        .serve_graphql_addr(graphql_addr)
        .serve_metrics_addr(metrics_addr)
        .build_and_launch()
        .await;

    // Wait for server to boot
    tokio::time::sleep(Duration::from_millis(100)).await;

    let uri = Uri::builder()
        .path_and_query("/")
        .authority(addr.to_string())
        .scheme("http")
        .build()
        .unwrap();

    // This block represent a subnet A
    spawn(async move {
        let channel = channel::Channel::builder(uri).connect_lazy();
        let mut client = ApiServiceClient::new(channel);
        let in_stream = async_stream::stream! {
            yield OpenStream {
                target_checkpoint: Some(TargetCheckpoint {
                    target_subnet_ids: vec![TARGET_SUBNET_ID_1.into()],
                    positions: vec![
                        TargetStreamPosition {
                            certificate_id: None,
                            position: 5,
                            source_subnet_id: Some(SOURCE_SUBNET_ID_1.into()),
                            target_subnet_id: Some(TARGET_SUBNET_ID_1.into())
                        }
                    ]
                }),
                source_checkpoint: None
            }.into()
        };

        let response = client.watch_certificates(in_stream).await.unwrap();

        let mut resp_stream = response.into_inner();

        while let Some(received) = resp_stream.next().await {
            let received = received.unwrap();
            if let Some(Event::CertificatePushed(CertificatePushed {
                certificate: Some(certificate),
                ..
            })) = received.event
            {
                _ = tx.send(certificate.try_into().unwrap()).await;
            }
        }
    });

    // Wait for client to be ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    let last = certificates.last().map(|c| c.certificate.id).unwrap();
    let cert = create_certificate_at_position(
        certificates.len().try_into().unwrap(),
        create_certificate(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], Some(last)),
    );

    let mut target_positions = std::collections::HashMap::new();
    target_positions.insert(
        TARGET_SUBNET_ID_1,
        topos_core::api::grpc::checkpoints::TargetStreamPosition {
            position: certificates.len() as u64,
            source_subnet_id: SOURCE_SUBNET_ID_1,
            target_subnet_id: TARGET_SUBNET_ID_1,
            certificate_id: Some(cert.certificate.id),
        },
    );

    // Send a dispatch command that will be push to the subnet A
    runtime_client
        .dispatch_certificate(cert.clone(), target_positions)
        .await;

    for (index, certificate) in certificates.iter().skip(5).enumerate() {
        let certificate_received = rx
            .recv()
            .await
            .unwrap_or_else(|| panic!("Didn't received index {index}"));
        assert_eq!(
            certificate.certificate, certificate_received,
            "Certificate at index {index} not received"
        );
    }

    let certificate_received = rx.recv().await.unwrap();
    assert_eq!(cert.certificate, certificate_received);
}

#[test(tokio::test)]
#[ignore = "not yet implemented"]
async fn can_listen_for_multiple_subnet_id() {}

#[rstest]
#[timeout(Duration::from_secs(4))]
#[test(tokio::test)]
async fn boots_healthy_graphql_server(
    broadcast_stream: broadcast::Receiver<CertificateDeliveredWithPositions>,
) {
    let addr = get_available_addr();
    let graphql_addr = get_available_addr();
    let metrics_addr = get_available_addr();

    // launch data store
    let certificates = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 15);

    let fullnode_store = create_fullnode_store::default().await;
    let store = create_validator_store(
        &certificates[..],
        futures::future::ready(fullnode_store.clone()),
    )
    .await;
    let storage_client = StorageClient::new(store.clone());
    let (_runtime_client, _launcher, _ctx) = Runtime::builder()
        .with_broadcast_stream(broadcast_stream)
        .storage(storage_client)
        .store(store)
        .serve_grpc_addr(addr)
        .serve_graphql_addr(graphql_addr)
        .serve_metrics_addr(metrics_addr)
        .build_and_launch()
        .await;

    // Wait for server to boot
    tokio::time::sleep(Duration::from_millis(100)).await;

    let res = reqwest::get(format!("http://{}/health", graphql_addr))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(res, "{\"healthy\":true}");
}

#[rstest]
#[timeout(Duration::from_secs(4))]
#[test(tokio::test)]
async fn graphql_server_enables_cors(
    broadcast_stream: broadcast::Receiver<CertificateDeliveredWithPositions>,
) {
    let addr = get_available_addr();
    let graphql_addr = get_available_addr();
    let metrics_addr = get_available_addr();

    // launch data store
    let certificates = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 15);

    let fullnode_store = create_fullnode_store::default().await;
    let store = create_validator_store(
        &certificates[..],
        futures::future::ready(fullnode_store.clone()),
    )
    .await;

    let storage_client = StorageClient::new(store.clone());

    let (_runtime_client, _launcher, _ctx) = Runtime::builder()
        .with_broadcast_stream(broadcast_stream)
        .storage(storage_client)
        .store(store)
        .serve_grpc_addr(addr)
        .serve_graphql_addr(graphql_addr)
        .serve_metrics_addr(metrics_addr)
        .build_and_launch()
        .await;

    // Wait for server to boot
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Origin", "http://example.com".parse().unwrap());
    headers.insert("Access-Control-Request-Method", "POST".parse().unwrap());
    headers.insert(
        "Access-Control-Request-Headers",
        "X-Requested-With".parse().unwrap(),
    );

    let client = reqwest::Client::new();

    let res = client
        .request(
            "OPTIONS".parse().unwrap(),
            format!("http://{}/health", graphql_addr),
        )
        .headers(headers)
        .send()
        .await
        .unwrap();

    let headers = res.headers();

    let ac_allow_origin = headers.get("Access-Control-Allow-Origin");
    assert_eq!(ac_allow_origin.unwrap().to_str().unwrap(), "*");

    let ac_allow_methods = headers.get("Access-Control-Allow-Methods");
    assert_eq!(ac_allow_methods.unwrap().to_str().unwrap(), "GET,POST");

    let ac_allow_headers = headers.get("Access-Control-Allow-Headers");
    assert_eq!(ac_allow_headers.unwrap().to_str().unwrap(), "content-type");
}

#[rstest]
#[timeout(Duration::from_secs(4))]
#[test(tokio::test)]
async fn can_query_graphql_endpoint_for_certificates(
    broadcast_stream: broadcast::Receiver<CertificateDeliveredWithPositions>,
) {
    let (tx, mut rx) = mpsc::channel::<Certificate>(16);

    let addr = get_available_addr();
    let graphql_addr = get_available_addr();
    let metrics_addr = get_available_addr();

    // launch data store
    let certificates = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 15);

    let fullnode_store = create_fullnode_store::default().await;

    let store = create_validator_store(
        &certificates[..],
        futures::future::ready(fullnode_store.clone()),
    )
    .await;

    let storage_client = StorageClient::new(store.clone());

    let (runtime_client, _launcher, _ctx) = Runtime::builder()
        .with_broadcast_stream(broadcast_stream)
        .storage(storage_client)
        .store(store)
        .serve_grpc_addr(addr)
        .serve_graphql_addr(graphql_addr)
        .serve_metrics_addr(metrics_addr)
        .build_and_launch()
        .await;

    // Wait for server to boot
    tokio::time::sleep(Duration::from_millis(100)).await;

    let uri = Uri::builder()
        .path_and_query("/")
        .authority(addr.to_string())
        .scheme("http")
        .build()
        .unwrap();

    // This block represent a subnet A
    spawn(async move {
        let channel = channel::Channel::builder(uri).connect_lazy();
        let mut client = ApiServiceClient::new(channel);
        let in_stream = async_stream::stream! {
            yield OpenStream {
                target_checkpoint: Some(TargetCheckpoint {
                    target_subnet_ids: vec![TARGET_SUBNET_ID_1.into()],
                    positions: vec![
                        TargetStreamPosition {
                            certificate_id: None,
                            position: 5,
                            source_subnet_id: Some(SOURCE_SUBNET_ID_1.into()),
                            target_subnet_id: Some(TARGET_SUBNET_ID_1.into())
                        }
                    ]
                }),
                source_checkpoint: None
            }.into()
        };

        let response = client.watch_certificates(in_stream).await.unwrap();

        let mut resp_stream = response.into_inner();

        while let Some(received) = resp_stream.next().await {
            let received = received.unwrap();
            if let Some(Event::CertificatePushed(CertificatePushed {
                certificate: Some(certificate),
                ..
            })) = received.event
            {
                _ = tx.send(certificate.try_into().unwrap()).await;
            }
        }
    });

    // Wait for client to be ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    let last = certificates.last().map(|c| c.certificate.id).unwrap();
    let cert = create_certificate_at_position(
        certificates.len().try_into().unwrap(),
        create_certificate(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], Some(last)),
    );

    let mut target_positions = std::collections::HashMap::new();
    target_positions.insert(
        TARGET_SUBNET_ID_1,
        topos_core::api::grpc::checkpoints::TargetStreamPosition {
            position: certificates.len() as u64,
            source_subnet_id: SOURCE_SUBNET_ID_1,
            target_subnet_id: TARGET_SUBNET_ID_1,
            certificate_id: Some(cert.certificate.id),
        },
    );

    // Send a dispatch command that will be push to the subnet A
    runtime_client
        .dispatch_certificate(cert.clone(), target_positions)
        .await;

    for (index, certificate) in certificates.iter().skip(5).enumerate() {
        let certificate_received = rx
            .recv()
            .await
            .unwrap_or_else(|| panic!("Didn't received index {index}"));
        assert_eq!(
            certificate.certificate, certificate_received,
            "Certificate at index {index} not received"
        );
    }

    let _ = rx.recv().await.unwrap();

    let query = format!(
        r#"
        query {{
            certificates(
                fromSourceCheckpoint: {{
                    sourceSubnetIds: [
                        "{SOURCE_SUBNET_ID_1}"
                    ],
                    positions: [
                        {{
                            sourceSubnetId:"{SOURCE_SUBNET_ID_1}",
                            position: 0,
                        }}
                    ]
                }},
                first: 10
            ) {{
                id
                prevId
                proof
                signature
                sourceSubnetId
                stateRoot
                targetSubnets
                txRootHash
                receiptsRootHash
                verifier
                positions {{
                  source {{
                    sourceSubnetId
                    position
                    certificateId
                  }}
                }}
            }}
        }}
        "#
    );

    #[derive(Deserialize)]
    struct Response {
        data: CertificatesResponse,
    }

    #[derive(Deserialize, Debug)]
    struct CertificatesResponse {
        certificates: Vec<GraphQLCertificate>,
    }

    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{}", graphql_addr))
        .json(&serde_json::json!({
            "query": query,
        }))
        .send()
        .await
        .unwrap()
        .json::<Response>()
        .await
        .unwrap();

    let graphql_certificate: GraphQLCertificate = cert.as_ref().into();

    assert_eq!(response.data.certificates.len(), 10);
    assert_eq!(
        response.data.certificates[0].source_subnet_id,
        graphql_certificate.source_subnet_id
    );
}

#[rstest]
#[timeout(Duration::from_secs(4))]
#[test(tokio::test)]
async fn check_storage_pool_stats(
    broadcast_stream: broadcast::Receiver<CertificateDeliveredWithPositions>,
) {
    let addr = get_available_addr();
    let graphql_addr = get_available_addr();
    let metrics_addr = get_available_addr();

    let fullnode_store = create_fullnode_store::default().await;

    let store = create_validator_store(&[], futures::future::ready(fullnode_store.clone())).await;
    STORAGE_PENDING_POOL_COUNT.set(10);
    STORAGE_PRECEDENCE_POOL_COUNT.set(200);

    let storage_client = StorageClient::new(store.clone());

    let (_runtime_client, _launcher, _ctx) = Runtime::builder()
        .with_broadcast_stream(broadcast_stream)
        .storage(storage_client)
        .store(store)
        .serve_grpc_addr(addr)
        .serve_graphql_addr(graphql_addr)
        .serve_metrics_addr(metrics_addr)
        .build_and_launch()
        .await;

    // Wait for server to boot
    tokio::time::sleep(Duration::from_millis(100)).await;

    let query = "query {getStoragePoolStats}";

    #[derive(Debug, Deserialize)]
    struct Response {
        // data: HashMap<String, serde_json::Value>,
        data: Stats,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Stats {
        get_storage_pool_stats: PoolStats,
    }

    #[derive(Debug, Deserialize)]
    struct PoolStats {
        pending_pool: u64,
        precedence_pool: u64,
    }

    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{}", graphql_addr))
        .json(&serde_json::json!({
            "query": query,
        }))
        .send()
        .await
        .unwrap()
        .json::<Response>()
        .await
        .unwrap();

    assert_eq!(response.data.get_storage_pool_stats.pending_pool, 10);
    assert_eq!(response.data.get_storage_pool_stats.precedence_pool, 200);
}

#[rstest]
#[timeout(Duration::from_secs(4))]
#[test(tokio::test)]
async fn get_pending_pool(
    broadcast_stream: broadcast::Receiver<CertificateDeliveredWithPositions>,
) {
    let addr = get_available_addr();
    let graphql_addr = get_available_addr();
    let metrics_addr = get_available_addr();

    // launch data store
    let certificates = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 15);

    let fullnode_store = create_fullnode_store::default().await;

    let store: Arc<ValidatorStore> =
        create_validator_store(vec![], futures::future::ready(fullnode_store.clone())).await;

    for certificate in &certificates {
        _ = store.insert_pending_certificate(&certificate.certificate);
    }

    let storage_client = StorageClient::new(store.clone());

    let (_runtime_client, _launcher, _ctx) = Runtime::builder()
        .with_broadcast_stream(broadcast_stream)
        .storage(storage_client)
        .store(store)
        .serve_grpc_addr(addr)
        .serve_graphql_addr(graphql_addr)
        .serve_metrics_addr(metrics_addr)
        .build_and_launch()
        .await;

    // Wait for server to boot
    tokio::time::sleep(Duration::from_millis(100)).await;

    let query = "query { getPendingPool }".to_string();

    #[derive(Debug, Deserialize)]
    struct Response {
        data: PendingPool,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PendingPool {
        #[serde(rename = "getPendingPool")]
        pool: HashMap<u64, String>,
    }

    let client = reqwest::Client::new();

    let mut response = client
        .post(format!("http://{}", graphql_addr))
        .json(&serde_json::json!({
            "query": query,
        }))
        .send()
        .await
        .unwrap()
        .json::<Response>()
        .await
        .unwrap();

    assert_eq!(response.data.pool.len(), 1);
    let first: CertificateId = response
        .data
        .pool
        .remove(&1)
        .unwrap()
        .as_bytes()
        .try_into()
        .unwrap();

    assert_eq!(first, certificates[0].certificate.id);
}

#[rstest]
#[timeout(Duration::from_secs(4))]
#[test(tokio::test)]
async fn check_precedence(
    broadcast_stream: broadcast::Receiver<CertificateDeliveredWithPositions>,
) {
    let addr = get_available_addr();
    let graphql_addr = get_available_addr();
    let metrics_addr = get_available_addr();

    // launch data store
    let certificates = create_certificate_chain(SOURCE_SUBNET_ID_1, &[TARGET_SUBNET_ID_1], 15);

    let fullnode_store = create_fullnode_store::default().await;

    let store: Arc<ValidatorStore> =
        create_validator_store(vec![], futures::future::ready(fullnode_store.clone())).await;

    for certificate in &certificates {
        _ = store.insert_pending_certificate(&certificate.certificate);
    }

    let storage_client = StorageClient::new(store.clone());

    let (_runtime_client, _launcher, _ctx) = Runtime::builder()
        .with_broadcast_stream(broadcast_stream)
        .storage(storage_client)
        .store(store)
        .serve_grpc_addr(addr)
        .serve_graphql_addr(graphql_addr)
        .serve_metrics_addr(metrics_addr)
        .build_and_launch()
        .await;

    // Wait for server to boot
    tokio::time::sleep(Duration::from_millis(100)).await;

    let certificate_one = certificates[0].certificate.id;

    let query = format!(
        r#"
        query {{ checkPrecedence(certificateId: "{}") {{ id }} }}
        "#,
        certificate_one
    );

    #[derive(Debug, Deserialize)]
    struct Response {
        data: CheckPrecedenceResponse,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct CheckPrecedenceResponse {
        check_precedence: CheckPrecedence,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct CheckPrecedence {
        id: String,
    }

    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{}", graphql_addr))
        .json(&serde_json::json!({
            "query": query,
        }))
        .send()
        .await
        .unwrap()
        .json::<Response>()
        .await
        .unwrap();

    assert_eq!(
        TryInto::<CertificateId>::try_into(response.data.check_precedence.id.as_bytes()).unwrap(),
        certificates[1].certificate.id
    );
}
