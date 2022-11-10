use rstest::rstest;
use topos_core::uci::Certificate;

use crate::{
    rocks::{
        map::Map, CertificatesColumn, PendingCertificatesColumn, SourceStreamRef,
        SourceSubnetStreamsColumn,
    },
    tests::support::SOURCE_SUBNET_ID,
    Height,
};

use super::support::columns::{certificates_column, pending_column, source_streams_column};

#[rstest]
#[tokio::test]
async fn can_persist_a_pending_certificate(pending_column: PendingCertificatesColumn) {
    let certificate = Certificate::new("".into(), "source_subnet_id".into(), Vec::new());

    assert!(pending_column.insert(&0, &certificate).is_ok());
    assert_eq!(pending_column.get(&0).unwrap(), certificate);
}

#[rstest]
#[tokio::test]
async fn can_persist_a_delivered_certificate(certificates_column: CertificatesColumn) {
    let certificate = Certificate::new("".into(), "source_subnet_id".into(), Vec::new());

    assert!(certificates_column
        .insert(&certificate.cert_id, &certificate)
        .is_ok());
    assert_eq!(
        certificates_column.get(&certificate.cert_id).unwrap(),
        certificate
    );
}

#[rstest]
#[tokio::test]
async fn delivered_certificate_height_are_incremented(
    certificates_column: CertificatesColumn,
    source_streams_column: SourceSubnetStreamsColumn,
) {
    let certificate = Certificate::new("".into(), "source_subnet_id".into(), Vec::new());

    assert!(certificates_column
        .insert(&certificate.cert_id, &certificate)
        .is_ok());
    assert!(source_streams_column
        .insert(
            &SourceStreamRef(SOURCE_SUBNET_ID, Height::ZERO),
            &certificate.cert_id
        )
        .is_ok());
}

#[rstest]
#[tokio::test]
async fn height_can_be_fetch_for_one_subnet(source_streams_column: SourceSubnetStreamsColumn) {
    let certificate = Certificate::new("".into(), "source_subnet_id".into(), Vec::new());

    assert!(source_streams_column
        .insert(
            &SourceStreamRef(SOURCE_SUBNET_ID, Height::ZERO),
            &certificate.cert_id
        )
        .is_ok());

    assert!(matches!(
        source_streams_column
            .prefix_iter(&SOURCE_SUBNET_ID)
            .unwrap()
            .last(),
        Some((SourceStreamRef(_, Height::ZERO), _))
    ));

    let certificate = Certificate::new("".into(), "source_subnet_id".into(), Vec::new());

    assert!(source_streams_column
        .insert(
            &SourceStreamRef(SOURCE_SUBNET_ID, Height(1)),
            &certificate.cert_id
        )
        .is_ok());

    assert!(matches!(
        source_streams_column
            .prefix_iter(&SOURCE_SUBNET_ID)
            .unwrap()
            .last(),
        Some((SourceStreamRef(_, Height(1)), _))
    ));
}

#[tokio::test]
#[ignore = "not yet implemented"]
async fn height_can_be_fetch_for_multiple_subnets() {}

#[tokio::test]
#[ignore = "not yet implemented"]
async fn height_can_be_fetch_for_all_subnets() {}
