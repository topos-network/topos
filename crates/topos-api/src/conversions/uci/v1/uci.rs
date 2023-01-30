///
/// Protobuf generated/native Rust structures related conversions for GRPC API
///
use crate::uci::v1 as proto_v1;

impl From<proto_v1::Certificate> for topos_uci::Certificate {
    fn from(certificate: proto_v1::Certificate) -> Self {
        topos_uci::Certificate {
            prev_id: certificate
                .prev_id
                .expect("valid previous certificate id")
                .value
                .try_into()
                .expect("valid previous certificate id with correct length"),
            source_subnet_id: certificate
                .source_subnet_id
                .expect("valid source subnet id")
                .value
                .try_into()
                .expect("valid source subnet id with correct length"),
            state_root: certificate
                .state_root
                .try_into()
                .expect("valid state root with correct length"),
            tx_root_hash: certificate
                .tx_root_hash
                .try_into()
                .expect("valid transaction root hash with correct length"),
            target_subnets: certificate
                .target_subnets
                .into_iter()
                .map(|target_subnet| target_subnet.into())
                .collect(),
            verifier: certificate.verifier,
            id: certificate
                .id
                .expect("valid certificate id")
                .value
                .try_into()
                .expect("valid certificate id with correct length"),
            proof: certificate.proof.expect("valid proof").value,
            signature: certificate.signature.expect("valid frost signature").value,
        }
    }
}

impl From<topos_uci::Certificate> for proto_v1::Certificate {
    fn from(certificate: topos_uci::Certificate) -> Self {
        proto_v1::Certificate {
            prev_id: Some(crate::shared::v1::CertificateId {
                value: certificate.prev_id.into(),
            }),
            source_subnet_id: Some(crate::shared::v1::SubnetId {
                value: certificate.source_subnet_id.to_vec(),
            }),
            state_root: certificate.state_root.to_vec(),
            tx_root_hash: certificate.tx_root_hash.to_vec(),
            verifier: certificate.verifier,
            target_subnets: certificate
                .target_subnets
                .into_iter()
                .map(|target_subnet| target_subnet.into())
                .collect(),
            id: Some(crate::shared::v1::CertificateId {
                value: certificate.id.into(),
            }),
            proof: Some(crate::shared::v1::StarkProof {
                value: certificate.proof,
            }),
            signature: Some(crate::shared::v1::Frost {
                value: certificate.signature,
            }),
        }
    }
}
