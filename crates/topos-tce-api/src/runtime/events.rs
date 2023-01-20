use tokio::sync::oneshot;
use topos_core::uci::{Certificate, SubnetId};
use topos_p2p::PeerId;

use super::error::RuntimeError;

pub enum RuntimeEvent {
    CertificateSubmitted {
        certificate: Certificate,
        sender: oneshot::Sender<Result<(), RuntimeError>>,
        ctx: tracing::Span,
    },

    PeerListPushed {
        peers: Vec<PeerId>,
        sender: oneshot::Sender<Result<(), RuntimeError>>,
    },

    GetSourceHead {
        subnet_id: SubnetId,
        sender: oneshot::Sender<Result<(u64, topos_core::uci::Certificate), RuntimeError>>,
    },
}
