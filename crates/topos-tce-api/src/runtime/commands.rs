use tokio::sync::{mpsc::Sender, oneshot};
use tonic::{Status, Streaming};
use topos_core::{
    api::tce::v1::{WatchCertificatesRequest, WatchCertificatesResponse},
    uci::Certificate,
};
use uuid::Uuid;

pub enum RuntimeCommand {
    DispatchCertificate {
        subnet_id: String,
        certificate: Certificate,
    },
}

#[derive(Debug)]
pub(crate) enum InternalRuntimeCommand {
    NewStream {
        stream: Streaming<WatchCertificatesRequest>,
        sender: Sender<Result<WatchCertificatesResponse, Status>>,
        internal_runtime_command_sender: Sender<Self>,
    },

    Register {
        stream_id: Uuid,
        subnet_id: String,
        sender: oneshot::Sender<Result<(), ()>>,
    },

    StreamTimeout {
        stream_id: Uuid,
    },

    #[allow(dead_code)]
    Handshaked {
        stream_id: Uuid,
    },

    CertificateSubmitted {
        certificate: Certificate,
    },
}
