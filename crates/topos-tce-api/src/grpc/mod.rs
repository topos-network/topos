use base64ct::{Base64, Encoding};
use futures::{FutureExt, Stream as FutureStream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use topos_core::api::grpc::tce::v1::LastPendingCertificate;
use topos_core::api::grpc::tce::v1::{
    api_service_server::ApiService, GetLastPendingCertificatesRequest,
    GetLastPendingCertificatesResponse, GetSourceHeadRequest, GetSourceHeadResponse,
    SubmitCertificateRequest, SubmitCertificateResponse, WatchCertificatesRequest,
    WatchCertificatesResponse,
};
use topos_core::uci::SubnetId;
use topos_metrics::API_GRPC_CERTIFICATE_RECEIVED_TOTAL;
use topos_tce_storage::validator::ValidatorStore;
use tracing::{error, info, Span};
use uuid::Uuid;

use crate::{
    runtime::InternalRuntimeCommand,
    stream::{Stream, StreamError, StreamErrorKind},
};

use self::messaging::{InboundMessage, OutboundMessage};

pub(crate) mod console;
#[cfg(test)]
mod tests;

const DEFAULT_CHANNEL_STREAM_CAPACITY: usize = 2024;

pub(crate) mod builder;
pub(crate) mod messaging;

pub(crate) struct TceGrpcService {
    store: Arc<ValidatorStore>,
    command_sender: mpsc::Sender<InternalRuntimeCommand>,
}

impl TceGrpcService {
    pub fn create_stream(
        rx: mpsc::Receiver<Result<(Option<Uuid>, OutboundMessage), Status>>,
    ) -> Pin<Box<dyn FutureStream<Item = Result<WatchCertificatesResponse, Status>> + Send + 'static>>
    {
        Box::pin(ReceiverStream::new(rx).map(|response| match response {
            Ok((request_id, response)) => Ok(WatchCertificatesResponse {
                event: Some(response.into()),
                request_id: request_id.map(Into::into),
            }),
            Err(error) => Err(error),
        }))
    }

    pub fn parse_stream(
        message: Result<WatchCertificatesRequest, Status>,
        stream_id: Uuid,
    ) -> Result<(Option<Uuid>, InboundMessage), StreamError> {
        match message {
            Ok(WatchCertificatesRequest {
                request_id,
                command,
            }) => match command {
                Some(command) => match command.try_into() {
                    Ok(inner_command) => Ok((request_id.map(Into::into), inner_command)),
                    Err(_) => Err(StreamError::new(stream_id, StreamErrorKind::InvalidCommand)),
                },
                None => Err(StreamError::new(stream_id, StreamErrorKind::InvalidCommand)),
            },
            Err(error) => Err(StreamError::new(
                stream_id,
                StreamErrorKind::Transport(error.code()),
            )),
        }
    }
}

#[tonic::async_trait]
impl ApiService for TceGrpcService {
    async fn submit_certificate(
        &self,
        request: Request<SubmitCertificateRequest>,
    ) -> Result<Response<SubmitCertificateResponse>, Status> {
        async {
            let data = request.into_inner();
            if let Some(certificate) = data.certificate {
                if let Some(ref id) = certificate.id {
                    Span::current().record("certificate_id", id.to_string());

                    let (sender, receiver) = oneshot::channel();
                    // FIXME: remove certificate cloning (may be a lot of data) when we
                    // resolve the issue with invalid certificate error
                    let certificate = match certificate.clone().try_into() {
                        Ok(c) => c,
                        Err(e) => {
                            error!(
                                "Invalid certificate error: {e:?}, certificate: {certificate:?}"
                            );
                            return Err(Status::invalid_argument(format!(
                                "Can't submit invalid certificate: {e}"
                            )));
                        }
                    };

                    if self
                        .command_sender
                        .send(InternalRuntimeCommand::CertificateSubmitted {
                            certificate: Box::new(certificate),
                            sender,
                        })
                        .await
                        .is_err()
                    {
                        return Err(Status::internal("Can't submit certificate: sender dropped"));
                    } else {
                        API_GRPC_CERTIFICATE_RECEIVED_TOTAL.inc();
                    }

                    receiver
                        .map(|value| match value {
                            Ok(Ok(_)) => Ok(Response::new(SubmitCertificateResponse {})),
                            Ok(Err(_)) => Err(Status::internal("Can't submit certificate")),
                            Err(_) => Err(Status::internal("Can't submit certificate")),
                        })
                        .await
                } else {
                    error!("No certificate id provided");
                    Err(Status::invalid_argument("Certificate is malformed"))
                }
            } else {
                Err(Status::invalid_argument("Certificate is malformed"))
            }
        }
        .await
    }

    /// This RPC allows a client to get last delivered source certificate
    /// for particular subnet
    async fn get_source_head(
        &self,
        request: Request<GetSourceHeadRequest>,
    ) -> Result<Response<GetSourceHeadResponse>, Status> {
        let data = request.into_inner();
        if let Some(subnet_id) = data.subnet_id {
            let (sender, receiver) = oneshot::channel();

            let subnet_id = match subnet_id.try_into() {
                Ok(id) => id,
                Err(e) => {
                    error!("Invalid subnet id: {e:?}");
                    return Err(Status::invalid_argument("Invalid subnet id"));
                }
            };

            if self
                .command_sender
                .send(InternalRuntimeCommand::GetSourceHead { subnet_id, sender })
                .await
                .is_err()
            {
                return Err(Status::internal(
                    "Can't get delivered certificate position by source: sender dropped",
                ));
            }

            receiver
                .map(|value| {
                    match value {

                    Ok(Ok(response)) => Ok(match response {
                        Some((position, certificate)) => Response::new(GetSourceHeadResponse {
                            certificate: Some(certificate.clone().into()),
                            position: Some(
                                topos_core::api::grpc::shared::v1::positions::SourceStreamPosition {
                                    source_subnet_id: Some(certificate.source_subnet_id.into()),
                                    certificate_id: Some((*certificate.id.as_array()).into()),
                                    position,

                                },
                            ),
                        }),
                        None => Response::new(GetSourceHeadResponse {
                            certificate: None,
                            position: None
                        })
                    }),

                    Ok(Err(crate::RuntimeError::UnknownSubnet(subnet_id))) =>
                        // Tce does not have Position::Zero certificate associated
                        {
                            Err(Status::internal(format!(
                                "Unknown subnet, no genesis certificate associated with subnet id \
                                 {}",
                                &subnet_id
                            )))
                        },

                        Ok(Err(e)) => Err(Status::internal(format!(
                            "Can't get source head certificate position: {e}"
                        ))),

                        Err(e) => Err(Status::internal(format!(
                            "Can't get source head certificate position: {e}"
                        ))),
                    }
                })
                .await
        } else {
            Err(Status::invalid_argument("Certificate is malformed"))
        }
    }

    async fn get_last_pending_certificates(
        &self,
        request: Request<GetLastPendingCertificatesRequest>,
    ) -> Result<Response<GetLastPendingCertificatesResponse>, Status> {
        let data = request.into_inner();

        let subnet_ids = data.subnet_ids;

        let subnet_ids: Vec<SubnetId> = subnet_ids
            .into_iter()
            .map(TryInto::try_into)
            .map(|v| v.map_err(|e| Status::internal(format!("Invalid subnet id: {e}"))))
            .collect::<Result<_, _>>()?;

        let last_pending_certificate = self
            .store
            .get_pending_certificates_for_subnets(&subnet_ids)
            .map_err(|e| Status::internal(format!("Can't get last pending certificates: {e}")))?
            .into_iter()
            .map(|(subnet_id, (index, maybe_certificate))| {
                (Base64::encode_string(subnet_id.as_array()), {
                    maybe_certificate
                        .map(|certificate| LastPendingCertificate {
                            index,
                            value: Some(certificate.into()),
                        })
                        .unwrap_or(LastPendingCertificate {
                            value: None,
                            index: 0,
                        })
                })
            })
            .collect();

        Ok(Response::new(GetLastPendingCertificatesResponse {
            last_pending_certificate,
        }))
    }

    ///Server streaming response type for the WatchCertificates method.
    type WatchCertificatesStream = Pin<
        Box<dyn FutureStream<Item = Result<WatchCertificatesResponse, Status>> + Send + 'static>,
    >;

    /// This RPC allows a client to open a bidirectional stream with a TCE
    async fn watch_certificates(
        &self,
        request: Request<Streaming<WatchCertificatesRequest>>,
    ) -> Result<Response<Self::WatchCertificatesStream>, Status> {
        match request.remote_addr() {
            Some(addr) => info!(client.addr = %addr, "Starting a new stream"),
            None => info!(client.addr = %"<unknown>", "Starting a new stream"),
        }
        // TODO: Use Cow
        let stream_id = Uuid::new_v4();

        let inbound_stream = request
            .into_inner()
            .map(move |message| Self::parse_stream(message, stream_id))
            .boxed();

        let (command_sender, command_receiver) = mpsc::channel(204_800);

        let (outbound_stream, rx) = mpsc::channel::<Result<(Option<Uuid>, OutboundMessage), Status>>(
            DEFAULT_CHANNEL_STREAM_CAPACITY,
        );

        let stream = Stream::new(
            stream_id,
            inbound_stream,
            outbound_stream,
            command_receiver,
            self.command_sender.clone(),
        );

        if self
            .command_sender
            .send(InternalRuntimeCommand::NewStream {
                stream,
                command_sender,
            })
            .await
            .is_err()
        {
            return Err(Status::internal("Can't submit certificate: sender dropped"));
        }

        Ok(Response::new(
            Self::create_stream(rx) as Self::WatchCertificatesStream
        ))
    }
}
