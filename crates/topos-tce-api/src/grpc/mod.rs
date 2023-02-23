use futures::{FutureExt, Stream as FutureStream, StreamExt};
use opentelemetry::{
    trace::{FutureExt as TraceFutureExt, TraceContextExt},
    Context,
};
use std::pin::Pin;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use topos_core::api::tce::v1::{
    api_service_server::ApiService, GetSourceHeadRequest, GetSourceHeadResponse,
    SubmitCertificateRequest, SubmitCertificateResponse, WatchCertificatesRequest,
    WatchCertificatesResponse,
};
use tracing::{error, field, info, instrument, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::{
    runtime::InternalRuntimeCommand,
    stream::{Stream, StreamError, StreamErrorKind},
};

use self::messaging::{InboundMessage, OutboundMessage};

pub(crate) mod console;
#[cfg(test)]
mod tests;

const DEFAULT_CHANNEL_STREAM_CAPACITY: usize = 100;

pub(crate) mod builder;
pub(crate) mod messaging;

#[derive(Debug)]
pub(crate) struct TceGrpcService {
    local_peer_id: String,
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
    #[instrument(name = "CertificateSubmitted", skip(self, request), fields(peer_id = self.local_peer_id, certificate_id = field::Empty))]
    async fn submit_certificate(
        &self,
        request: Request<SubmitCertificateRequest>,
    ) -> Result<Response<SubmitCertificateResponse>, Status> {
        tracing::warn!(span_span_id = ?Span::current().context().span().span_context().span_id(), "pre_run");
        tracing::warn!(cx_span_id = ?Context::current().span().span_context().span_id(), "pre_run");

        let data = request.into_inner();
        if let Some(certificate) = data.certificate {
            if let Some(ref id) = certificate.id {
                Span::current().record("certificate_id", id.to_string());

                let (sender, receiver) = oneshot::channel();
                let certificate = match certificate.try_into() {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Invalid certificate: {e:?}");
                        return Err(Status::invalid_argument(
                            "Can't submit certificate: invalid certificate",
                        ));
                    }
                };

                if self
                    .command_sender
                    .send(InternalRuntimeCommand::CertificateSubmitted {
                        certificate: Box::new(certificate),
                        sender,
                        ctx: Span::current().context(),
                    })
                    .with_current_context()
                    .instrument(Span::current())
                    .await
                    .is_err()
                {
                    return Err(Status::internal("Can't submit certificate: sender dropped"));
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

    async fn get_source_head(
        &self,
        request: Request<GetSourceHeadRequest>,
    ) -> Result<Response<GetSourceHeadResponse>, Status> {
        let data = request.into_inner();
        if let Some(subnet_id) = data.subnet_id {
            let (sender, receiver) =
                oneshot::channel::<Result<(u64, topos_core::uci::Certificate), _>>();

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
                .map(|value| match value {
                    Ok(Ok((position, ref certificate))) => {
                        Ok(Response::new(GetSourceHeadResponse {
                            certificate: Some(certificate.clone().into()),
                            position: Some(topos_core::api::tce::v1::SourceStreamPosition {
                                subnet_id: Some(certificate.source_subnet_id.into()),
                                certificate_id: Some((*certificate.id.as_array()).into()),
                                position,
                            }),
                        }))
                    }
                    Ok(Err(crate::RuntimeError::UnknownSubnet(subnet_id))) => {
                        // Tce does not have Position::Zero certificate associated
                        Err(Status::internal(format!(
                            "Unknown subnet, no genesis certificate associated with subnet id {:?}",
                            &subnet_id
                        )))
                    }
                    Ok(Err(_)) => Err(Status::internal(
                        "Can't get source head certificate position",
                    )),
                    Err(_) => Err(Status::internal(
                        "Can't get source head certificate position",
                    )),
                })
                .await
        } else {
            Err(Status::invalid_argument("Certificate is malformed"))
        }
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
            Some(addr) => info!(client.addr = %addr, "starting a new stream"),
            None => info!(client.addr = %"<unknown>", "starting a new stream"),
        }
        // TODO: Use Cow
        let stream_id = Uuid::new_v4();

        let inbound_stream = request
            .into_inner()
            .map(move |message| Self::parse_stream(message, stream_id))
            .boxed();

        let (command_sender, command_receiver) = mpsc::channel(2048);

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
