use crate::{Error, TceProxyEvent};
use base64ct::{Base64, Encoding};
use futures::stream::FuturesOrdered;
use opentelemetry::trace::FutureExt;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;

use tonic::IntoRequest;
use topos_core::api::grpc::checkpoints::{TargetCheckpoint, TargetStreamPosition};
use topos_core::api::grpc::tce::v1::{
    GetLastPendingCertificatesRequest, GetSourceHeadRequest, GetSourceHeadResponse,
};
use topos_core::{
    api::grpc::tce::v1::{
        watch_certificates_request, watch_certificates_response, SubmitCertificateRequest,
        WatchCertificatesRequest, WatchCertificatesResponse,
    },
    uci::{Certificate, SubnetId},
};
use tracing::{debug, error, info, info_span, warn, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

const CERTIFICATE_OUTBOUND_CHANNEL_SIZE: usize = 2048;
const CERTIFICATE_INBOUND_CHANNEL_SIZE: usize = 2048;
const TCE_PROXY_COMMAND_CHANNEL_SIZE: usize = 2048;

// Maximum backoff retry timeout in seconds (1 hour)
const TCE_SUBMIT_CERTIFICATE_BACKOFF_TIMEOUT: Duration = Duration::from_secs(3600);

pub(crate) enum TceClientCommand {
    // Get head certificate that was sent to the TCE node for this subnet
    GetSourceHead {
        subnet_id: SubnetId,
        sender: oneshot::Sender<Result<(Certificate, u64), Error>>,
    },
    // Get map of subnet id->last pending certificate
    GetLastPendingCertificates {
        subnet_ids: Vec<SubnetId>,
        #[allow(clippy::type_complexity)]
        sender: oneshot::Sender<Result<HashMap<SubnetId, Option<(Certificate, u64)>>, Error>>,
    },
    // Open the stream to the TCE node
    // Mark the position from which TCE node certificates should be retrieved
    OpenStream {
        target_checkpoint: TargetCheckpoint,
    },
    // Send generated certificate to the TCE node
    SendCertificate {
        cert: Box<Certificate>,
        span: tracing::Span,
    },
    Shutdown,
}

/// Create new backoff library error based on error that happened
pub(crate) fn new_tce_proxy_backoff_err<E: std::fmt::Display>(err: E) -> backoff::Error<E> {
    // Retry according to backoff policy
    backoff::Error::Transient {
        err,
        retry_after: None,
    }
}

pub struct TceClient {
    subnet_id: topos_core::uci::SubnetId,
    tce_endpoint: String,
    command_sender: mpsc::Sender<TceClientCommand>,
}

impl TceClient {
    pub async fn open_stream(&self, positions: Vec<TargetStreamPosition>) -> Result<(), Error> {
        self.command_sender
            .send(TceClientCommand::OpenStream {
                target_checkpoint: TargetCheckpoint {
                    target_subnet_ids: vec![self.subnet_id],
                    positions,
                },
            })
            .await
            .map_err(|_| Error::InvalidChannelError)?;
        Ok(())
    }
    pub async fn send_certificate(&mut self, cert: Certificate) -> Result<(), Error> {
        self.command_sender
            .send(TceClientCommand::SendCertificate {
                cert: Box::new(cert),
                span: tracing::Span::current(),
            })
            .with_current_context()
            .in_current_span()
            .await
            .map_err(|_| Error::InvalidChannelError)?;
        Ok(())
    }
    pub async fn close(&mut self) -> Result<(), Error> {
        self.command_sender
            .send(TceClientCommand::Shutdown)
            .await
            .map_err(|_| Error::InvalidChannelError)?;
        Ok(())
    }

    // Return source head and position of the certificate
    pub async fn get_source_head(&mut self) -> Result<(Certificate, u64), Error> {
        #[allow(clippy::type_complexity)]
        let (sender, receiver): (
            oneshot::Sender<Result<(Certificate, u64), Error>>,
            oneshot::Receiver<Result<(Certificate, u64), Error>>,
        ) = oneshot::channel();
        self.command_sender
            .send(TceClientCommand::GetSourceHead {
                subnet_id: self.subnet_id,
                sender,
            })
            .await
            .map_err(|_| Error::InvalidChannelError)?;

        receiver.await.map_err(|_| Error::InvalidChannelError)?
    }

    pub async fn get_last_pending_certificates(
        &mut self,
        subnet_ids: Vec<SubnetId>,
    ) -> Result<HashMap<SubnetId, Option<(Certificate, u64)>>, Error> {
        #[allow(clippy::type_complexity)]
        let (sender, receiver) = oneshot::channel();
        self.command_sender
            .send(TceClientCommand::GetLastPendingCertificates { subnet_ids, sender })
            .await
            .map_err(|_| Error::InvalidChannelError)?;

        receiver.await.map_err(|_| Error::InvalidChannelError)?
    }

    pub fn get_subnet_id(&self) -> SubnetId {
        self.subnet_id
    }

    pub fn get_tce_endpoint(&self) -> &str {
        self.tce_endpoint.as_str()
    }
}

#[derive(Default)]
pub struct TceClientBuilder {
    tce_endpoint: Option<String>,
    subnet_id: Option<SubnetId>,
    tce_proxy_event_sender: Option<mpsc::Sender<TceProxyEvent>>,
}

impl TceClientBuilder {
    pub fn set_tce_endpoint<T: ToString>(mut self, endpoint: T) -> Self {
        self.tce_endpoint = Some(endpoint.to_string());
        self
    }

    pub fn set_subnet_id(mut self, subnet_id: SubnetId) -> Self {
        self.subnet_id = Some(subnet_id);
        self
    }

    pub fn set_proxy_event_sender(
        mut self,
        tce_proxy_event_sender: mpsc::Sender<TceProxyEvent>,
    ) -> Self {
        self.tce_proxy_event_sender = Some(tce_proxy_event_sender);
        self
    }

    pub async fn build_and_launch(
        self,
        mut shutdown: mpsc::Receiver<oneshot::Sender<()>>,
    ) -> Result<
        (
            TceClient,
            impl futures::stream::Stream<Item = (Certificate, TargetStreamPosition)>,
        ),
        Error,
    > {
        // Channel used to pass received certificates (certificates pushed TCE node) from the TCE client to the application
        let (inbound_certificate_sender, inbound_certificate_receiver) =
            mpsc::channel::<(Certificate, TargetStreamPosition)>(CERTIFICATE_INBOUND_CHANNEL_SIZE);

        let tce_endpoint = self
            .tce_endpoint
            .as_ref()
            .ok_or(Error::InvalidTceEndpoint)?
            .clone();
        // Connect to tce node service using backoff strategy
        let mut tce_grpc_client =
            match crate::connect_to_tce_service_with_retry(tce_endpoint.clone()).await {
                Ok(client) => {
                    info!("Connected to the TCE service at {}", &tce_endpoint);
                    client
                }
                Err(e) => {
                    error!("Unable to connect to tce client: {}", e);
                    return Err(e);
                }
            };

        // Channel used to initiate watch_certificates_request::Command that will be sent to the TCE through stream
        let (outbound_stream_command_sender, mut outbound_stream_command_receiver) =
            mpsc::channel::<WatchCertificatesRequest>(CERTIFICATE_OUTBOUND_CHANNEL_SIZE);

        // Outbound stream used to send watch_certificates_request::Command to the TCE node service
        let outbound_watch_certificates_stream = async_stream::stream! {
            loop {
                while let Some(request) = outbound_stream_command_receiver.recv().await {
                    yield request;
                }
            }
        };

        // Call TCE service watch certificates, get inbound response stream
        let mut inbound_watch_certificates_stream: tonic::Streaming<WatchCertificatesResponse> =
            tce_grpc_client
                .watch_certificates(outbound_watch_certificates_stream)
                .await
                .map(|r| r.into_inner())?;

        // Channel used to shut down task for inbound stream responses processing
        let (inbound_shutdown_sender, mut inbound_shutdown_receiver) =
            mpsc::unbounded_channel::<()>();

        let subnet_id = *self.subnet_id.as_ref().ok_or(Error::InvalidSubnetId)?;

        let tce_proxy_event_sender = self.tce_proxy_event_sender.clone();

        // Run task and process inbound watch certificate stream responses
        tokio::spawn(async move {
            // Listen for feedback from TCE service (WatchCertificatesResponse)
            info!(
                "Entering watch certificate response loop for tce node {} for subnet id {}",
                &tce_endpoint, &subnet_id
            );
            loop {
                tokio::select! {
                    Some(response) = inbound_watch_certificates_stream.next() => {
                        match response {
                            Ok(watch_certificate_response) => match watch_certificate_response.event {
                                // Received CertificatePushed event from TCE (new certificate has been received from TCE)
                                Some(watch_certificates_response::Event::CertificatePushed(
                                    mut certificate_pushed
                                )) => {
                                    info!("Certificate {:?} received from the TCE", &certificate_pushed);
                                    if let Some(certificate) = certificate_pushed.certificate.take() {
                                        let cert: Certificate = match certificate.try_into() {
                                            Ok(c) => c,
                                            Err(e) => {
                                                error!("Invalid Certificate conversion for  certificate: {e}");
                                                continue;
                                            }
                                        };
                                        // Currently only one target stream position is expected
                                        let position: TargetStreamPosition = match certificate_pushed.positions.first() {
                                            Some(p) => {
                                                if let Ok(p) = TryInto::<TargetStreamPosition>::try_into(p.clone()) {
                                                    p
                                                } else {
                                                    error!("Invalid target stream position for certificate id {}",cert.id);
                                                    continue;
                                                }
                                            },
                                            None => {
                                                error!("Invalid target stream position for certificate id {}",cert.id);
                                                continue;
                                            }
                                        };
                                        if let Err(e) = inbound_certificate_sender
                                            .send((cert, position))
                                            .await
                                        {
                                            error!(
                                                "Unable to pass received certificate to application: {e}"
                                            )
                                        }
                                    }
                                }
                                // Confirmation from TCE that stream has been opened
                                Some(watch_certificates_response::Event::StreamOpened(stream_opened)) => {
                                    info!(
                                        "Successfully opened the Certificate stream with the TCE at {} for the subnet(s): {:?}",
                                         &tce_endpoint, stream_opened.subnet_ids
                                    );
                                }
                                None => {
                                    warn!(
                                        "Watch certificate stream received None object from the TCE node at {}", &tce_endpoint
                                    );
                                }
                            },
                            Err(e) => {
                                error!(
                                    "Failed to open the Certificate stream with the TCE node at {} for the subnet(s): {:?}: {}",
                                    &tce_endpoint, &subnet_id, e.to_string()
                                );
                                // Send warning to restart TCE proxy
                                if let Some(tce_proxy_event_sender) = tce_proxy_event_sender.clone() {
                                    if let Err(e) = tce_proxy_event_sender.send(TceProxyEvent::WatchCertificatesChannelFailed).await {
                                          error!("Unable to send watch certificates channel failed signal: {e}");
                                    }
                                }
                            }
                        }
                    }
                    Some(_) = inbound_shutdown_receiver.recv() => {
                        info!("Finishing watch certificates task...");
                        // Finish this task listener
                        break;
                    }
                }
            }
            info!(
                "Finishing watch certificate task for tce node {} subnet_id {:?}",
                &tce_endpoint, &subnet_id
            );
        });

        // Channel used to pass commands from the application to the TCE proxy
        // To close to chanel worker task, send None as Certificate
        let (tce_command_sender, mut tce_command_receiver) =
            mpsc::channel::<TceClientCommand>(TCE_PROXY_COMMAND_CHANNEL_SIZE);

        // Run task for sending certificates to the TCE stream
        let tce_endpoint = self
            .tce_endpoint
            .as_ref()
            .ok_or(Error::InvalidTceEndpoint)?
            .clone();

        let tce_proxy_event_sender = self.tce_proxy_event_sender.clone();

        tokio::spawn(async move {
            let mut certificate_to_send = FuturesOrdered::new();
            info!(
                "Entering tce proxy command loop for stream {}",
                &tce_endpoint
            );
            loop {
                tokio::select! {
                    Some(result) = certificate_to_send.next() => {
                        match result {
                            Ok(()) => {
                                // All good, after one certificate is submitted carry on
                                continue;
                            }
                            Err(e) => {
                                // Backoff maximum period timeout. We need to restart sequencer.
                                error!("Failed to submit certificate to the tce network, backoff timeout with error: {e}. Restarting sequencer...");
                                if let Some(tce_proxy_event_sender) = tce_proxy_event_sender.clone() {
                                    if let Err(e) = tce_proxy_event_sender.send(TceProxyEvent::TceServiceFailure).await {
                                          error!("Unable to send tce communication failure signal: {e}");
                                    }
                                }
                            }
                        }
                    }
                    Some(sender) = shutdown.recv() => {
                        info!("Shutdown tce proxy command received...");
                        if !certificate_to_send.is_empty() {
                            info!("Waiting for all certificates to be sent...");
                            while certificate_to_send.next().await.is_some() {}
                        }

                        inbound_shutdown_sender.send(()).expect("valid channel for shutting down task");

                        sender.send(()).expect("valid channel for shutting down task");
                        break;
                    }
                    command = tce_command_receiver.recv() => {
                        match command {
                           Some(TceClientCommand::SendCertificate {cert, span}) =>  {
                                // Send new ceritficate to the TCE network
                                let cert_id = cert.id;
                                let previous_cert_id = cert.prev_id;
                                let span = info_span!(parent: &span, "SendCertificate", %cert_id, %previous_cert_id, %tce_endpoint);
                                let context = span.context();
                                let tce_endpoint = tce_endpoint.clone();
                                let tce_grpc_client = tce_grpc_client.clone();
                                let context_backoff = context.clone();
                                // TODO: Push certificates to the TCE one by one
                                certificate_to_send.push_back(async move {
                                    debug!("Submitting certificate {} to the TCE using backoff strategy...", &tce_endpoint);
                                    let cert = cert.clone();
                                    let op = || async {
                                        let mut tce_grpc_client = tce_grpc_client.clone();
                                        let mut request = SubmitCertificateRequest {
                                            certificate: Some(topos_core::api::grpc::uci::v1::Certificate::from(*(cert.clone()))),
                                        }.into_request();

                                        let mut span_context = topos_telemetry::TonicMetaInjector(request.metadata_mut());
                                        span_context.inject(&context_backoff);

                                        tce_grpc_client
                                        .submit_certificate(request)
                                        .with_context(context_backoff.clone())
                                        .instrument(Span::current())
                                        .await
                                        .map(|_response| {
                                            info!("Successfully submitted the Certificate {} (previous: {}) to the TCE at {}",
                                                &cert_id, &previous_cert_id, &tce_endpoint);
                                        })
                                        .map_err(|e| {
                                            error!("Failed to submit the Certificate to the TCE at {}, error: {e}", &tce_endpoint);
                                            new_tce_proxy_backoff_err(e)
                                        })
                                    };

                                    let backoff_configuration = backoff::ExponentialBackoff {
                                        max_elapsed_time: Some(TCE_SUBMIT_CERTIFICATE_BACKOFF_TIMEOUT),
                                        ..Default::default()
                                    };
                                    backoff::future::retry(backoff_configuration, op)
                                        .await
                                        .map_err(|e| {
                                            error!("Failed to submit certificate to the TCE: {e}");
                                           e
                                        })
                                }
                                .with_context(context)
                                .instrument(span));
                            }
                            Some(TceClientCommand::OpenStream {target_checkpoint}) =>  {
                                // Send command to TCE to open stream with my subnet id
                                info!(
                                    "Sending OpenStream command to the TCE node at {} for the Subnet {}",
                                    &tce_endpoint, &subnet_id
                                );
                                if let Err(e) = outbound_stream_command_sender
                                    .send(
                                            watch_certificates_request::OpenStream {
                                                target_checkpoint:
                                                    Some(target_checkpoint.into()),
                                                source_checkpoint: None
                                            }.into(),
                                    )
                                    .await
                                    {
                                        error!(
                                            "Unable to send OpenStream command: {e}"
                                        )
                                    }
                            }
                            Some(TceClientCommand::Shutdown) =>  {
                                info!("Shutdown tce proxy command received...");
                                inbound_shutdown_sender.send(()).expect("valid channel for shutting down task");
                                break;
                            }
                            Some(TceClientCommand::GetSourceHead {subnet_id, sender}) =>  {
                                    let result: Result<(Certificate, u64), Error> = match tce_grpc_client
                                    .get_source_head(GetSourceHeadRequest {
                                        subnet_id: Some(subnet_id.into())
                                    })
                                    .await
                                    .map(|r| r.into_inner()) {
                                        Ok(GetSourceHeadResponse {
                                            position: Some(pos),
                                            certificate: Some(cert),
                                        }) => {
                                            info!("Source head certificate acquired from tce, position: {}, certificate: {:?}", pos.position, &cert);
                                            Ok((cert.try_into().map_err(|_| Error::InvalidCertificate)?,
                                                pos.position))
                                        },
                                        Ok(_) => {
                                            Err(Error::SourceHeadEmpty{subnet_id})
                                        },
                                        Err(e) => {
                                            Err(Error::UnableToGetSourceHeadCertificate{subnet_id, details: e.to_string()})
                                        }
                                    };

                                if sender.send(result).is_err() {
                                    error!("Unable to pass result of the source head, channel failed");
                                };
                            }
                            Some(TceClientCommand::GetLastPendingCertificates { subnet_ids, sender }) => {
                                let result =
                                    match tce_grpc_client
                                        .get_last_pending_certificates(GetLastPendingCertificatesRequest {
                                            subnet_ids: subnet_ids.into_iter().map(Into::into).collect(),
                                        })
                                        .await
                                        .map(|r| r.into_inner())
                                    {
                                        Ok(response) => {
                                            let result = response
                                                .last_pending_certificate
                                                .into_iter()
                                                .map(|(subnet_id, last_pending_certificate)| {
                                                    let subnet_id: SubnetId = TryInto::<SubnetId>::try_into(
                                                        Base64::decode_vec(subnet_id.as_str()).map_err(|_| Error::InvalidSubnetId)?.as_slice(),
                                                    )
                                                    .map_err(|_| Error::InvalidSubnetId)?;

                                                    let certificate_and_index: Option<(Certificate, u64)> =
                                                        match last_pending_certificate.value {
                                                            Some(certificate) => Some(
                                                                Certificate::try_from(certificate)
                                                                .map(|certificate| (certificate, last_pending_certificate.index))
                                                                .map_err(
                                                                    |e| Error::UnableToGetLastPendingCertificates {
                                                                        details: e.to_string(),
                                                                        subnet_id,
                                                                    },
                                                                )?,
                                                            ),
                                                            None => None,
                                                        };


                                                    Ok((
                                                        subnet_id,
                                                        certificate_and_index
                                                    ))
                                                })
                                                .collect::<Result<HashMap<SubnetId, Option<(Certificate, u64)>>, Error>>()?;
                                            Ok(result)
                                        }
                                        Err(e) => Err(Error::UnableToGetLastPendingCertificates {
                                            subnet_id,
                                            details: e.to_string(),
                                        }),
                                    };

                                if sender.send(result).is_err() {
                                    error!("Unable to pass result for the last pending certificates, channel failed");
                                };
                            }
                            None => {
                                error!("Unexpected termination of the TCE proxy service of the Sequencer");
                                break;
                            }
                        }
                    }
                }
            }
            info!(
                "Finished submit certificate loop for stream {}",
                &tce_endpoint
            );
            Result::<(), Error>::Ok(())
        });

        Ok((
            TceClient {
                subnet_id: self.subnet_id.ok_or(Error::InvalidSubnetId)?,
                tce_endpoint: self.tce_endpoint.ok_or(Error::InvalidTceEndpoint)?,
                command_sender: tce_command_sender,
            },
            tokio_stream::wrappers::ReceiverStream::new(inbound_certificate_receiver),
        ))
    }
}
