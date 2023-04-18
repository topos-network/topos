use crate::{client::TceClientBuilder, Error, TceProxyCommand, TceProxyConfig, TceProxyEvent};
use opentelemetry::trace::FutureExt;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use topos_core::uci::Certificate;
use tracing::{error, info, info_span, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Proxy with the TCE
///
/// 1) Fetch the delivered certificates from the TCE
/// 2) Submit the new certificate to the TCE
pub struct TceProxyWorker {
    pub config: TceProxyConfig,
    commands: mpsc::Sender<TceProxyCommand>,
    events: mpsc::Receiver<TceProxyEvent>,
}

impl TceProxyWorker {
    pub async fn new(config: TceProxyConfig) -> Result<(Self, Option<Certificate>), Error> {
        let (command_sender, mut command_rcv) = mpsc::channel::<TceProxyCommand>(128);
        let (evt_sender, evt_rcv) = mpsc::channel::<TceProxyEvent>(128);

        let (mut tce_client, mut receiving_certificate_stream) = TceClientBuilder::default()
            .set_subnet_id(config.subnet_id)
            .set_tce_endpoint(&config.base_tce_api_url)
            .set_proxy_event_sender(evt_sender.clone())
            .build_and_launch()
            .await?;

        tce_client.open_stream(config.positions.clone()).await?;

        // Get pending certificates from the TCE node. Source head certificate
        // is latest pending certificate for this subnet
        let mut source_last_pending_certificate: Option<Certificate> = match tce_client
            .get_last_pending_certificates(vec![tce_client.get_subnet_id()])
            .await
        {
            Ok(mut pending_certificates) => pending_certificates
                .remove(&tce_client.get_subnet_id())
                .unwrap_or_default(),
            Err(e) => {
                error!("Unable to retrieve latest pending certificate {e}");
                return Err(e);
            }
        };

        if source_last_pending_certificate.is_none() {
            // There are no pending certificates on the TCE
            // Retrieve source head from TCE node (latest delivered certificate), so that
            // we know from where to start creating certificates
            source_last_pending_certificate = match tce_client.get_source_head().await {
                Ok(certificate) => Some(certificate),
                Err(Error::SourceHeadEmpty { subnet_id: _ }) => {
                    // This is also OK, TCE node does not have any data about certificates
                    // We should start certificate production from scratch
                    None
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        tokio::spawn(async move {
            info!(
                "Starting the TCE proxy connected to the TCE at {}",
                tce_client.get_tce_endpoint()
            );
            loop {
                tokio::select! {
                    // process TCE proxy commands received from application
                    Some(cmd) = command_rcv.recv() => {
                        match cmd {
                            TceProxyCommand::SubmitCertificate{cert, ctx} => {
                                let span = info_span!("Sequencer TCE Proxy");
                                span.set_parent(ctx);
                                async {
                                    info!("Submitting new certificate to the TCE network: {}", &cert.id);
                                    if let Err(e) = tce_client.send_certificate(*cert).await {
                                        error!("Failure on the submission of the Certificate to the TCE client: {e}");
                                    }
                                }
                                .with_context(span.context())
                                .instrument(span)
                                .await;
                            }
                            TceProxyCommand::Shutdown(sender) => {
                                info!("Received TceProxyCommand::Shutdown command, closing tce client...");
                                if let Err(e) = tce_client.close().await {
                                    error!("Unable to shutdown the TCE client: {e}");
                                }
                                 _ = sender.send(());
                                break;
                            }
                        }
                    }

                     // Process certificates received from the TCE node
                    Some((cert, target_stream_position)) = receiving_certificate_stream.next() => {
                        let span = info_span!("PushCertificate");
                        async {
                            info!("Received certificate from TCE {:?}, target stream position {}", cert, target_stream_position.position);
                            if let Err(e) = evt_sender.send(TceProxyEvent::NewDeliveredCerts {
                                certificates: vec![(cert, target_stream_position.position)],
                                ctx: Span::current().context()}
                            )
                            .await {
                                error!("Unable to send NewDeliveredCerts event {e}");
                            }
                        }
                        .with_context(span.context())
                        .instrument(span)
                        .await;
                    }
                }
            }
            info!(
                "Exiting the TCE proxy worker handle loop connected to the TCE at {}",
                tce_client.get_tce_endpoint()
            );
        });

        // Save channels and handles, return latest tce known certificate
        Ok((
            Self {
                commands: command_sender,
                events: evt_rcv,
                config,
            },
            source_last_pending_certificate,
        ))
    }

    /// Send commands to TCE
    pub async fn send_command(&self, cmd: TceProxyCommand) -> Result<(), String> {
        match self.commands.send(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    /// Pollable (in select!) event listener
    pub async fn next_event(&mut self) -> Result<TceProxyEvent, String> {
        let event = self.events.recv().await;
        Ok(event.unwrap())
    }

    /// Shut down TCE proxy
    pub async fn shutdown(&self) -> Result<(), String> {
        info!("Shutting down TCE proxy worker...");
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.commands.send(TceProxyCommand::Shutdown(sender)).await {
            error!("Error sending shutdown signal to TCE worker {e}");
            return Err(e.to_string());
        };
        receiver.await.map_err(|e| e.to_string())
    }
}
