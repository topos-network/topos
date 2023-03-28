//!
//! Application logic glue
//!
use crate::SequencerConfiguration;
use opentelemetry::trace::FutureExt;
use topos_sequencer_certification::CertificationWorker;
use topos_sequencer_subnet_runtime_proxy::SubnetRuntimeProxyWorker;
use topos_sequencer_tce_proxy::TceProxyWorker;
use topos_sequencer_types::*;
use tracing::{debug, error, info, info_span, warn, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Top-level transducer sequencer app context & driver (alike)
///
/// Implements <...Host> traits for network and Api, listens for protocol events in events
/// (store is not active component).
///
/// In the end we shall come to design where this struct receives
/// config+data as input and runs app returning data as output
///
pub struct AppContext {
    pub config: SequencerConfiguration,
    pub certification_worker: CertificationWorker,
    pub runtime_proxy_worker: SubnetRuntimeProxyWorker,
    pub tce_proxy_worker: TceProxyWorker,
}

impl AppContext {
    /// Factory
    pub fn new(
        config: SequencerConfiguration,
        certification_worker: CertificationWorker,
        runtime_proxy_worker: SubnetRuntimeProxyWorker,
        tce_proxy_worker: TceProxyWorker,
    ) -> Self {
        Self {
            config,
            certification_worker,
            runtime_proxy_worker,
            tce_proxy_worker,
        }
    }

    /// Main processing loop
    pub(crate) async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            tokio::select! {

                // Runtime View Worker
                Ok(evt) = self.runtime_proxy_worker.next_event() => {
                    debug!("runtime_proxy_worker.next_event(): {:?}", &evt);
                    self.on_runtime_proxy_event(evt).await;
                },

                // Certification Worker
                Ok(evt) = self.certification_worker.next_event() => {
                    debug!("certification_worker.next_event(): {:?}", &evt);
                    self.on_certification_event(evt).await;
                },

                // TCE events
                Ok(tce_evt) = self.tce_proxy_worker.next_event() => {
                    debug!("tce_proxy_worker.next_event(): {:?}", &tce_evt);
                    self.on_tce_proxy_event(tce_evt).await;
                },
            }
        }
    }

    async fn on_runtime_proxy_event(&mut self, evt: SubnetRuntimeProxyEvent) {
        debug!("on_runtime_proxy_event : {:?}", &evt);
        // This will always be a runtime proxy event
        let event = Event::RuntimeProxyEvent(evt);
        if let Err(e) = self.certification_worker.eval(event).await {
            error!("Unable to evaluate subnet runtime proxy event {e}");
        }
    }

    async fn on_certification_event(&mut self, evt: CertificationEvent) {
        debug!("on_certification_event : {:?}", &evt);
        match evt {
            CertificationEvent::NewCertificate { cert, ctx } => {
                let span = info_span!("Sequencer app context");
                span.set_parent(ctx);
                if let Err(e) = self
                    .tce_proxy_worker
                    .send_command(TceProxyCommand::SubmitCertificate {
                        cert: Box::new(cert),
                        ctx: span.context(),
                    })
                    .with_context(span.context())
                    .instrument(span)
                    .await
                {
                    error!("Unable to send tce proxy command {e}");
                }
            }
        }
    }

    async fn on_tce_proxy_event(&mut self, evt: TceProxyEvent) {
        match evt {
            TceProxyEvent::NewDeliveredCerts { certificates, ctx } => {
                let span = info_span!("Sequencer app context");
                span.set_parent(ctx);

                span.in_scope(|| {
                    // New certificates acquired from TCE
                    for (cert, cert_position) in certificates {
                        self.runtime_proxy_worker
                            .eval(SubnetRuntimeProxyCommand::OnNewDeliveredCertificate {
                                certificate: cert,
                                position: cert_position,
                                ctx: Span::current().context(),
                            })
                            .expect("Propagate new delivered Certificate to the runtime");
                    }
                });
            }
            TceProxyEvent::WatchCertificatesChannelFailed => {
                warn!("Restarting tce proxy worker...");
                let config = &self.tce_proxy_worker.config;
                // Here try to restart tce proxy
                _ = self.tce_proxy_worker.shutdown().await;

                // TODO: Retrieve subnet checkpoint from where to start receiving certificates, again
                let (tce_proxy_worker, _source_head_certificate_id) = match TceProxyWorker::new(
                    topos_sequencer_tce_proxy::TceProxyConfig {
                        subnet_id: config.subnet_id,
                        base_tce_api_url: config.base_tce_api_url.clone(),
                        positions: Vec::new(), // TODO acquire from subnet
                    },
                )
                .await
                {
                    Ok((tce_proxy_worker, source_head_certificate)) => {
                        info!("TCE proxy client is restarted for the source subnet {:?} from the head {:?}",config.subnet_id, source_head_certificate);
                        let source_head_certificate_id =
                            source_head_certificate.map(|cert| cert.id);
                        (tce_proxy_worker, source_head_certificate_id)
                    }
                    Err(e) => {
                        panic!("Unable to create TCE Proxy: {e}");
                    }
                };

                self.tce_proxy_worker = tce_proxy_worker;
            }
        }
    }

    // Shutdown app
    #[allow(dead_code)]
    async fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.tce_proxy_worker.shutdown().await?;
        self.certification_worker.shutdown().await?;
        self.runtime_proxy_worker.shutdown().await?;

        Ok(())
    }
}
