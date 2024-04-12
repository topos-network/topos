use self::commands::{RegtestCommand, RegtestCommands};

use opentelemetry::global;
use tokio::{
    spawn,
    sync::{mpsc, oneshot},
};
use topos_certificate_spammer::{error::Error, CertificateSpammerConfig};
use topos_telemetry::tracing::setup_tracing;
use tracing::{error, info};

pub(crate) mod commands;

pub(crate) async fn handle_command(
    RegtestCommand {
        verbose,
        subcommands,
    }: RegtestCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match subcommands {
        Some(RegtestCommands::Spam(cmd)) => {
            info!("{:#?}", cmd);
            let config = CertificateSpammerConfig {
                target_nodes: cmd.target_nodes,
                target_nodes_path: cmd.target_nodes_path,
                local_key_seed: cmd.local_key_seed,
                cert_per_batch: cmd.cert_per_batch,
                nb_subnets: cmd.nb_subnets,
                nb_batches: cmd.nb_batches,
                batch_interval: cmd.batch_interval,
                target_subnets: cmd.target_subnets,
                benchmark: cmd.benchmark,
                target_hosts: cmd.target_hosts,
                number: cmd.number,
            };

            // Setup instrumentation if both otlp agent and otlp service name
            // are provided as arguments
            setup_tracing(
                verbose,
                false,
                cmd.otlp_agent,
                cmd.otlp_service_name,
                env!("TOPOS_VERSION"),
            )?;

            info!("Verify version of topos_certificate_spammer: 222");

            let (shutdown_sender, shutdown_receiver) = mpsc::channel::<oneshot::Sender<()>>(1);
            let mut runtime = spawn(topos_certificate_spammer::run(config, shutdown_receiver));

            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        info!("Received ctrl_c, shutting down application...");

                        let (shutdown_finished_sender, shutdown_finished_receiver) = oneshot::channel::<()>();
                        if let Err(e) = shutdown_sender.send(shutdown_finished_sender).await {
                            error!("Error sending shutdown signal to Spammer application: {e}");
                        }
                        if let Err(e) = shutdown_finished_receiver.await {
                            error!("Error with shutdown receiver: {e}");
                        }
                        info!("Shutdown procedure finished, exiting...");
                    }
                    result = &mut runtime =>{
                        global::shutdown_tracer_provider();

                        if let Ok(Err(Error::BenchmarkConfig(ref msg))) = result {
                            error!("Benchmark configuration error:\n{}", msg);
                            std::process::exit(1);
                        }

                        if let Err(ref error) = result {

                            error!("Unable to execute network spam command due to: {error}");
                            std::process::exit(1);
                        }
                        break;
                    }
                }
            }

            Ok(())
        }
        None => Ok(()),
    }
}
