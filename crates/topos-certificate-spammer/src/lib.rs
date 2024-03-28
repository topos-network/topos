//! Utility to spam dummy certificates

use http::Uri;
use opentelemetry::trace::FutureExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{self, Duration};
use tokio_stream::StreamExt;
use topos_core::uci::*;
use topos_tce_proxy::client::{TceClient, TceClientBuilder};
use tracing::{debug, error, info, info_span, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

mod config;
pub mod error;
mod utils;

use error::Error;

use crate::utils::{generate_source_subnets, generate_test_certificate};
pub use config::CertificateSpammerConfig;

type NodeApiAddress = String;

#[derive(Deserialize)]
struct FileNodes {
    nodes: Vec<String>,
}

/// Represents connection from one sequencer to a TCE node
/// Multiple different subnets could be connected to the same TCE node address (represented with TargetNodeConnection with different SubnetId and created client)
/// Multiple topos-sequencers from the same subnet could be connected to the same TCE node address (so they would have same SubnetID, but different client instances)
struct TargetNodeConnection {
    address: NodeApiAddress,
    client: Arc<Mutex<TceClient>>,
    shutdown: mpsc::Sender<oneshot::Sender<()>>,
    source_subnet: SourceSubnet,
}

#[derive(Debug, Clone)]
pub struct SourceSubnet {
    signing_key: [u8; 32],
    source_subnet_id: SubnetId,
    last_certificate_id: CertificateId,
}

impl TargetNodeConnection {
    pub async fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let (sender, receiver) = oneshot::channel();
        self.shutdown.send(sender).await?;
        receiver.await?;

        Ok(())
    }
}

async fn open_target_node_connection(
    nodes: &[String],
    source_subnet: &SourceSubnet,
) -> Result<Vec<TargetNodeConnection>, Error> {
    let mut target_node_connections: Vec<TargetNodeConnection> = Vec::new();
    for tce_address in nodes {
        info!(
            "Opening client for tce service {}, source subnet id: {}",
            &tce_address, &source_subnet.source_subnet_id
        );

        let (tce_client_shutdown_channel, shutdown_receiver) =
            mpsc::channel::<oneshot::Sender<()>>(1);

        let (tce_client, mut receiving_certificate_stream) = match TceClientBuilder::default()
            .set_subnet_id(source_subnet.source_subnet_id)
            .set_tce_endpoint(tce_address)
            .build_and_launch(shutdown_receiver)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                error!(
                    "Unable to create TCE client for node {}: {}",
                    &tce_address, e
                );
                return Err(Error::TCENodeConnection(e));
            }
        };

        match tce_client.open_stream(Vec::new()).await {
            Ok(_) => {}
            Err(e) => {
                error!("Unable to connect to node {}: {}", &tce_address, e);
                return Err(Error::TCENodeConnection(e));
            }
        }

        let (shutdown_channel, mut shutdown_receiver) = mpsc::channel::<oneshot::Sender<()>>(1);

        let client = Arc::new(Mutex::new(tce_client));
        {
            let source_subnet_id = source_subnet.source_subnet_id;
            let tce_address = tce_address.clone();
            tokio::spawn(async move {
                loop {
                    // process certificates received from the TCE node
                    tokio::select! {
                         Some((cert, position)) = receiving_certificate_stream.next() => {
                            info!("Delivered certificate from tce address: {} for subnet id: {} cert id {}, position {:?}",
                                &tce_address, &source_subnet_id, &cert.id, position);
                         },
                         Some(sender) = shutdown_receiver.recv() => {
                            info!("Shutting down client for tce address: {} for subnet id: {}",
                                &tce_address, &source_subnet_id);

                            let (killer, waiter) = oneshot::channel::<()>();
                            tce_client_shutdown_channel.send(killer).await.unwrap();
                            waiter.await.unwrap();

                            info!("Finishing watch certificates task...");
                            _ = sender.send(());
                            // Finish this task listener
                            break;
                         }
                    }
                }
            });
        }

        target_node_connections.push(TargetNodeConnection {
            address: tce_address.clone(),
            client,
            shutdown: shutdown_channel,
            source_subnet: source_subnet.clone(),
        });
    }
    Ok(target_node_connections)
}

async fn close_target_node_connections(
    target_node_connections: HashMap<SubnetId, Vec<TargetNodeConnection>>,
) {
    for mut target_node in target_node_connections
        .into_iter()
        .flat_map(|(_, connections)| connections)
        .collect::<Vec<TargetNodeConnection>>()
    {
        info!("Closing connection to target node {}", target_node.address);
        if let Err(e) = target_node.shutdown().await {
            error!("Failed to close stream with {}: {e}", target_node.address);
        }
    }
}

/// Submit the certificate to the TCE node
async fn submit_cert_to_tce(node: &TargetNodeConnection, cert: Certificate) {
    let client = node.client.clone();
    let span = Span::current();
    span.record("certificate_id", cert.id.to_string());
    span.record("source_subnet_id", cert.source_subnet_id.to_string());

    let mut tce_client = client.lock().await;
    send_new_certificate(&mut tce_client, cert)
        .with_context(span.context())
        .instrument(span)
        .await
}

async fn send_new_certificate(tce_client: &mut TceClient, cert: Certificate) {
    if let Err(e) = tce_client
        .send_certificate(cert)
        .with_current_context()
        .instrument(Span::current())
        .await
    {
        error!("Failed to send the Certificate to the TCE client: {}", e);
    }
}

async fn dispatch(cert: Certificate, target_node: &TargetNodeConnection) {
    info!(
        "Sending cert id={:?} prev_cert_id= {:?} subnet_id={:?} to tce node {}",
        &cert.id, &cert.prev_id, &cert.source_subnet_id, target_node.address
    );
    submit_cert_to_tce(target_node, cert).await
}

pub async fn run(
    args: CertificateSpammerConfig,
    mut shutdown: mpsc::Receiver<oneshot::Sender<()>>,
) -> Result<(), Error> {
    // Is list of nodes is specified in the command line use them otherwise use
    // config file provided nodes
    let target_nodes = if args.benchmark {
        if let (Some(target_hosts), Some(number)) = (args.target_hosts, args.number) {
            let uri = target_hosts
                .replace("{N}", &0.to_string())
                .parse::<Uri>()
                .map_err(|e| Error::BenchmarkConfig(e.to_string()))?;

            if uri.host().is_none() || uri.path().is_empty() || uri.port_u16().is_none() {
                return Err(Error::BenchmarkConfig(
                    "Invalid target-hosts pattern. Has to be in the format of http://validator-1:9090"
                        .into(),
                ));
            }

            (0..number)
                .map(|n| target_hosts.replace("{N}", &n.to_string()))
                .collect::<Vec<String>>()
        } else {
            return Err(Error::BenchmarkConfig(
                "The --benchmark flag needs the following two additional flags being passed to it:\n--target-hosts http://validator-{N}\n--number 10".into(),
            ));
        }
    } else if let Some(nodes) = args.target_nodes {
        nodes
    } else if let Some(target_nodes_path) = args.target_nodes_path {
        let json_str = std::fs::read_to_string(target_nodes_path)
            .map_err(|e| Error::ReadingTargetNodesJsonFile(e.to_string()))?;

        let json: FileNodes = serde_json::from_str(&json_str)
            .map_err(|e| Error::InvalidTargetNodesJsonFile(e.to_string()))?;
        json.nodes
    } else {
        return Err(Error::TargetNodesNotSpecified);
    };

    // Generate keys for all required subnets (`nb_subnets`)
    let mut source_subnets = generate_source_subnets(args.local_key_seed, args.nb_subnets)?;
    info!("Generated source subnets: {source_subnets:#?}");

    // Target subnets (randomly assigned to every generated certificate)
    let target_subnet_ids: Vec<SubnetId> = args
        .target_subnets
        .iter()
        .flat_map(|id| {
            id.iter().map(|id| {
                let id =
                    hex::decode(&id[2..]).map_err(|e| Error::InvalidSubnetId(e.to_string()))?;
                TryInto::<[u8; 32]>::try_into(id.as_slice())
                    .map_err(|e| Error::InvalidSubnetId(e.to_string()))
            })
        })
        .map(|id| id.map(SubnetId::from_array))
        .collect::<Result<_, _>>()?;

    let mut target_node_connections: HashMap<SubnetId, Vec<TargetNodeConnection>> = HashMap::new();

    // For every source subnet, open connection to every target node, so we will have
    // nb_subnets * len(target_nodes) connections
    for source_subnet in &source_subnets {
        let connections_for_source_subnet =
            open_target_node_connection(target_nodes.as_slice(), source_subnet).await?;
        target_node_connections.insert(
            source_subnet.source_subnet_id,
            connections_for_source_subnet,
        );
    }

    target_node_connections
        .iter()
        .flat_map(|(_, connections)| connections)
        .for_each(|connection| {
            info!(
                "Certificate spammer target nodes address: {}, source_subnet_id: {}, target \
                 subnet ids {:?}",
                connection.address, connection.source_subnet.source_subnet_id, target_subnet_ids
            );
        });

    let number_of_peer_nodes = target_nodes.len();
    let mut batch_interval = time::interval(Duration::from_millis(args.batch_interval));
    let mut batch_number: u64 = 0;

    let shutdown_sender = loop {
        let should_send_batch = tokio::select! {
            _ = batch_interval.tick() => true,
            Some(sender) = shutdown.recv() => {
                info!("Received shutdown signal, stopping certificate spammer");

                for (_, connections) in target_node_connections {
                    for mut connection in connections {
                        info!("Closing connection to target node {}", connection.address);
                        _ = connection.shutdown().await;
                    }
                }


                break Some(sender);
            }
        };

        if should_send_batch {
            // Starting batch, generate cert_per_batch certificates
            batch_number += 1;
            let batch_id = uuid::Uuid::new_v4().to_string();
            // TODO: Need a better name for this span
            let span = info_span!(
                "Batch",
                batch_id,
                batch_number,
                cert_per_batch = args.cert_per_batch,
                number_of_peer_nodes
            );
            async {
                info!("Starting batch {batch_number}");

                let mut batch: Vec<Certificate> = Vec::new(); // Certificates for this batch
                for b in 0..args.cert_per_batch {
                    // Randomize source subnet id
                    let source_subnet =
                        &mut source_subnets[rand::random::<usize>() % args.nb_subnets as usize];
                    // Randomize number of target subnets if target subnet list cli argument is provided
                    let target_subnets: Vec<SubnetId> = if target_subnet_ids.is_empty() {
                        // Empty list of target subnets in certificate
                        Vec::new()
                    } else {
                        // Generate random list in size of 0..len(target_subnet_ids) as target subnets
                        let number_of_target_subnets =
                            rand::random::<usize>() % (target_subnet_ids.len() + 1);
                        let mut target_subnets = Vec::new();
                        for _ in 0..number_of_target_subnets {
                            target_subnets.push(
                                target_subnet_ids
                                    [rand::random::<usize>() % target_subnet_ids.len()],
                            );
                        }
                        target_subnets
                    };

                    let new_cert =
                        match generate_test_certificate(source_subnet, target_subnets.as_slice()) {
                            Ok(cert) => cert,
                            Err(e) => {
                                error!("Unable to generate certificate: {e}");
                                continue;
                            }
                        };
                    debug!("New cert number {b} in batch {batch_number} generated");
                    batch.push(new_cert);
                }

                // Dispatch certs in this batch
                // Dispatch certs in this batch
                for cert in batch {
                    // Randomly choose target tce node for every certificate from related source_subnet_id connection list
                    // let target_node_connection = &target_node_connections[&cert.source_subnet_id]
                    //     [rand::random::<usize>() % target_nodes.len()];

                    for connection in &target_node_connections[&cert.source_subnet_id] {
                        debug!(
                            "Sending certificate {cert:?} to target node {}",
                            connection.address
                        );
                        dispatch(cert.clone(), connection)
                            .instrument(Span::current())
                            .with_current_context()
                            .await;
                    }
                }
            }
            .instrument(span)
            .await;

            if let Some(nb_batches) = args.nb_batches {
                if batch_number >= nb_batches {
                    info!("Generated {nb_batches}, finishing certificate spammer...");

                    tokio::time::sleep(Duration::from_secs(5)).await;
                    close_target_node_connections(target_node_connections).await;
                    info!("Cert spammer finished");
                    break None;
                }
            }
        }
    };

    info!("Certificate spammer finished");
    if let Some(sender) = shutdown_sender {
        sender
            .send(())
            .expect("Failed to send shutdown signal from certificate spammer");
    }

    Ok(())
}
