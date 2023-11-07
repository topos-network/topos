use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::path::Path;
use std::{
    fs::{create_dir_all, remove_dir_all, OpenOptions},
    io::Write,
};
use tokio::{signal, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use self::commands::{NodeCommand, NodeCommands};
use crate::config::genesis::Genesis;
use crate::edge::BINARY_NAME;
use crate::{
    config::{insert_into_toml, node::NodeConfig, node::NodeRole},
    tracing::setup_tracing,
};
use services::*;
use topos_wallet::SecretManager;

pub(crate) mod commands;
pub mod services;

pub(crate) async fn handle_command(
    NodeCommand {
        subcommands,
        verbose,
        home,
        edge_path,
    }: NodeCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing(verbose, None, None)?;

    match subcommands {
        Some(NodeCommands::Init(cmd)) => {
            let cmd = *cmd;
            let name = cmd.name.as_ref().expect("No name or default was given");

            // Construct path to node config
            // will be $TOPOS_HOME/node/default/ with no given name
            // and $TOPOS_HOME/node/<name>/ with a given name
            let node_path = home.join("node").join(name);

            // If the folders don't exist yet, create it
            create_dir_all(&node_path).expect("failed to create node folder");

            // Check if the config file exists
            let config_path = node_path.join("config.toml");

            if Path::new(&config_path).exists() {
                println!("Config file: {} already exists", config_path.display());
                std::process::exit(1);
            }

            // Generate the configuration as per the role
            let mut config_toml = toml::Table::new();

            // Generate the Edge configuration
            if let Ok(result) =
                generate_edge_config(edge_path.join(BINARY_NAME), node_path.clone()).await
            {
                if result.is_err() {
                    println!("Failed to generate edge config");
                    remove_dir_all(node_path).expect("failed to remove config folder");
                    std::process::exit(1);
                }
            }

            let node_config = NodeConfig::new(&node_path, Some(cmd));

            // Creating the TOML output
            insert_into_toml(&mut config_toml, node_config);

            let config_path = node_path.join("config.toml");
            let mut node_config_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(config_path)
                .expect("failed to create default node file");

            node_config_file
                .write_all(toml::to_string(&config_toml).unwrap().as_bytes())
                .expect("failed to write to default node file");

            println!(
                "Created node config file at {}/config.toml",
                node_path.display()
            );

            Ok(())
        }
        Some(NodeCommands::Up(cmd)) => {
            let name = cmd
                .name
                .as_ref()
                .expect("No name or default was given for node");
            let node_path = home.join("node").join(name);
            let config_path = node_path.join("config.toml");

            if !Path::new(&config_path).exists() {
                println!(
                    "Please run 'topos node init --name {name}' to create a config file first for \
                     {name}."
                );
                std::process::exit(1);
            }

            // FIXME: Handle properly the `cmd`
            let config = NodeConfig::new(&node_path, None);
            info!(
                "⚙️ Reading the configuration from {}/config.toml",
                node_path.display()
            );

            // Load genesis pointed by the local config
            let genesis = Genesis::new(
                home.join("subnet")
                    .join(config.base.subnet.clone())
                    .join("genesis.json"),
            );

            // Get secrets
            let keys = match &config.base.secrets_config {
                Some(secrets_config) => SecretManager::from_aws(secrets_config),
                None => SecretManager::from_fs(node_path.clone()),
            };

            info!(
                "🧢 New joiner: {} for the \"{}\" subnet as {:?}",
                config.base.name, config.base.subnet, config.base.role
            );

            let shutdown_token = CancellationToken::new();
            let shutdown_trigger = shutdown_token.clone();
            let (shutdown_sender, shutdown_receiver) = mpsc::channel(1);

            let mut processes = FuturesUnordered::new();

            // Edge node
            if cmd.no_edge_process {
                info!("Using external edge node, skip running of local edge instance...")
            } else if let Some(edge_config) = config.edge {
                let data_dir = node_path.clone();
                info!(
                    "Spawning edge process with genesis file: {}, data directory: {}, additional \
                     edge arguments: {:?}",
                    genesis.path.display(),
                    data_dir.display(),
                    edge_config.args
                );
                processes.push(services::spawn_edge_process(
                    edge_path.join(BINARY_NAME),
                    data_dir,
                    genesis.path.clone(),
                    edge_config.args,
                ));
            } else {
                error!("Missing edge configuration, could not run edge node!");
                std::process::exit(1);
            }

            // Sequencer
            if matches!(config.base.role, NodeRole::Sequencer) {
                let sequencer_config = config
                    .sequencer
                    .clone()
                    .expect("valid sequencer configuration");
                info!(
                    "Running sequencer with configuration {:?}",
                    sequencer_config
                );
                processes.push(services::spawn_sequencer_process(
                    sequencer_config,
                    &keys,
                    (shutdown_token.clone(), shutdown_sender.clone()),
                ));
            }

            // TCE
            if config.base.subnet == "topos" {
                info!("Running topos TCE service...",);
                processes.push(services::spawn_tce_process(
                    config.tce.clone().unwrap(),
                    keys,
                    genesis,
                    (shutdown_token.clone(), shutdown_sender.clone()),
                ));
            }

            drop(shutdown_sender);

            tokio::select! {
                _ = signal::ctrl_c() => {
                    info!("Received ctrl_c, shutting down application...");
                    shutdown(shutdown_trigger, shutdown_receiver).await;
                }
                Some(result) = processes.next() => {
                    info!("Terminate: {result:?}");
                    if let Err(e) = result {
                        error!("Termination: {e}");
                    }
                    shutdown(shutdown_trigger, shutdown_receiver).await;
                    processes.clear();
                }
            };

            Ok(())
        }
        None => Ok(()),
    }
}

pub async fn shutdown(trigger: CancellationToken, mut termination: mpsc::Receiver<()>) {
    trigger.cancel();
    // Wait that all sender get dropped
    info!("Waiting that all components dropped");
    let _ = termination.recv().await;
    info!("Shutdown procedure finished, exiting...");
}
