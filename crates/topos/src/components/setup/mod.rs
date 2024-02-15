use self::commands::{SetupCommand, SetupCommands};
use tokio::{signal, spawn};
use tracing::{error, info};

use topos::{install_polygon_edge, list_polygon_edge_releases};

pub(crate) mod commands;

pub(crate) async fn handle_command(
    SetupCommand { subcommands }: SetupCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match subcommands {
        Some(SetupCommands::Subnet(cmd)) => {
            spawn(async move {
                if cmd.list_releases {
                    info!(
                        "Retrieving release version list from repository: {}",
                        &cmd.repository
                    );
                    if let Err(e) = list_polygon_edge_releases(cmd.repository).await {
                        error!("Error listing Polygon Edge release versions: {e}");
                        std::process::exit(1);
                    } else {
                        std::process::exit(0);
                    }
                } else {
                    info!(
                        "Starting installation of Polygon Edge binary to target path: {}",
                        &cmd.path.display()
                    );
                    println!(
                        "Starting installation of Polygon Edge binary to target path: {}",
                        &cmd.path.display()
                    );
                    if let Err(e) =
                        install_polygon_edge(cmd.repository, cmd.release, cmd.path.as_path()).await
                    {
                        error!("Error installing Polygon Edge: {e}");
                        eprintln!("Error installing Polygon Edge: {e}");
                        std::process::exit(1);
                    } else {
                        info!("Polygon Edge installation successful");
                        println!("Polygon Edge installation successful");
                        std::process::exit(0);
                    }
                }
            });

            signal::ctrl_c()
                .await
                .expect("failed to listen for signals");

            Ok(())
        }
        None => {
            error!("No subcommand provided. You can use `--help` to see available subcommands.");
            eprintln!("No subcommand provided. You can use `--help` to see available subcommands.");
            std::process::exit(1);
        }
    }
}
