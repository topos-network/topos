use clap::{Parser, Subcommand};
use std::{ffi::OsString, path::PathBuf};

use crate::components::node::commands::NodeCommand;
use crate::components::regtest::commands::RegtestCommand;
use crate::components::setup::commands::SetupCommand;

pub(crate) mod input_format;

#[derive(Parser, Debug)]
#[clap(name = "topos", about = "Topos CLI")]
pub(crate) struct Opt {
    /// Defines the verbosity level
    #[arg(
        long,
        short = 'v',
        action = clap::ArgAction::Count,
        global = true
    )]
    pub(crate) verbose: u8,

    /// Disable color in logs
    #[arg(long, global = true, env = "TOPOS_LOG_NOCOLOR")]
    no_color: bool,

    /// Home directory for the configuration
    #[arg(
        long,
        env = "TOPOS_HOME",
        default_value = get_default_home(),
        global = true
    )]
    pub(crate) home: PathBuf,

    #[command(subcommand)]
    pub(crate) commands: ToposCommand,
}

/// If no path is given for the --home argument, we use the default one
/// ~/.config/topos for a UNIX subsystem
fn get_default_home() -> OsString {
    let mut home = dirs::home_dir().unwrap();
    home.push(".config");
    home.push("topos");
    home.into_os_string()
}

#[derive(Subcommand, Debug)]
pub(crate) enum ToposCommand {
    Setup(SetupCommand),
    Node(NodeCommand),
    Regtest(RegtestCommand),
}
