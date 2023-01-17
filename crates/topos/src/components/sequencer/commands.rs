use clap::{Args, Subcommand};

mod run;

pub(crate) use run::Run;

#[derive(Args, Debug)]
pub(crate) struct SequencerCommand {
    #[clap(subcommand)]
    pub(crate) subcommands: Option<SequencerCommands>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum SequencerCommands {
    Run(Box<Run>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run() {
        assert!(SequencerCommands::has_subcommand("run"));
    }
}
