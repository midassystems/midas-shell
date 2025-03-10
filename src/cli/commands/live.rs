use crate::commands::TaskManager;
use crate::Result;
use clap::{Args, Subcommand};
use std::fmt::Debug;

#[derive(Debug, Args)]
pub struct LiveArgs {
    #[command(subcommand)]
    pub subcommand: LiveCommands,
}

#[derive(Debug, Subcommand)]
pub enum LiveCommands {
    /// Run Strategy Live.
    Run(RunArgs),
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Name of the strategy running live.
    #[arg(long)]
    pub name: String,
}

impl LiveCommands {
    pub fn process_command(&self, mut context: TaskManager) -> Result<()> {
        match self {
            LiveCommands::Run(args) => {
                context.run_live(&args.name);
            }
        }
        Ok(())
    }
}
