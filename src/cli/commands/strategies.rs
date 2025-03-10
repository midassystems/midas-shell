use crate::commands::TaskManager;
use crate::error::Result;
use clap::{Args, Subcommand};
use std::fmt::Debug;

#[derive(Debug, Args)]
pub struct StrategyArgs {
    #[command(subcommand)]
    pub subcommand: StrategyCommands,
}

#[derive(Debug, Subcommand)]
pub enum StrategyCommands {
    /// List all available strategies.
    List,
}

impl StrategyCommands {
    pub async fn process_command(&self, context: TaskManager) -> Result<()> {
        match self {
            StrategyCommands::List => {
                context.list_strategies().await;
            }
        }

        Ok(())
    }
}
