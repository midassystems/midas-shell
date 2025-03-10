use crate::commands::TaskManager;
use crate::error::Result;
use clap::{Args, Subcommand};
use std::fmt::Debug;

#[derive(Debug, Args)]
pub struct BacktestArgs {
    #[command(subcommand)]
    pub subcommand: BacktestCommands,
}

#[derive(Debug, Subcommand)]
pub enum BacktestCommands {
    List,
    Run(RunArgs),
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Name of the strategy to backtest.
    #[arg(long)]
    pub name: String,
}

impl BacktestCommands {
    pub async fn process_command(&self, context: TaskManager) -> Result<()> {
        match self {
            BacktestCommands::List => {
                context.list_backtest().await;
            }
            BacktestCommands::Run(args) => {
                context.run_backtest(&args.name);
            }
        }
        Ok(())
    }
}
