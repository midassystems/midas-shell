use crate::commands::TaskManager;
use crate::error::Result;
use clap::{Args, Subcommand};
use inquire::Text;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Args)]
pub struct BacktestArgs {
    #[command(subcommand)]
    pub subcommand: BacktestCommands,
}

#[derive(Debug, Subcommand)]
pub enum BacktestCommands {
    List,
    Run,
}

impl BacktestCommands {
    pub async fn process_command(&self, context: Arc<Mutex<TaskManager>>) -> Result<()> {
        match self {
            BacktestCommands::List => {
                context.lock().await.list_backtest().await;
            }
            BacktestCommands::Run => {
                let name = Text::new("Strategy:").prompt()?;
                context.lock().await.run_backtest(&name);
            }
        }
        Ok(())
    }
}
