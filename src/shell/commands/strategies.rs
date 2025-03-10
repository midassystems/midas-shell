use crate::{Result, TaskManager};
pub struct Commands;
use clap::{Args, Subcommand};
use std::sync::Arc;
use tokio::sync::Mutex;

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

// #[async_trait]
impl StrategyCommands {
    pub async fn process_command(&self, context: Arc<Mutex<TaskManager>>) -> Result<()> {
        match self {
            StrategyCommands::List => {
                context.lock().await.list_strategies().await;
            }
        }

        Ok(())
    }
}
