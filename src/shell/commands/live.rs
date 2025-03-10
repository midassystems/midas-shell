use crate::commands::TaskManager;
use crate::error::Result;
use clap::{Args, Subcommand};
use inquire::Text;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Args)]
pub struct LiveArgs {
    #[command(subcommand)]
    pub subcommand: LiveCommands,
}

#[derive(Debug, Subcommand)]
pub enum LiveCommands {
    /// Run Strategy Live.
    Run,
}

impl LiveCommands {
    pub async fn process_command(&self, context: Arc<Mutex<TaskManager>>) -> Result<()> {
        match self {
            LiveCommands::Run => {
                let name = Text::new("Strategy:").prompt()?;
                context.lock().await.run_live(&name);
            }
        }
        Ok(())
    }
}
