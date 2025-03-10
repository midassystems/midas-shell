use crate::commands::TaskManager;
use crate::error::Result;
use clap::{Args, Subcommand};
use inquire::Text;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Args)]
pub struct MidasArgs {
    #[command(subcommand)]
    pub subcommand: MidasCommands,
}

#[derive(Debug, Subcommand)]
pub enum MidasCommands {
    /// Compare databento and midas data
    Compare,
    /// Compare databento and midas data
    Duplicates,
}

impl MidasCommands {
    pub async fn process_command(&self, context: Arc<Mutex<TaskManager>>) -> Result<()> {
        match self {
            MidasCommands::Compare => {
                let filepath1 = Text::new("File1:").prompt()?;
                let filepath2 = Text::new("File2:").prompt()?;
                context
                    .lock()
                    .await
                    .compare_mbinay_files(PathBuf::from(filepath1), PathBuf::from(filepath2))
                    .await;

                Ok(())
            }
            MidasCommands::Duplicates => {
                let filepath = Text::new("FilePath:").prompt()?;

                context
                    .lock()
                    .await
                    .check_duplicates(PathBuf::from(filepath))
                    .await;

                Ok(())
            }
        }
    }
}
