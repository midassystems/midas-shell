use crate::commands::TaskManager;
use crate::error::Result;
use clap::{Args, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Args)]
pub struct MidasArgs {
    #[command(subcommand)]
    pub subcommand: MidasCommands,
}

#[derive(Debug, Subcommand)]
pub enum MidasCommands {
    /// Compare databento and midas data
    Compare {
        #[arg(long)]
        filepath1: String,
        #[arg(long)]
        filepath2: String,
    },
    /// Compare databento and midas data
    Duplicates {
        #[arg(long)]
        filepath: String,
    },
}

impl MidasCommands {
    pub async fn process_command(&self, context: TaskManager) -> Result<()> {
        match self {
            MidasCommands::Compare {
                filepath1,
                filepath2,
            } => {
                context
                    .compare_mbinay_files(PathBuf::from(filepath1), PathBuf::from(filepath2))
                    .await;

                Ok(())
            }
            MidasCommands::Duplicates { filepath } => {
                context.check_duplicates(PathBuf::from(filepath)).await;

                Ok(())
            }
        }
    }
}
