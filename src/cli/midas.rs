use crate::cli::ProcessCommand;
use crate::context::Context;
use crate::error::Result;
use crate::pipeline::midas::checks::find_duplicates;
use crate::pipeline::midas::compare::compare_mbinary;
use async_trait::async_trait;
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

#[async_trait]
impl ProcessCommand for MidasCommands {
    async fn process_command(&self, context: &Context) -> Result<()> {
        let _client = context.get_historical_client();

        match self {
            MidasCommands::Compare {
                filepath1,
                filepath2,
            } => {
                let _ =
                    compare_mbinary(&PathBuf::from(filepath1), &PathBuf::from(filepath2)).await?;

                Ok(())
            }
            MidasCommands::Duplicates { filepath } => {
                let _ = find_duplicates(&PathBuf::from(filepath)).await?;

                Ok(())
            }
        }
    }
}
