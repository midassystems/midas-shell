use crate::commands::TaskManager;
use crate::error::Result;
use clap::{Args, Subcommand};
use inquire::CustomType;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Args)]
pub struct ProcessArgs {
    #[command(subcommand)]
    pub subcommand: ProcessCommands,
}

#[derive(Debug, Subcommand)]
pub enum ProcessCommands {
    List,
    Kill,
}

impl ProcessCommands {
    pub async fn process_command(&self, context: Arc<Mutex<TaskManager>>) -> Result<()> {
        match self {
            ProcessCommands::List => {
                context.lock().await.list_processes().await;
            }
            ProcessCommands::Kill => {
                let id: u32 = CustomType::new("Instrument ID:")
                    .with_formatter(&|i: u32| format!("{i}"))
                    .with_error_message("Please type a valid number")
                    .prompt()
                    .unwrap();

                context.lock().await.kill_process(id);
            }
        }
        Ok(())
    }
}
