use crate::cli::ProcessCommand;
use crate::context::Context;
use crate::Result;
use async_trait::async_trait;
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

#[async_trait]
impl ProcessCommand for LiveCommands {
    async fn process_command(&self, _context: &Context) -> Result<()> {
        match self {
            LiveCommands::Run(args) => {
                let strategy_name = &args.name;
                let strategy_path = std::path::Path::new("strategies/").join(strategy_name);
                if strategy_path.exists() && strategy_path.is_dir() {
                    println!("Live strategy: {}", strategy_name);
                    // Here you would implement the actual backtesting logic
                } else {
                    println!("Strategy '{}' does not exist.", strategy_name);
                }
            }
        }
        Ok(())
    }
}
