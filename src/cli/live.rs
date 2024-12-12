use crate::cli::ProcessCommand;
use crate::context::Context;
use crate::utils::run_python_engine;
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
                let config_path = strategy_path.join("config.toml");

                if config_path.exists() {
                    println!("Strategy {} going Live.", strategy_name);

                    // Call the Python engine with the path to the config file
                    if let Err(e) = run_python_engine(config_path.to_str().unwrap(), "live") {
                        println!("Error running Python engine: {}", e);
                    }
                } else {
                    println!(
                        "Strategy '{}' must have a config.toml at the root level.",
                        strategy_name
                    );
                }
            }
        }
        Ok(())
    }
}
