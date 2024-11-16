use crate::cli::ProcessCommand;
use crate::context::Context;
use crate::error::Result;
use async_trait::async_trait;
use clap::{Args, Subcommand};
use serde::Deserialize;
use std::fmt::Debug;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct StrategyConfig {
    name: String,
}

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

#[async_trait]
impl ProcessCommand for StrategyCommands {
    async fn process_command(&self, _context: &Context) -> Result<()> {
        match self {
            StrategyCommands::List => {
                // Logic for listing strategies
                let strategy_dir = Path::new("strategies/");
                if strategy_dir.exists() && strategy_dir.is_dir() {
                    for entry in fs::read_dir(strategy_dir).unwrap() {
                        let entry = entry.unwrap();
                        if entry.path().is_dir() {
                            let config_path = entry.path().join("config.toml");
                            if config_path.exists() {
                                match fs::read_to_string(&config_path) {
                                    Ok(config_content) => {
                                        match toml::from_str::<StrategyConfig>(&config_content) {
                                            Ok(config) => {
                                                println!("  - {}", config.name);
                                            }
                                            Err(e) => {
                                                println!(
                                                    "Error parsing TOML in {:?}: {}",
                                                    config_path, e
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        println!("Error reading file {:?}: {}", config_path, e);
                                    }
                                }
                            } else {
                                println!("Warning: No config.toml found in {:?}", entry.path());
                            }
                        }
                    }
                } else {
                    println!("No strategies found or 'strategies/' directory does not exist.");
                }
            }
        }
        Ok(())
    }
}
