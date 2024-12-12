use crate::cli::ProcessCommand;
use crate::context::Context;
use crate::error::Result;
use crate::utils::run_python_engine;
use async_trait::async_trait;
use clap::{Args, Subcommand};
use std::fmt::Debug;

#[derive(Debug, Args)]
pub struct BacktestArgs {
    #[command(subcommand)]
    pub subcommand: BacktestCommands,
}

#[derive(Debug, Subcommand)]
pub enum BacktestCommands {
    List,
    Run(RunArgs),
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Name of the strategy to backtest.
    #[arg(long)]
    pub name: String,
}

#[async_trait]
impl ProcessCommand for BacktestCommands {
    async fn process_command(&self, context: &Context) -> Result<()> {
        match self {
            BacktestCommands::List => {
                let client = context.get_trading_client();

                // Call the method on the client
                let backtests = client.list_backtest().await?;

                if backtests.data.len() > 0 {
                    // Display the results
                    println!("{:?}", backtests.data);
                } else {
                    // Display the results
                    println!("No backtests found.");
                }

                // Display the results
                // println!("May not be any backtests yet: {:?}", backtests.data);
            }
            BacktestCommands::Run(args) => {
                let strategy_name = &args.name;
                let strategy_path = std::path::Path::new("strategies/").join(strategy_name);
                let config_path = strategy_path.join("config.toml");

                if config_path.exists() {
                    println!("Backtesting strategy: {}", strategy_name);

                    // Call the Python engine with the path to the config file
                    if let Err(e) = run_python_engine(config_path.to_str().unwrap(), "backtest") {
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
