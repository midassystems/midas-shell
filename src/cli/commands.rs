pub mod backtest;
pub mod historical;
pub mod instrument;
pub mod live;
pub mod midas;
pub mod strategies;
pub mod vendors;

use crate::commands::TaskManager;
use crate::error::Result;
use backtest::BacktestArgs;
use clap::{Parser, Subcommand};
use historical::HistoricalArgs;
use instrument::InstrumentArgs;
use live::LiveArgs;
use midas::MidasArgs;
use std::fmt::Debug;
use strategies::StrategyArgs;
use vendors::databento::DatabentoArgs;

#[derive(Debug, Parser)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Download historical price data.
    Historical(HistoricalArgs),
    /// Instrument
    Instrument(InstrumentArgs),
    /// Strategy related commands.
    Strategy(StrategyArgs),
    /// Backtest related commands.
    Backtest(BacktestArgs),
    /// Live trading related commands.
    Live(LiveArgs),
    /// Open GUI dashboard.
    Dashboard,
    /// Commands for Midas files.
    Midas(MidasArgs),
    /// Commands for Databento source
    Databento(DatabentoArgs),
}

// #[async_trait]
impl Commands {
    pub async fn process_command(&self, context: TaskManager) -> Result<()> {
        match self {
            Commands::Historical(args) => Ok(args.process_command(context).await?),
            Commands::Instrument(args) => Ok(args.subcommand.process_command(context).await?),
            Commands::Strategy(args) => Ok(args.subcommand.process_command(context).await?),
            Commands::Backtest(args) => Ok(args.subcommand.process_command(context).await?),
            Commands::Live(args) => Ok(args.subcommand.process_command(context)?),
            Commands::Dashboard => Ok(context.launch_dashboard()),
            Commands::Databento(args) => args.subcommand.process_command(context).await,
            Commands::Midas(args) => args.subcommand.process_command(context).await,
        }
    }
}
