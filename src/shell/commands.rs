pub mod backtest;
pub mod clear;
pub mod exit;
pub mod historical;
pub mod instrument;
pub mod live;
pub mod midas;
pub mod processes;
pub mod strategies;
pub mod vendors;

use crate::{Result, TaskManager};
use backtest::BacktestArgs;
use clap::{Parser, Subcommand};
use clear::Clear;
use exit::Exit;
use historical::HistoricalArgs;
use instrument::InstrumentArgs;
use live::LiveArgs;
use midas::MidasArgs;
use processes::ProcessArgs;
use std::fmt::Debug;
use std::sync::Arc;
use strategies::StrategyArgs;
use tokio::sync::Mutex;
use vendors::databento::DatabentoArgs;

// Data options
pub struct Datasets;

impl Datasets {
    pub fn list() -> Vec<&'static str> {
        vec!["Futures", "Equities", "Option"]
    }
}

pub struct Schemas;

impl Schemas {
    pub fn list() -> Vec<&'static str> {
        vec![
            "mbp-1", "ohlcv-1s", "ohlcv-1m", "ohlcv-1h", "ohlcv-1d", "trades", "tbbo", "bbo-1s",
            "bbo-1m",
        ]
    }
}

pub struct Stypes;

impl Stypes {
    pub fn list() -> Vec<&'static str> {
        vec!["raw", "continuous"]
    }
}

pub struct Vendor;

impl Vendor {
    pub fn list() -> Vec<&'static str> {
        vec!["databento", "yfinance"]
    }
}

// Command Options
#[derive(Debug, Parser)]
pub struct ShellArgs {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Download historical price data.
    Historical,
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
    /// Processes shell
    Processes(ProcessArgs),
    /// Clear shell
    Clear,
    /// Exit shell
    Exit,
}

impl Commands {
    pub fn list() -> Vec<&'static str> {
        vec![
            "Historical",
            "Instrument",
            "Strategy",
            "Backtest",
            "Live",
            "Dashboard",
            "Midas",
            "Databento",
            "Clear",
            "Exit",
        ]
    }

    pub async fn process_command(self, context: Arc<Mutex<TaskManager>>) -> Result<()> {
        match self {
            Commands::Historical => HistoricalArgs::command(context).await?,
            Commands::Strategy(args) => args.subcommand.process_command(context).await?,
            Commands::Backtest(args) => args.subcommand.process_command(context).await?,
            Commands::Live(args) => args.subcommand.process_command(context).await?,
            Commands::Dashboard => context.lock().await.launch_dashboard(),
            Commands::Instrument(args) => args.subcommand.process_command(context).await?,
            Commands::Midas(args) => args.subcommand.process_command(context).await?,
            Commands::Databento(args) => args.subcommand.process_command(context).await?,
            Commands::Processes(args) => args.subcommand.process_command(context).await?,
            Commands::Clear => Clear::process_command(&Clear).await?,
            Commands::Exit => Exit::process_command(&Exit).await?,
        };
        Ok(())
    }
}
