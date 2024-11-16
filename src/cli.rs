pub mod backtest;
pub mod dashboard;
mod help;
pub mod historical;
mod live;
pub mod strategies;
// mod utils;
pub mod vendors;

use crate::context::Context;
use crate::error::Result;
use async_trait::async_trait;
use backtest::BacktestArgs;
use clap::{Command, CommandFactory};
use clap::{Parser, Subcommand};
use help::{Clear, Exit};
use historical::HistoricalArgs;
use live::LiveArgs;
use std::collections::HashSet;
use std::fmt::Debug;
use strategies::StrategyArgs;
use vendors::databento::DatabentoArgs;

pub fn get_commands() -> Vec<String> {
    let mut commands = HashSet::new();
    let app = CliArgs::command(); // Create the Clap Command app

    // Traverse top-level and subcommands recursively
    collect_clap_commands(&app, &mut commands);

    // Convert HashSet to Vec for completion
    commands.into_iter().collect()
}

fn collect_clap_commands(cmd: &Command, commands: &mut HashSet<String>) {
    // Add the command itself
    commands.insert(cmd.get_name().to_string());

    // Add all subcommands recursively
    for subcmd in cmd.get_subcommands() {
        collect_clap_commands(subcmd, commands);
    }
}

/// Trait for processing commands
#[async_trait]
pub trait ProcessCommand {
    async fn process_command(&self, context: &Context) -> Result<()>;
}

#[derive(Debug, Parser)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Download historical price data.
    Historical(HistoricalArgs),
    /// Strategy related commands.
    Strategy(StrategyArgs),
    /// Backtest related commands.
    Backtest(BacktestArgs),
    /// Live trading related commands.
    Live(LiveArgs),
    /// Open GUI dashboard.
    Dashboard,
    /// Commands for Databento source
    Databento(DatabentoArgs),
    /// Clear shell.
    Clear,
    /// Exit shell.
    Exit,
}

#[async_trait]
impl ProcessCommand for Commands {
    async fn process_command(&self, context: &Context) -> Result<()> {
        match self {
            Commands::Historical(args) => {
                args.process_command(context).await?;
                Ok(())
            }
            Commands::Strategy(strategy_args) => {
                strategy_args.subcommand.process_command(context).await?;
                Ok(())
            }
            Commands::Backtest(backtest_args) => {
                backtest_args.subcommand.process_command(context).await?;
                Ok(())
            }
            Commands::Live(live_args) => {
                live_args.subcommand.process_command(context).await?;
                Ok(())
            }
            Commands::Dashboard => Ok(()),
            Commands::Clear => {
                Clear::process_command(&Clear, context).await?;
                Ok(())
            }
            Commands::Databento(args) => {
                // Delegate Databento subcommands
                args.subcommand.process_command(context).await
            }
            Commands::Exit => {
                Exit::process_command(&Exit, context).await?;
                Ok(())
            }
        }
    }
}
