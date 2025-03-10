use clap::Parser;
use midas_clilib::cli::commands::CliArgs;
use midas_clilib::commands::TaskManager;
use midas_clilib::context::Context;
use midas_clilib::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the Context, which holds the Config data.
    let context = Context::init().expect("Error with then context loading.");
    let task_manager = TaskManager::new(context);

    // Parse CLI arguments
    let args = CliArgs::parse();

    args.command.process_command(task_manager).await?;

    Ok(())
}
