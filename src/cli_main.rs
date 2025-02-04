use clap::Parser;
use midas_clilib::cli::{CliArgs, ProcessCommand};
use midas_clilib::context::Context;
use midas_clilib::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the Context, which holds the Config data.
    let context = Context::init().expect("Error with then context loading.");

    // Parse CLI arguments
    let args = CliArgs::parse();

    // Process the command and pass the ApiClient to it
    args.command.process_command(&context).await?;

    Ok(())
}
