use clap::Parser;
// use cli::{
//     cli::{CliArgs, ProcessCommand},
//     Result,
// };
use repl_shell::cli::{CliArgs, ProcessCommand};
use repl_shell::context::Context;
use repl_shell::error::Result;

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
