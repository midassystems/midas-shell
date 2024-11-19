use repl_shell::error::Result;
use repl_shell::repl::Repl;

#[tokio::main]
async fn main() -> Result<()> {
    let mut repl = Repl::new()
        .with_logger("INFO".to_string())
        .with_history(1000)
        .with_starship_prompt();

    let _ = repl.run().await;

    Ok(())
}
