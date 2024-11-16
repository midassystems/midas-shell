use repl_shell::error::Result;
use repl_shell::repl::Repl;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    let mut repl = Repl::new()
        .with_history(PathBuf::from("history.txt").to_path_buf(), 1000)
        .with_starship_prompt();

    let _ = repl.run().await;

    Ok(())
}
