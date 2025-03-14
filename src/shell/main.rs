use midas_clilib::shell::Repl;
use midas_clilib::{context::Context, error::Result};
use nu_ansi_term::{Color, Style};

#[tokio::main]
async fn main() -> Result<()> {
    let hinter_style = Style {
        foreground: Some(Color::DarkGray),
        background: None,
        is_bold: false,
        is_dimmed: true,
        is_italic: false,
        is_underline: false,
        is_blink: false,
        is_reverse: false,
        is_hidden: false,
        is_strikethrough: false,
        prefix_with_reset: false,
    };
    let context = Context::init()?;

    let mut repl = Repl::new(context)
        .with_logger("INFO".to_string())
        .with_history(1000)
        .with_hinter_style(hinter_style);

    let _ = repl.run().await;

    Ok(())
}
